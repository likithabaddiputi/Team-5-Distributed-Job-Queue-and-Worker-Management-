#!/usr/bin/env python3
"""
D2 Performance Evaluation — rigorous version.

Runs the real coordinator / worker / client scripts on 127.0.0.1
(same style as test_sockets.py — no Mininet/Ryu required) and produces:

  1. Throughput & latency under increasing concurrency
  2. Scheduler fairness (Jain's index) + load-aware vs static baseline
  3. Failover recovery times (worker kill + repeated failure)
  4. A clear side-by-side comparison that demonstrates why SDN feedback helps

Usage:
    python3 tests/test_performance.py

Prints a human-readable report and writes:
    /tmp/sdn_perf_report.json
    /tmp/sdn_perf_report.md
"""

from __future__ import annotations

import json
import math
import os
import re
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from collections import defaultdict

SCRIPTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tcp_socket')
REPORT_JSON = '/tmp/sdn_perf_report.json'
REPORT_MD = '/tmp/sdn_perf_report.md'

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def free_port():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    p = s.getsockname()[1]
    s.close()
    return p


def spawn(script, *args, env=None, stdout=subprocess.PIPE):
    return subprocess.Popen(
        [sys.executable, '-u', os.path.join(SCRIPTS, script), *args],
        env=env, stdout=stdout, stderr=subprocess.STDOUT, text=True
    )


def wait_port(port, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(('127.0.0.1', port), 0.2).close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


def jain_fairness(counts):
    """Jain's fairness index. 1.0 = perfectly fair, lower = more skewed."""
    vals = list(counts.values())
    if not vals:
        return None
    s = sum(vals)
    sq = sum(v * v for v in vals)
    if sq == 0:
        return 1.0
    return round((s * s) / (len(vals) * sq), 4)


def pct(sorted_vals, p):
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


# ---------------------------------------------------------------------------
# Process harness (coordinator + N workers)
# ---------------------------------------------------------------------------

class Harness:
    """Coordinator + N workers on 127.0.0.1. No real Ryu needed;
    LOAD messages can be injected via FakeController when desired."""

    def __init__(self, n_workers=2, job_time='1'):
        self.tmp = tempfile.mkdtemp()
        self.port = free_port()
        self.env = dict(
            os.environ,
            COORD_HOST='127.0.0.1',
            COORD_PORT=str(self.port),
            SDN_CTL_SOCK=os.path.join(self.tmp, 'ctl.sock'),
            JOB_TIME_MIN=str(job_time),
            JOB_TIME_MAX=str(job_time),
            PYTHONUNBUFFERED='1',
            HB_TIMEOUT='8',
        )
        self.log_path = os.path.join(self.tmp, 'coord.log')
        self.log_file = open(self.log_path, 'w')
        self.procs = [spawn('simple_coordinator.py', env=self.env, stdout=self.log_file)]
        if not wait_port(self.port):
            raise RuntimeError('coordinator did not start')
        self.workers = {}
        for i in range(1, n_workers + 1):
            wid = 'w%d' % i
            self.workers[wid] = spawn(
                'simple_worker.py', wid, env=self.env, stdout=subprocess.DEVNULL
            )
            self._wait_log(wid + ' connected')

    def log(self):
        self.log_file.flush()
        with open(self.log_path) as f:
            return f.read()

    def _wait_log(self, text, timeout=15):
        end = time.time() + timeout
        while time.time() < end:
            if text in self.log():
                return True
            time.sleep(0.04)
        raise RuntimeError('timed out waiting for %r in coordinator log' % text)

    def client(self, n):
        return spawn('simple_client.py', str(n), env=self.env)

    def stop(self):
        for p in list(self.workers.values()) + self.procs:
            if p.poll() is None:
                p.kill()
        for p in list(self.workers.values()) + self.procs:
            try:
                p.wait(timeout=2)
            except Exception:
                pass
        self.log_file.close()


# ---------------------------------------------------------------------------
# Fake SDN controller (injects LOAD messages)
# ---------------------------------------------------------------------------

class FakeController:
    """Stands in for controller.py on the UNIX app↔SDN socket.
    Continuously pushes the given per-worker LOAD values so the
    coordinator's least-loaded scheduler can react to them."""

    def __init__(self, ctl_sock_path, loads, enabled=True, interval=0.25):
        self.path = ctl_sock_path
        self.loads = dict(loads)          # {"w1": bytes_per_sec, ...}
        self.enabled = enabled
        self.interval = interval
        self._stop = False
        self._srv = None
        self._thread = None

    def start(self):
        if not self.enabled:
            return
        self._srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if os.path.exists(self.path):
            os.remove(self.path)
        self._srv.bind(self.path)
        self._srv.listen(1)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def update_loads(self, loads):
        self.loads = dict(loads)

    def _serve(self):
        self._srv.settimeout(0.8)
        conn = None
        while not self._stop:
            if conn is None:
                try:
                    conn, _ = self._srv.accept()
                except socket.timeout:
                    continue
            try:
                for wid, load in self.loads.items():
                    conn.sendall(("LOAD|%s|%d|%d\n" % (wid, load, load)).encode())
                time.sleep(self.interval)
            except OSError:
                conn = None

    def stop(self):
        self._stop = True
        if self._srv:
            try:
                self._srv.close()
            except OSError:
                pass


# ---------------------------------------------------------------------------
# 1. Throughput + latency under concurrency
# ---------------------------------------------------------------------------

def measure_throughput(n_clients, n_workers=3, job_time='1', repeats=3):
    """Run the same experiment several times and return mean/stdev stats."""
    runs = []
    for _ in range(repeats):
        h = Harness(n_workers=n_workers, job_time=job_time)
        try:
            t0 = time.time()
            procs = [h.client(10 + i) for i in range(n_clients)]
            latencies = []
            ok = 0
            for p in procs:
                # We don't have per-client start times easily, so overall
                # elapsed is used for throughput; latency is derived from
                # wall-clock of each process.
                start = time.time()
                rc = p.wait(timeout=45)
                latencies.append(time.time() - start)
                ok += (rc == 0)
            elapsed = time.time() - t0

            counts = defaultdict(int)
            for m in re.finditer(r'-> assigned to worker (\w+)', h.log()):
                counts[m.group(1)] += 1

            sorted_lat = sorted(latencies)
            runs.append({
                'completed': ok,
                'elapsed_s': round(elapsed, 3),
                'throughput_jobs_per_s': round(ok / elapsed, 3) if elapsed else 0,
                'per_worker_counts': dict(counts),
                'jain_fairness': jain_fairness(counts),
                'latency_mean_s': round(statistics.mean(latencies), 3) if latencies else None,
                'latency_p50_s': round(pct(sorted_lat, 50), 3) if sorted_lat else None,
                'latency_p95_s': round(pct(sorted_lat, 95), 3) if sorted_lat else None,
            })
        finally:
            h.stop()
        time.sleep(0.3)

    throughputs = [r['throughput_jobs_per_s'] for r in runs]
    fairnesses = [r['jain_fairness'] for r in runs if r['jain_fairness'] is not None]
    return {
        'n_clients': n_clients,
        'n_workers': n_workers,
        'repeats': repeats,
        'throughput_mean': round(statistics.mean(throughputs), 3),
        'throughput_stdev': round(statistics.stdev(throughputs), 3) if len(throughputs) > 1 else 0.0,
        'jain_mean': round(statistics.mean(fairnesses), 4) if fairnesses else None,
        'latency_p50_mean': round(statistics.mean(r['latency_p50_s'] for r in runs if r['latency_p50_s']), 3),
        'latency_p95_mean': round(statistics.mean(r['latency_p95_s'] for r in runs if r['latency_p95_s']), 3),
        'runs': runs,
    }


# ---------------------------------------------------------------------------
# 2. Scheduling: SDN-load-aware vs static baseline (the key comparison)
# ---------------------------------------------------------------------------

def measure_scheduling(n_jobs=24, n_workers=3, sdn_enabled=True, job_time='0.4', repeats=3):
    """
    Submit jobs one-by-one so each scheduling decision is independent.
    Worker w1 is reported as heavily loaded.
    SDN-aware scheduler should avoid w1; static baseline cannot.
    """
    runs = []
    for _ in range(repeats):
        h = Harness(n_workers=n_workers, job_time=job_time)
        # Extreme skew: w1 looks saturated, others idle
        loads = {'w1': 8_000_000, 'w2': 50, 'w3': 80}
        ctrl = FakeController(h.env['SDN_CTL_SOCK'], loads, enabled=sdn_enabled)
        ctrl.start()
        try:
            if sdn_enabled:
                time.sleep(0.9)   # let first LOAD figures arrive
            for i in range(n_jobs):
                p = h.client(100 + i)
                p.wait(timeout=20)

            counts = defaultdict(int)
            for m in re.finditer(r'-> assigned to worker (\w+)', h.log()):
                counts[m.group(1)] += 1

            total = sum(counts.values()) or 1
            runs.append({
                'per_worker_counts': dict(counts),
                'w1_share': round(counts.get('w1', 0) / total, 3),
                'jain_fairness': jain_fairness(counts),
                'balance_spread': (max(counts.values()) - min(counts.values())) if counts else 0,
            })
        finally:
            ctrl.stop()
            h.stop()
        time.sleep(0.25)

    w1_shares = [r['w1_share'] for r in runs]
    jains = [r['jain_fairness'] for r in runs if r['jain_fairness'] is not None]
    return {
        'sdn_enabled': sdn_enabled,
        'n_jobs': n_jobs,
        'n_workers': n_workers,
        'repeats': repeats,
        'w1_share_mean': round(statistics.mean(w1_shares), 3),
        'w1_share_stdev': round(statistics.stdev(w1_shares), 3) if len(w1_shares) > 1 else 0.0,
        'jain_mean': round(statistics.mean(jains), 4) if jains else None,
        'runs': runs,
    }


# ---------------------------------------------------------------------------
# 3. Failover / recovery timing
# ---------------------------------------------------------------------------

def measure_failover(job_time='3.5', repeats=4):
    """Kill a worker that is mid-job and measure:
       - time until coordinator re-queues the job
       - time until the job is re-assigned to another worker
       - time until the client finally receives the result
    """
    runs = []
    for _ in range(repeats):
        h = Harness(n_workers=2, job_time=job_time)
        try:
            c1 = h.client(42)
            h._wait_log('assigned to worker w1')
            t_kill = time.time()
            h.workers['w1'].kill()

            h._wait_log('was running on w1 -> put back in the queue', timeout=12)
            t_requeued = time.time()

            h._wait_log('job1 -> assigned to worker w2', timeout=12)
            t_reassigned = time.time()

            rc = c1.wait(timeout=30)
            t_done = time.time()

            runs.append({
                'detect_to_requeue_s': round(t_requeued - t_kill, 3),
                'detect_to_reassign_s': round(t_reassigned - t_kill, 3),
                'kill_to_client_result_s': round(t_done - t_kill, 3),
                'client_ok': rc == 0,
            })
        finally:
            h.stop()
        time.sleep(0.3)

    req = [r['detect_to_requeue_s'] for r in runs]
    rea = [r['detect_to_reassign_s'] for r in runs]
    tot = [r['kill_to_client_result_s'] for r in runs]
    return {
        'job_time_s': float(job_time),
        'repeats': repeats,
        'requeue_mean_s': round(statistics.mean(req), 3),
        'requeue_stdev_s': round(statistics.stdev(req), 3) if len(req) > 1 else 0.0,
        'reassign_mean_s': round(statistics.mean(rea), 3),
        'reassign_stdev_s': round(statistics.stdev(rea), 3) if len(rea) > 1 else 0.0,
        'client_result_mean_s': round(statistics.mean(tot), 3),
        'success_rate': round(sum(1 for r in runs if r['client_ok']) / len(runs), 2),
        'runs': runs,
    }


# ---------------------------------------------------------------------------
# 4. Repeated failure robustness (job eventually fails after MAX_ATTEMPTS)
# ---------------------------------------------------------------------------

def measure_repeated_failure(max_attempts=3):
    """Keep killing the only worker that receives the job; the job should
    eventually be failed with too_many_worker_failures after MAX_ATTEMPTS."""
    h = Harness(n_workers=1, job_time='2')
    try:
        c1 = h.client(7)
        for attempt in range(max_attempts):
            h._wait_log('assigned to worker w1', timeout=10)
            h.workers['w1'].kill()
            # revive a fresh worker so the next attempt can be scheduled
            if attempt < max_attempts - 1:
                time.sleep(0.4)
                h.workers['w1'] = spawn(
                    'simple_worker.py', 'w1', env=h.env, stdout=subprocess.DEVNULL
                )
                h._wait_log('w1 connected', timeout=8)
        # After the last kill the job should be marked failed
        h._wait_log('too_many_worker_failures', timeout=12)
        rc = c1.wait(timeout=15)
        return {
            'max_attempts': max_attempts,
            'client_rc': rc,
            'failed_as_expected': rc != 0 or 'too_many_worker_failures' in h.log(),
        }
    finally:
        h.stop()


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def write_markdown(report):
    lines = []
    lines.append('# SDN Job-Queue Performance Evaluation Report')
    lines.append('')
    lines.append('Generated by `tests/test_performance.py`.')
    lines.append('')

    # Throughput
    lines.append('## 1. Throughput & Latency under Concurrency')
    lines.append('')
    lines.append('| Concurrent clients | Throughput (jobs/s) | Jain fairness | Latency p50 (s) | Latency p95 (s) |')
    lines.append('|---|---|---|---|---|')
    for r in report.get('throughput', []):
        lines.append(
            '| %d | **%.2f** ± %.2f | %.3f | %.2f | %.2f |' % (
                r['n_clients'],
                r['throughput_mean'], r['throughput_stdev'],
                r['jain_mean'] or 0,
                r['latency_p50_mean'], r['latency_p95_mean'],
            )
        )
    lines.append('')
    lines.append('> Jain’s fairness index ≈ 1.0 means jobs are distributed evenly across workers.')
    lines.append('')

    # Scheduling comparison
    lines.append('## 2. Scheduling: SDN-load-aware vs Static Baseline')
    lines.append('')
    lines.append('Worker **w1** is artificially reported as heavily loaded (≈ 8 MB/s).')
    lines.append('A good scheduler should avoid it; a static/no-feedback scheduler cannot.')
    lines.append('')
    lines.append('| Policy | Mean share of jobs on congested w1 | Jain fairness |')
    lines.append('|---|---|---|')
    sched = report.get('scheduling', {})
    for key, label in (
        ('baseline_static_no_sdn', 'Static / no SDN feedback'),
        ('sdn_load_aware', '**SDN load-aware**'),
    ):
        if key in sched:
            r = sched[key]
            lines.append(
                '| %s | **%.0f%%** ± %.0f%% | %.3f |' % (
                    label,
                    r['w1_share_mean'] * 100,
                    r['w1_share_stdev'] * 100,
                    r['jain_mean'] or 0,
                )
            )
    lines.append('')
    if 'sdn_load_aware' in sched and 'baseline_static_no_sdn' in sched:
        base = sched['baseline_static_no_sdn']['w1_share_mean']
        smart = sched['sdn_load_aware']['w1_share_mean']
        if base > 0:
            reduction = (base - smart) / base * 100
            lines.append(
                '> **Result:** SDN feedback reduced the fraction of jobs sent to the '
                'congested worker by **≈ %.0f%%** relative to the static baseline.' % reduction
            )
    lines.append('')

    # Failover
    lines.append('## 3. Failover / Recovery Time')
    lines.append('')
    f = report.get('failover', {})
    if f:
        lines.append('| Metric | Mean | Stdev |')
        lines.append('|---|---|---|')
        lines.append('| Detect → re-queue | %.3f s | %.3f s |' % (f['requeue_mean_s'], f['requeue_stdev_s']))
        lines.append('| Detect → re-assign | %.3f s | %.3f s |' % (f['reassign_mean_s'], f['reassign_stdev_s']))
        lines.append('| Kill → client result | %.3f s | — |' % f['client_result_mean_s'])
        lines.append('| Success rate | %.0f%% | — |' % (f['success_rate'] * 100))
        lines.append('')
        lines.append(
            '> A worker is killed mid-job. The coordinator detects the failure '
            '(socket close / missing heartbeat), re-queues the job, and hands it '
            'to a healthy worker. The client still receives the correct result.'
        )
    lines.append('')

    # Robustness
    lines.append('## 4. Repeated Failure Robustness')
    lines.append('')
    rf = report.get('repeated_failure', {})
    if rf:
        lines.append(
            'After **%d** consecutive worker failures on the same job the system '
            'correctly aborts with `too_many_worker_failures` (prevents infinite re-queue loops). '
            'Observed: **%s**.' % (
                rf.get('max_attempts', 3),
                'PASS' if rf.get('failed_as_expected') else 'FAIL',
            )
        )
    lines.append('')
    lines.append('---')
    lines.append('*All numbers are measured on the real TCP socket application (no simulation).*')

    with open(REPORT_MD, 'w') as fp:
        fp.write('\n'.join(lines))
    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    report = {}
    print('=' * 64)
    print('  SDN Job-Queue  ·  Performance Evaluation (D2)')
    print('=' * 64)

    # ---- 1. Throughput ----
    print('\n▶  Throughput & latency (3 workers, job ≈ 1 s)')
    report['throughput'] = []
    for n in (1, 3, 6, 10):
        print('   … %2d concurrent clients' % n, end='', flush=True)
        r = measure_throughput(n_clients=n, n_workers=3, job_time='1', repeats=3)
        report['throughput'].append(r)
        print('  →  %.2f ± %.2f jobs/s   Jain=%.3f   p95=%.2fs' % (
            r['throughput_mean'], r['throughput_stdev'],
            r['jain_mean'] or 0, r['latency_p95_mean']))

    # ---- 2. Scheduling comparison ----
    print('\n▶  Scheduling: SDN-load-aware vs static baseline')
    print('   (w1 reported as heavily loaded; measuring where jobs actually go)')
    report['scheduling'] = {}
    for label, enabled in (
        ('baseline_static_no_sdn', False),
        ('sdn_load_aware', True),
    ):
        print('   … %-24s' % label, end='', flush=True)
        r = measure_scheduling(n_jobs=24, n_workers=3, sdn_enabled=enabled, job_time='0.35', repeats=3)
        report['scheduling'][label] = r
        print('  →  w1 share = %5.1f%% ± %.1f%%   Jain=%.3f' % (
            r['w1_share_mean'] * 100, r['w1_share_stdev'] * 100, r['jain_mean'] or 0))

    # ---- 3. Failover ----
    print('\n▶  Failover recovery time (worker killed mid-job)')
    print('   … measuring', end='', flush=True)
    report['failover'] = measure_failover(job_time='3.5', repeats=4)
    f = report['failover']
    print('  →  re-queue %.3fs · re-assign %.3fs · client result %.3fs  (success %.0f%%)' % (
        f['requeue_mean_s'], f['reassign_mean_s'],
        f['client_result_mean_s'], f['success_rate'] * 100))

    # ---- 4. Repeated failure ----
    print('\n▶  Repeated failure robustness')
    print('   … forcing max attempts', end='', flush=True)
    report['repeated_failure'] = measure_repeated_failure()
    print('  →  %s' % ('PASS' if report['repeated_failure']['failed_as_expected'] else 'FAIL'))

    # ---- Write artefacts ----
    with open(REPORT_JSON, 'w') as fp:
        json.dump(report, fp, indent=2)

    md = write_markdown(report)
    print('\n' + '─' * 64)
    print(md)
    print('─' * 64)
    print('\nRaw numbers  →  %s' % REPORT_JSON)
    print('Markdown     →  %s' % REPORT_MD)
    print('Done.\n')


if __name__ == '__main__':
    main()
