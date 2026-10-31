#!/usr/bin/env python3
"""
D2 performance evaluation.

Runs the real coordinator/worker/client scripts on 127.0.0.1 (same harness
style as test_sockets.py -- no Mininet/Ryu needed, so this is easy to run
repeatedly while tuning) and measures:

  1. Throughput: jobs/sec completed as the number of concurrent clients grows.
  2. Scheduling comparison: the SDN-load-aware scheduler vs. a naive
     round-robin/static baseline that ignores load, both fed the SAME skewed
     per-worker load so the difference is attributable to the scheduling
     policy alone -- reported as the spread (max-min) of per-worker job
     counts and each worker's share of the jobs.
  3. Failover / recovery time: wall-clock time between a worker dying and
     the coordinator finishing re-queuing + reassigning its in-flight job.

Usage:
    python3 tests/test_performance.py

Prints a small report to stdout; also writes raw numbers to
/tmp/sdn_perf_report.json for the writeup.
"""
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time

SCRIPTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tcp_socket')
REPORT_PATH = '/tmp/sdn_perf_report.json'


def free_port():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    p = s.getsockname()[1]
    s.close()
    return p


def spawn(script, *args, env=None, stdout=subprocess.PIPE):
    return subprocess.Popen([sys.executable, '-u', os.path.join(SCRIPTS, script), *args],
                            env=env, stdout=stdout, stderr=subprocess.STDOUT, text=True)


def wait_port(port, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(('127.0.0.1', port), 0.2).close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


class Harness:
    """One coordinator + N workers on 127.0.0.1, no SDN controller attached
    (the coordinator runs fine without one -- LOAD figures just stay at 0,
    so the scheduler falls back to picking the first free worker)."""

    def __init__(self, n_workers=2, job_time='1'):
        self.tmp = tempfile.mkdtemp()
        self.port = free_port()
        self.env = dict(os.environ, COORD_HOST='127.0.0.1', COORD_PORT=str(self.port),
                        SDN_CTL_SOCK=os.path.join(self.tmp, 'ctl.sock'),
                        JOB_TIME_MIN=job_time, JOB_TIME_MAX=job_time, PYTHONUNBUFFERED='1')
        self.log_path = os.path.join(self.tmp, 'coord.log')
        self.log_file = open(self.log_path, 'w')
        self.procs = [spawn('simple_coordinator.py', env=self.env, stdout=self.log_file)]
        if not wait_port(self.port):
            raise RuntimeError('coordinator did not start')
        self.workers = {}
        for i in range(1, n_workers + 1):
            wid = 'w%d' % i
            self.workers[wid] = spawn('simple_worker.py', wid, env=self.env, stdout=subprocess.DEVNULL)
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
            time.sleep(0.05)
        raise RuntimeError('timed out waiting for %r in coordinator log' % text)

    def client(self, n):
        return spawn('simple_client.py', str(n), env=self.env)

    def stop(self):
        for p in list(self.workers.values()) + self.procs:
            if p.poll() is None:
                p.kill()
        for p in list(self.workers.values()) + self.procs:
            p.wait()
        self.log_file.close()


# -------------------------------------------------------------------------
# 1 & 2. Throughput + scheduling balance
# -------------------------------------------------------------------------
def measure_throughput(n_clients, n_workers=3, job_time='1'):
    h = Harness(n_workers=n_workers, job_time=job_time)
    try:
        t0 = time.time()
        procs = [h.client(i) for i in range(n_clients)]
        ok = 0
        for p in procs:
            rc = p.wait(timeout=30)
            ok += (rc == 0)
        elapsed = time.time() - t0

        # per-worker job counts, for the balance figure
        counts = {}
        for m in re.finditer(r'-> assigned to worker (\w+)', h.log()):
            counts[m.group(1)] = counts.get(m.group(1), 0) + 1

        return {
            'n_clients': n_clients, 'n_workers': n_workers, 'completed': ok,
            'elapsed_s': round(elapsed, 3),
            'throughput_jobs_per_s': round(ok / elapsed, 2) if elapsed else None,
            'per_worker_counts': counts,
            'balance_spread': (max(counts.values()) - min(counts.values())) if counts else None,
        }
    finally:
        h.stop()


# -------------------------------------------------------------------------
# 1b. Scheduling comparison: SDN-load-aware vs. a static/naive baseline
# -------------------------------------------------------------------------
class FakeController:
    """Stands in for controller.py on the UNIX app<->SDN socket. Sends a
    fixed, skewed LOAD figure per worker so we can compare scheduling
    policies under an identical (known) load pattern, without needing
    Mininet/Ryu for this specific comparison."""

    def __init__(self, ctl_sock_path, loads, enabled=True):
        self.path = ctl_sock_path
        self.loads = loads          # {"w1": 9000, "w2": 0, "w3": 0, ...}
        self.enabled = enabled      # False = baseline: never reply, so
                                     # worker_load stays empty (naive/static)
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

    def _serve(self):
        self._srv.settimeout(1.0)
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
                time.sleep(0.3)
            except OSError:
                conn = None

    def stop(self):
        self._stop = True
        if self._srv:
            self._srv.close()


def measure_scheduling(n_jobs=12, n_workers=3, sdn_enabled=True, job_time='1'):
    """Submit n_jobs sequentially (one at a time, so each dispatch decision
    is independently made by the scheduler) with worker w1 reported as
    heavily loaded. With the real SDN-aware scheduler this should steer
    jobs away from w1; with the baseline (no controller reply, i.e. the
    static/naive case) load is never known so jobs land on whichever free
    worker comes first every time."""
    h = Harness(n_workers=n_workers, job_time=job_time)
    loads = {'w1': 9_000_000}  # w1 looks saturated; w2, w3 implicitly 0
    ctrl = FakeController(h.env['SDN_CTL_SOCK'], loads, enabled=sdn_enabled)
    ctrl.start()
    try:
        if sdn_enabled:
            time.sleep(1.0)  # let the first LOAD figures arrive before jobs start
        for i in range(n_jobs):
            p = h.client(i)
            p.wait(timeout=15)

        counts = {}
        for m in re.finditer(r'-> assigned to worker (\w+)', h.log()):
            counts[m.group(1)] = counts.get(m.group(1), 0) + 1

        return {
            'sdn_enabled': sdn_enabled, 'n_jobs': n_jobs,
            'per_worker_counts': counts,
            'w1_share': round(counts.get('w1', 0) / n_jobs, 2),
            'balance_spread': (max(counts.values()) - min(counts.values())) if counts else None,
        }
    finally:
        ctrl.stop()
        h.stop()


# -------------------------------------------------------------------------
# 3. Failover / recovery time
# -------------------------------------------------------------------------
def measure_failover(job_time='4'):
    h = Harness(n_workers=2, job_time=job_time)
    try:
        c1 = h.client(5)
        h._wait_log('assigned to worker w1')
        t_kill = time.time()
        h.workers['w1'].kill()
        h._wait_log('was running on w1 -> put back in the queue')
        h._wait_log('job1 -> assigned to worker w2', timeout=15)
        t_reassigned = time.time()
        rc = c1.wait(timeout=30)
        t_done = time.time()
        return {
            'job_time_s': job_time,
            'detect_to_reassign_s': round(t_reassigned - t_kill, 3),
            'kill_to_client_result_s': round(t_done - t_kill, 3),
            'client_rc': rc,
        }
    finally:
        h.stop()


def main():
    report = {}

    print('=== Throughput (load-aware scheduler, 3 workers) ===')
    report['throughput'] = []
    for n in (1, 3, 6, 10):
        r = measure_throughput(n_clients=n, n_workers=3, job_time='1')
        report['throughput'].append(r)
        print('  %2d concurrent clients -> %5.2f jobs/s   (balance spread across workers: %s)'
              % (n, r['throughput_jobs_per_s'] or 0, r['balance_spread']))

    print()
    print('=== Scheduling: SDN-load-aware vs. static/naive baseline ===')
    print('  (w1 is reported as heavily loaded; a good scheduler should avoid it)')
    report['scheduling'] = {}
    for label, enabled in (('baseline_static_no_sdn', False), ('sdn_load_aware', True)):
        r = measure_scheduling(n_jobs=12, n_workers=3, sdn_enabled=enabled)
        report['scheduling'][label] = r
        print('  %-24s -> per-worker counts: %-28s  w1 share: %4.0f%%   spread: %s'
              % (label, r['per_worker_counts'], r['w1_share'] * 100, r['balance_spread']))

    print()
    print('=== Failover / recovery time (worker killed mid-job) ===')
    report['failover'] = measure_failover()
    f = report['failover']
    print('  worker killed -> job reassigned to backup worker in %.3fs' % f['detect_to_reassign_s'])
    print('  worker killed -> client received its (re-run) result in %.3fs' % f['kill_to_client_result_s'])

    with open(REPORT_PATH, 'w') as fp:
        json.dump(report, fp, indent=2)
    print()
    print('Full numbers written to', REPORT_PATH)


if __name__ == '__main__':
    main()
