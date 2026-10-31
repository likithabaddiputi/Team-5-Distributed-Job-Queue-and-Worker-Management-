#!/usr/bin/env python3
"""
Socket-layer tests (no Mininet / Ryu needed). Runs coordinator, workers and clients on
127.0.0.1 and plays the role of the Ryu controller on the UNIX-socket channel.

    python3 tests/test_sockets.py          (from the project root)
"""
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

SCRIPTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tcp_socket')


def free_port():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    p = s.getsockname()[1]
    s.close()
    return p


class FakeController(threading.Thread):
    """Stands in for Ryu: records APP->SDN messages and can push SDN->APP messages."""

    def __init__(self, path):
        super().__init__(daemon=True)
        self.received, self.conn = [], None
        self.srv = socket.socket(socket.AF_UNIX)
        self.srv.bind(path)
        self.srv.listen(1)

    def run(self):
        try:
            self.conn, _ = self.srv.accept()
        except OSError:
            return
        buf = b''
        while True:
            try:
                d = self.conn.recv(1024)
            except OSError:
                return
            if not d:
                return
            buf += d
            while b'\n' in buf:
                line, buf = buf.split(b'\n', 1)
                self.received.append(line.decode())

    def push(self, text):
        self.conn.sendall((text + '\n').encode())

    def wait_for(self, prefix, timeout=10):
        end = time.time() + timeout
        while time.time() < end:
            if any(m.startswith(prefix) for m in self.received):
                return True
            time.sleep(0.1)
        return False


class Lab(unittest.TestCase):
    JOB_TIME = '2'

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.port = free_port()
        self.env = dict(os.environ, COORD_HOST='127.0.0.1', COORD_PORT=str(self.port),
                        SDN_CTL_SOCK=os.path.join(self.tmp, 'ctl.sock'),
                        JOB_TIME_MIN=self.JOB_TIME, JOB_TIME_MAX=self.JOB_TIME, PYTHONUNBUFFERED='1')
        self.procs, self.workers = [], {}
        self.ctl = FakeController(self.env['SDN_CTL_SOCK'])
        self.ctl.start()
        self.coord_log = open(os.path.join(self.tmp, 'coord.log'), 'w')
        self.spawn('simple_coordinator.py', stdout=self.coord_log)
        self.wait_port()

    def tearDown(self):
        for p in self.procs:
            if p.poll() is None:
                p.kill()
        for p in self.procs:
            p.wait()
            if p.stdout:
                p.stdout.close()
        self.ctl.srv.close()
        if self.ctl.conn:
            self.ctl.conn.close()
        self.coord_log.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ---- helpers
    def spawn(self, script, *args, **kw):
        p = subprocess.Popen([sys.executable, '-u', os.path.join(SCRIPTS, script), *args],
                             env=self.env, stderr=subprocess.STDOUT, text=True, **kw)
        self.procs.append(p)
        return p

    def wait_port(self):
        for _ in range(50):
            try:
                socket.create_connection(('127.0.0.1', self.port), 0.2).close()
                return
            except OSError:
                time.sleep(0.1)
        self.fail('coordinator did not start')

    def log(self):
        self.coord_log.flush()
        with open(self.coord_log.name) as f:
            return f.read()

    def wait_log(self, text, timeout=15):
        end = time.time() + timeout
        while time.time() < end:
            if text in self.log():
                return True
            time.sleep(0.1)
        return False

    def start_workers(self, *ids):
        for i in ids:
            self.workers[i] = self.spawn('simple_worker.py', i, stdout=subprocess.DEVNULL)
            self.assertTrue(self.wait_log(i + ' connected'), i + ' did not register')

    def client(self, n):
        return self.spawn('simple_client.py', str(n), stdout=subprocess.PIPE)

    def finish(self, p, timeout=30):
        out, _ = p.communicate(timeout=timeout)
        return p.returncode, out

    def raw(self, text):
        s = socket.create_connection(('127.0.0.1', self.port), 3)
        s.sendall((text + '\n').encode())
        s.settimeout(3)
        reply = s.recv(100).decode().strip()
        s.close()
        return reply


class NormalOperation(Lab):
    JOB_TIME = '1'

    def test_five_concurrent_clients_get_correct_answers(self):
        self.start_workers('w1', 'w2')
        clients = {n: self.client(n) for n in (0, 1, 2, 3, 4)}
        for n, p in clients.items():
            rc, out = self.finish(p)
            self.assertEqual(rc, 0, out)
            self.assertIn('= %d' % (n * n), out)

    def test_app_events_reach_the_sdn_controller(self):
        self.start_workers('w1', 'w2')
        rc, _ = self.finish(self.client(6))
        self.assertEqual(rc, 0)
        self.assertTrue(self.ctl.wait_for('JOB|job1|'))
        got = self.ctl.received
        self.assertTrue(any(m.startswith('HELLO|') for m in got))
        self.assertIn('WORKER_UP|w1|127.0.0.1', got)
        self.assertIn('WORKER_UP|w2|127.0.0.1', got)

    def test_scheduler_uses_load_reported_by_sdn(self):
        self.start_workers('w1', 'w2')
        self.assertTrue(self.ctl.wait_for('WORKER_UP|w2'))
        self.ctl.push('LOAD|w1|90000|90000')          # controller says w1's flows are busy
        self.ctl.push('LOAD|w2|10|10')
        time.sleep(0.5)
        rc, _ = self.finish(self.client(3))
        self.assertEqual(rc, 0)
        self.assertIn('job1 -> assigned to worker w2', self.log())
        self.assertNotIn('job1 -> assigned to worker w1', self.log())


class BadInput(Lab):
    JOB_TIME = '1'

    def test_non_numeric_job_is_rejected_and_workers_survive(self):
        self.start_workers('w1', 'w2')
        for bad in ('abc', '1.5', '', '99999999999999999999'):
            rc, out = self.finish(self.client(bad) if bad else self.spawn('simple_client.py', '', stdout=subprocess.PIPE))
            self.assertEqual(rc, 2, 'n=%r -> %s' % (bad, out))
            self.assertIn('rejected', out)
        for w in self.workers.values():
            self.assertIsNone(w.poll(), 'a worker crashed on bad input')
        rc, out = self.finish(self.client(7))               # system still works afterwards
        self.assertEqual(rc, 0, out)
        self.assertIn('= 49', out)

    def test_malformed_protocol_messages_are_refused(self):
        self.assertTrue(self.raw('GARBAGE|x').startswith('ERR'))
        self.assertTrue(self.raw('SUBMIT').startswith('ERR'))
        self.assertTrue(self.raw('REGISTER|').startswith('ERR'))


class Failures(Lab):
    JOB_TIME = '3'

    def test_killed_worker_job_is_redistributed(self):
        self.start_workers('w1', 'w2')
        a, b = self.client(5), self.client(6)
        self.assertTrue(self.wait_log('assigned to worker w2'))
        self.workers['w1'].kill()                            # dies while running its job
        for p, n in ((a, 5), (b, 6)):
            rc, out = self.finish(p)
            self.assertEqual(rc, 0, out)
            self.assertIn('= %d' % (n * n), out)
        self.assertIn('Worker w1 disconnected', self.log())
        self.assertIn('was running on w1 -> put back in the queue', self.log())
        self.assertTrue(self.ctl.wait_for('WORKER_DOWN|w1'))

    def test_sdn_link_down_event_fails_over_the_job(self):
        self.start_workers('w1', 'w2')
        a, b = self.client(8), self.client(9)
        self.assertTrue(self.wait_log('assigned to worker w2'))
        self.ctl.push('LINK_DOWN|w1')                        # what Ryu sends on OFPT_PORT_STATUS
        for p, n in ((a, 8), (b, 9)):
            rc, out = self.finish(p)
            self.assertEqual(rc, 0, out)
            self.assertIn('= %d' % (n * n), out)
        self.assertIn('controller reports the link to w1 is DOWN', self.log())
        self.assertIn('Worker w1 unreachable (link down)', self.log())

    def test_worker_that_always_dies_does_not_loop_forever(self):
        # A job that loses its worker MAX_ATTEMPTS times is failed back to the client.
        self.start_workers('w1')
        c = self.client(4)
        for i in range(3):
            self.assertTrue(self.wait_log('job1 -> assigned to worker w%d' % (i + 1)), self.log())
            self.workers['w%d' % (i + 1)].kill()
            if i < 2:
                self.start_workers('w%d' % (i + 2))
        rc, out = self.finish(c, timeout=20)
        self.assertEqual(rc, 3, out)
        self.assertIn('too_many_worker_failures', out)


if __name__ == '__main__':
    unittest.main(verbosity=2)
