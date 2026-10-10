#!/usr/bin/env python3
"""
End-to-end test in the REAL environment: Mininet + Open vSwitch + Ryu + the tcp_socket scripts.

  Terminal 1:  cd ~/sdn-project && ryu-manager controller.py
  Terminal 2:  cd ~/sdn-project && sudo python3 tests/test_mininet.py

Prints PASS/FAIL per check and exits non-zero if any check fails.
"""
import os
import re
import subprocess
import sys
import time

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.log import setLogLevel

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
SCRIPTS = os.path.join(ROOT, 'tcp_socket')
OUT = '/tmp/sdn_test_coord.log'
results = []


def check(name, ok, detail=''):
    print(('PASS  ' if ok else 'FAIL  ') + name + ('   ' + detail if detail else ''))
    results.append(bool(ok))


def read(path):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return ''


def wait_until(fn, timeout):
    end = time.time() + timeout
    while time.time() < end:
        if fn():
            return True
        time.sleep(0.5)
    return False


def flows():
    return subprocess.run(['ovs-ofctl', '-O', 'OpenFlow13', 'dump-flows', 's1'],
                          stdout=subprocess.PIPE, text=True).stdout


def client(h, n):
    return h.cmd('python3 -u %s/simple_client.py %s' % (SCRIPTS, n))


def main():
    setLogLevel('warning')
    net = Mininet(controller=None, switch=OVSSwitch, autoSetMacs=True)
    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)
    s1 = net.addSwitch('s1', protocols='OpenFlow13')
    h1, h2, h3, h4 = [net.addHost('h%d' % i, ip='10.0.0.%d/24' % i) for i in (1, 2, 3, 4)]
    for h in (h1, h2, h3, h4):
        net.addLink(h, s1)
    procs = []
    try:
        net.start()
        check('switch s1 connected to Ryu', _connected(net))
        check('all hosts reach each other (pingAll)', net.pingAll(timeout=2) == 0)

        env = dict(os.environ, JOB_TIME_MIN='2', JOB_TIME_MAX='2')
        if os.path.exists(OUT):
            os.remove(OUT)
        out = open(OUT, 'w')
        procs.append(h2.popen(['python3', '-u', SCRIPTS + '/simple_coordinator.py'],
                              stdout=out, stderr=subprocess.STDOUT, env=env))
        time.sleep(1)
        for h, wid in ((h3, 'w1'), (h4, 'w2')):
            procs.append(h.popen(['python3', '-u', SCRIPTS + '/simple_worker.py', wid],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, env=env))
        check('both workers registered at the coordinator',
              wait_until(lambda: 'w1 connected' in read(OUT) and 'w2 connected' in read(OUT), 15))

        check('SDN: job-traffic flows installed for the workers (TCP port 6000, cookie-tagged)',
              wait_until(lambda: 'tp_src=6000' in flows() and 'tp_dst=6000' in flows(), 15),
              'see: ovs-ofctl -O OpenFlow13 dump-flows s1')

        out1 = client(h1, 7)
        check('job from h1 completes with the right answer (7*7=49)', '= 49' in out1, out1.strip().splitlines()[-1] if out1.strip() else '')

        check('SDN -> app: coordinator receives non-zero LOAD measured from flow stats',
              wait_until(lambda: re.search(r'load: [1-9]\d* B/s', client_and_read(h1)) is not None, 25))

        net.configLinkStatus('s1', 'h3', 'down')
        check('SDN -> app: Ryu PORT_STATUS makes the coordinator fail w1 (link down)',
              wait_until(lambda: 'link to w1 is DOWN' in read(OUT), 10))
        before = len(read(OUT))
        out2 = client(h1, 9)
        check('job submitted after the link failure still completes on the other worker (9*9=81)', '= 81' in out2)
        check('...and it was scheduled to w2', 'assigned to worker w2' in read(OUT)[before:])

        out3 = client(h1, 'abc')
        check('bad input is rejected, system keeps running', 'rejected' in out3 and '= 49' in client(h1, 7))
    finally:
        for p in procs:
            p.kill()
        net.stop()
    print('\n%d/%d checks passed' % (sum(results), len(results)))
    return 0 if all(results) else 1


def client_and_read(h):
    client(h, 1)
    return read(OUT)


def _connected(net):
    try:
        net.waitConnected(timeout=10)
        return True
    except Exception:
        return False


if __name__ == '__main__':
    if os.geteuid() != 0:
        sys.exit('Run with sudo (Mininet needs root).')
    sys.exit(main())
