#!/usr/bin/env python3
"""
End-to-end test in the REAL environment: Mininet + Open vSwitch + Ryu + the tcp_socket scripts.

  Terminal 1:  cd ~/sdn-project && ryu-manager controller.py
  Terminal 2:  cd ~/sdn-project && sudo python3 tests/test_mininet.py

Prints PASS/FAIL per check and exits non-zero if any check fails.

D2: topology is now s1 (h1 client, h2 coordinator) <-> s2 (h3 w1, h4 w2),
joined by a redundant PRIMARY + BACKUP link. The new checks at the end
bring down only the primary backbone link (never touching worker links)
and confirm Ryu reroutes flows onto the backup link without the
coordinator/app layer ever being told anything failed.
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


def flows(sw='s1'):
    return subprocess.run(['ovs-ofctl', '-O', 'OpenFlow13', 'dump-flows', sw],
                          stdout=subprocess.PIPE, text=True).stdout


def client(h, n):
    return h.cmd('python3 -u %s/simple_client.py %s' % (SCRIPTS, n))


def main():
    setLogLevel('warning')
    net = Mininet(controller=None, switch=OVSSwitch, autoSetMacs=True)
    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)
    s1 = net.addSwitch('s1', protocols='OpenFlow13')
    s2 = net.addSwitch('s2', protocols='OpenFlow13')
    h1 = net.addHost('h1', ip='10.0.0.1/24')     # client   -> s1
    h2 = net.addHost('h2', ip='10.0.0.2/24')     # coord    -> s1
    h3 = net.addHost('h3', ip='10.0.0.3/24')     # worker 1 -> s2
    h4 = net.addHost('h4', ip='10.0.0.4/24')     # worker 2 -> s2
    net.addLink(h1, s1)
    net.addLink(h2, s1)
    net.addLink(h3, s2)
    net.addLink(h4, s2)
    primary_link = net.addLink(s1, s2)    # port 3 on both switches
    backup_link = net.addLink(s1, s2)     # port 4 on both switches

    procs = []
    try:
        net.start()
        check('s1 and s2 connected to Ryu', _connected(net))
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

        check('SDN: job-traffic flows installed on s1 (TCP port 6000, cookie-tagged)',
              wait_until(lambda: 'tp_src=6000' in flows('s1') and 'tp_dst=6000' in flows('s1'), 15),
              'see: ovs-ofctl -O OpenFlow13 dump-flows s1')

        out1 = client(h1, 7)
        check('job from h1 completes with the right answer (7*7=49)', '= 49' in out1, out1.strip().splitlines()[-1] if out1.strip() else '')

        check('SDN -> app: coordinator receives non-zero LOAD measured from flow stats',
              wait_until(lambda: re.search(r'load: [1-9]\d* B/s', client_and_read(h1)) is not None, 25))

        # ---- original D1-style failure test: a WORKER's own link goes down ----
        net.configLinkStatus('s2', 'h3', 'down')
        check('SDN -> app: Ryu PORT_STATUS makes the coordinator fail w1 (link down)',
              wait_until(lambda: 'link to w1 is DOWN' in read(OUT), 10))
        before = len(read(OUT))
        out2 = client(h1, 9)
        check('job submitted after the worker-link failure still completes on w2 (9*9=81)', '= 81' in out2)
        check('...and it was scheduled to w2', 'assigned to worker w2' in read(OUT)[before:])
        net.configLinkStatus('s2', 'h3', 'up')

        out3 = client(h1, 'abc')
        check('bad input is rejected, system keeps running', 'rejected' in out3 and '= 49' in client(h1, 7))

        # ---- D2: bring down only the PRIMARY s1<->s2 link, keep backup up ----
        # net.configLinkStatus('s1','s2',...) would hit BOTH parallel links at
        # once, so go straight to the primary link's own interfaces instead.
        before = len(read(OUT))
        primary_link.intf1.ifconfig('down')
        primary_link.intf2.ifconfig('down')
        time.sleep(3)   # give OVS/Ryu time to notice and reroute

        out4 = client(h1, 11)
        check('D2: job submitted right after primary-backbone failure still completes (11*11=121) '
              '-- i.e. SDN rerouted onto the backup link', '= 121' in out4)
        check('D2: no WORKER_DOWN/coordinator failure was triggered by the backbone outage '
              '(the app layer should never notice)', 'link to w1 is DOWN' not in read(OUT)[before:]
              and 'link to w2 is DOWN' not in read(OUT)[before:])
        check('D2: flows on s1 now point out the backup port (port 4), not the dead primary (port 3)',
              wait_until(lambda: 'output:4' in flows('s1') and re.search(r'cookie=0x[1-9a-f].*output:3', flows('s1')) is None, 10),
              'see: ovs-ofctl -O OpenFlow13 dump-flows s1')

        primary_link.intf1.ifconfig('up')
        primary_link.intf2.ifconfig('up')
        time.sleep(3)
        out5 = client(h1, 12)
        check('D2: job after the backbone link is restored still completes (12*12=144)', '= 144' in out5)

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
