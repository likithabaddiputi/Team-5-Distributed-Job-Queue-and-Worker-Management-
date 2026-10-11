import os
import time

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel

# Folder containing simple_coordinator.py, simple_worker.py, simple_client.py
SCRIPTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tcp_socket')

LOG_DIR = '/tmp/sdn_logs'


def create_topology():

    net = Mininet(
        controller=None,
        switch=OVSSwitch,
        autoSetMacs=True
    )

    # Ryu controller
    c0 = net.addController(
        'c0',
        controller=RemoteController,
        ip='127.0.0.1',
        port=6633
    )

    # Hosts
    h1 = net.addHost('h1', ip='10.0.0.1/24')
    h2 = net.addHost('h2', ip='10.0.0.2/24')
    h3 = net.addHost('h3', ip='10.0.0.3/24')
    h4 = net.addHost('h4', ip='10.0.0.4/24')

    # Switch
    s1 = net.addSwitch(
        's1',
        protocols='OpenFlow13'
    )

    # Links
    net.addLink(h1, s1)
    net.addLink(h2, s1)
    net.addLink(h3, s1)
    net.addLink(h4, s1)

    net.start()

    print("\n*** Network started ***")
    print("h1 = Client       = 10.0.0.1")
    print("h2 = Coordinator  = 10.0.0.2")
    print("h3 = Worker 1     = 10.0.0.3")
    print("h4 = Worker 2     = 10.0.0.4")
    print("s1 = OVS Switch")
    print("c0 = Ryu Controller = 127.0.0.1:6633")
    print()

    # ---- Auto-launch coordinator + workers in the background ----
    os.makedirs(LOG_DIR, exist_ok=True)

    print("*** Starting coordinator on h2 (log: %s/coord.log) ***" % LOG_DIR)
    h2.cmd('python3 -u %s/simple_coordinator.py > %s/coord.log 2>&1 &' % (SCRIPTS, LOG_DIR))
    time.sleep(1)

    print("*** Starting worker w1 on h3 (log: %s/w1.log) ***" % LOG_DIR)
    h3.cmd('python3 -u %s/simple_worker.py w1 > %s/w1.log 2>&1 &' % (SCRIPTS, LOG_DIR))
    time.sleep(0.5)

    print("*** Starting worker w2 on h4 (log: %s/w2.log) ***" % LOG_DIR)
    h4.cmd('python3 -u %s/simple_worker.py w2 > %s/w2.log 2>&1 &' % (SCRIPTS, LOG_DIR))
    time.sleep(0.5)

    print()
    print("*** Everything is running in the background. ***")
    print("    Watch any of them live with, e.g.:")
    print("        tail -f %s/coord.log" % LOG_DIR)
    print("        tail -f %s/w1.log" % LOG_DIR)
    print("        tail -f %s/w2.log" % LOG_DIR)
    print()
    print("*** To submit a job, either:")
    print("    - type it right here:  mininet> h1 python3 %s/simple_client.py 7" % SCRIPTS)
    print("    - or open a client xterm:  mininet> xterm h1")
    print()

    CLI(net)
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    create_topology()
