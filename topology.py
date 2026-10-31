import os
import time

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel

# Folder containing simple_coordinator.py, simple_worker.py, simple_client.py
SCRIPTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tcp_socket')

LOG_DIR = '/tmp/sdn_logs'

# D2: two switches joined by a redundant pair of links, instead of the D1
# single-switch layout. h1 (client) and h2 (coordinator) sit behind s1;
# h3/h4 (workers) sit behind s2, so every job's TCP traffic now has to
# cross the s1<->s2 backbone, which is exactly what makes rerouting on
# link failure a real, observable thing (controller.py handles the
# failover logic; see PRIMARY_PORT/BACKUP_PORT there).
#
# Link-add order fixes the port numbers the controller relies on:
#   s1: port1->h1, port2->h2, port3->primary(s1-s2), port4->backup(s1-s2)
#   s2: port1->h3, port2->h4, port3->primary(s1-s2), port4->backup(s1-s2)


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

    # Switches
    s1 = net.addSwitch('s1', protocols='OpenFlow13')
    s2 = net.addSwitch('s2', protocols='OpenFlow13')

    # Host links (ports 1-2 on each switch)
    net.addLink(h1, s1)
    net.addLink(h2, s1)
    net.addLink(h3, s2)
    net.addLink(h4, s2)

    # Redundant backbone: PRIMARY link first (port 3 on both switches),
    # BACKUP link second (port 4 on both switches).
    #
    # IMPORTANT: Mininet's `net.configLinkStatus('s1','s2',status)` (and the
    # CLI's `link s1 s2 down`) toggles *every* link between the two nodes at
    # once -- with two parallel links that would kill both simultaneously
    # and there would be nothing left to fail over to. So we keep direct
    # references to each link and bring down only the primary interfaces
    # (see the `py` snippet printed below).
    primary_link = net.addLink(s1, s2)   # primary -> port 3 on both switches
    backup_link = net.addLink(s1, s2)    # backup  -> port 4 on both switches
    net.primary_link = primary_link
    net.backup_link = backup_link

    net.start()

    print("\n*** Network started (D2: two switches, redundant backbone) ***")
    print("h1 = Client       = 10.0.0.1   (behind s1)")
    print("h2 = Coordinator  = 10.0.0.2   (behind s1)")
    print("h3 = Worker 1     = 10.0.0.3   (behind s2)")
    print("h4 = Worker 2     = 10.0.0.4   (behind s2)")
    print("s1 <-> s2         = two links: primary (port 3) + backup (port 4)")
    print("c0 = Ryu Controller = 127.0.0.1:6633")
    print()
    print("To demo rerouting (do NOT use `link s1 s2 down` -- it takes BOTH links down):")
    print('  mininet> py net.primary_link.intf1.ifconfig("down"); net.primary_link.intf2.ifconfig("down")')
    print("  ... jobs keep completing; Ryu failed over to the backup link ...")
    print('  mininet> py net.primary_link.intf1.ifconfig("up"); net.primary_link.intf2.ifconfig("up")')
    print("  ... Ryu fails back to the primary link ...")
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
