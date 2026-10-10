from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel


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

    CLI(net)

    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    create_topology()
