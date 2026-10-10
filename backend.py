#!/usr/bin/env python3
"""
GUI bridge: runs your REAL Mininet topology, Ryu controller and tcp_socket scripts,
and streams their output + the real packets to jobqueue-sdn-ui.html.

Put this file and jobqueue-sdn-ui.html next to controller.py and tcp_socket/, then:

    sudo python3 backend.py --ryu "/home/<you>/ryu-env/bin/ryu-manager controller.py"

and open http://localhost:8000 in a browser. (sudo is needed by Mininet and tcpdump.)
"""
import argparse, json, os, queue, shlex, signal, socket, struct, subprocess, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
PIPE, STDOUT = subprocess.PIPE, subprocess.STDOUT


# ---------- read a pcap stream (from tcpdump -w -) and report every TCP segment ----------
def parse_pcap(stream, on_tcp):
    hdr = stream.read(24)
    if len(hdr) < 24:
        return
    e = '<' if struct.unpack('<I', hdr[:4])[0] in (0xa1b2c3d4, 0xa1b23c4d) else '>'
    while True:
        ph = stream.read(16)
        if len(ph) < 16:
            return
        incl = struct.unpack(e + 'IIII', ph)[2]
        data = stream.read(incl)
        if len(data) < incl:
            return
        if len(data) < 54 or data[12:14] != b'\x08\x00':      # Ethernet + IPv4 only
            continue
        ip = data[14:]
        ihl = (ip[0] & 15) * 4
        if ip[9] != 6:                                          # TCP only
            continue
        src, dst = socket.inet_ntoa(ip[12:16]), socket.inet_ntoa(ip[16:20])
        tcp = ip[ihl:]
        flags, off = tcp[13], (tcp[12] >> 4) * 4
        total = struct.unpack('>H', ip[2:4])[0]
        sport, dport, seq = struct.unpack('>HHI', tcp[0:2] + tcp[2:4] + tcp[4:8])
        on_tcp(src, dst, flags, ip[ihl + off:total], (sport, dport, seq))


def kill_old_ryu():
    """Kill leftover ryu-manager processes, but never ourselves (our own command line contains 'ryu-manager' too)."""
    mine = {os.getpid(), os.getppid()}
    for pid in os.listdir('/proc'):
        if not pid.isdigit() or int(pid) in mine:
            continue
        try:
            cmd = open('/proc/%s/cmdline' % pid, 'rb').read().replace(b'\0', b' ').decode(errors='replace')
        except OSError:
            continue
        if 'ryu-manager' in cmd and 'backend.py' not in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except OSError:
                pass


def hard_cleanup(log=lambda m: None):
    """Remove everything a previous (crashed) run may have left behind."""
    try:
        from mininet.clean import cleanup
        cleanup()
    except Exception as e:
        log('mininet cleanup: ' + str(e))
    # stale veth ends (h1-eth0, s1-eth1, ...) that mn -c sometimes misses
    try:
        out = subprocess.run(['ip', '-o', 'link', 'show'], stdout=PIPE, stderr=PIPE, text=True).stdout
        import re
        for name in set(re.findall(r'\d+: ((?:h\d+|s\d+)-eth\d+)', out)):
            subprocess.run(['ip', 'link', 'delete', name], stdout=PIPE, stderr=PIPE)
            log('removed stale interface ' + name)
    except Exception as e:
        log('interface cleanup: ' + str(e))
    # leftover OVS bridge / controller / tcpdump from earlier runs
    kill_old_ryu()
    for cmd in (['ovs-vsctl', '--if-exists', 'del-br', 's1'], ['pkill', '-x', 'tcpdump']):
        try:
            subprocess.run(cmd, stdout=PIPE, stderr=PIPE)
        except Exception:
            pass
    time.sleep(0.5)


class Bridge:
    def __init__(self, args):
        self.a, self.subs, self.net, self.sw = args, [], None, None
        self.procs, self.info, self.frozen, self.linkdown = {}, {}, set(), set()
        self.ctl = self.dump = None
        self.starting = False
        self.seen = {}
        self._start_lock = threading.Lock()

    # ----- events to the browser -----
    def emit(self, **ev):
        data = 'data: ' + json.dumps(ev) + '\n\n'
        for q in list(self.subs):
            q.put(data)

    def pipe(self, host, proc, track=False):
        def run():
            for line in proc.stdout:
                if line.strip():
                    self.emit(type='log', host=host, line=line.rstrip())
            proc.wait()
            if track and self.procs.get(host) is proc:
                self.emit(type='state', host=host, alive=False, frozen=False)
        threading.Thread(target=run, daemon=True).start()

    def on_tcp(self, src, dst, flags, payload, key=None):
        if payload and key:
            now = time.time()
            k = (src, dst) + key
            if k in self.seen:                      # retransmission / duplicate copy of the same segment
                return
            self.seen[k] = now
            if len(self.seen) > 4000:
                for old in [x for x, t in self.seen.items() if now - t > 30]:
                    del self.seen[old]
        if payload:
            for line in payload.decode(errors='replace').split('\n'):
                if line.strip():
                    self.emit(type='pkt', src=src, dst=dst, label=line.strip())
        elif flags & 0x02 and not flags & 0x10:
            self.emit(type='pkt', src=src, dst=dst, label='SYN')
        elif flags & 0x01:
            self.emit(type='pkt', src=src, dst=dst, label='FIN')
        elif flags & 0x04:
            self.emit(type='pkt', src=src, dst=dst, label='RST')

    # ----- starting / stopping the real environment -----
    def start(self, nodes):
        from mininet.net import Mininet
        from mininet.node import RemoteController, OVSSwitch
        # Clean previous run without telling the UI we "stopped" (avoids dimmed screen mid-start)
        if self.net or self.ctl or self.procs:
            self.stop(silent=True)
        self.emit(type='log', host='c0', line='cleaning up old Mininet/OVS/Ryu state...')
        hard_cleanup(lambda m: self.emit(type='log', host='c0', line=m))
        self.emit(type='log', host='c0', line='starting Ryu: ' + self.a.ryu)
        env = dict(os.environ, PYTHONUNBUFFERED='1')
        self.ctl = subprocess.Popen(shlex.split(self.a.ryu), cwd=HERE, stdout=PIPE, stderr=STDOUT, text=True, env=env)
        self.pipe('c0', self.ctl)
        self.emit(type='log', host='c0', line='waiting 3s for Ryu to listen on :6633...')
        time.sleep(3)
        self.emit(type='log', host='c0', line='building Mininet topology (s1 + hosts)...')
        net = Mininet(controller=None, switch=OVSSwitch, autoSetMacs=True)
        net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)
        self.sw = net.addSwitch('s1', protocols='OpenFlow13')
        self.info = {}
        for n in nodes:
            h = net.addHost(n['host'], ip=n['ip'] + '/24')
            link = net.addLink(h, self.sw)
            self.info[n['host']] = dict(n, iface=link.intf2.name)
        net.start()
        self.net = net
        self.emit(type='log', host='c0', line='Mininet started, waiting for switch to connect...')
        try:
            net.waitConnected()
            self.emit(type='log', host='c0', line='switch connected to Ryu')
        except Exception as e:
            self.emit(type='log', host='c0', line='waitConnected: ' + str(e) + ' (continuing)')
            time.sleep(2)
        coord = [n for n in nodes if n['type'] == 'coord'][0]
        # every message passes the coordinator's switch port, so one capture sees all traffic
        self.dump = subprocess.Popen(['tcpdump', '-U', '-n', '-s0', '-i', self.info[coord['host']]['iface'],
                                      '-w', '-', 'tcp', 'port', '6000'], stdout=PIPE, stderr=subprocess.DEVNULL)
        threading.Thread(target=parse_pcap, args=(self.dump.stdout, self.on_tcp), daemon=True).start()
        self.emit(type='log', host='c0', line='launching coordinator on ' + coord['host'] + '...')
        self.launch(coord)
        time.sleep(1)
        for n in nodes:
            if n['type'] == 'worker':
                self.emit(type='log', host='c0', line='launching worker ' + n.get('wid', n['host']) + '...')
                self.launch(n)
                time.sleep(0.4)
        # Warm ARP / routes so early client connects don't see ENETUNREACH
        self.emit(type='log', host='c0', line='warming ARP (ping hosts)...')
        try:
            coord_host = self.net.get(coord['host'])
            for n in nodes:
                if n['type'] != 'coord':
                    h = self.net.get(n['host'])
                    h.cmd('ping -c 1 -W 1 ' + coord['ip'] + ' >/dev/null 2>&1 || true')
            coord_host.cmd('ping -c 1 -W 1 10.0.0.1 >/dev/null 2>&1 || true')
        except Exception as e:
            self.emit(type='log', host='c0', line='ping warmup: ' + str(e))
        self.emit(type='log', host='c0', line='network ready — you can send jobs now')
        self.emit(type='ready')

    def launch(self, n):
        script = {'coord': 'simple_coordinator.py', 'worker': 'simple_worker.py'}[n['type']]
        cmd = ['python3', '-u', os.path.join(self.a.scripts, script)]
        if n['type'] == 'worker':
            cmd.append(n['wid'])
        p = self.net.get(n['host']).popen(cmd, stdout=PIPE, stderr=STDOUT, text=True)
        self.procs[n['host']] = p
        self.frozen.discard(n['host'])
        self.pipe(n['host'], p, track=True)
        self.emit(type='state', host=n['host'], alive=True, frozen=False)

    def stop(self, silent=False):
        for p in list(self.procs.values()) + [self.dump, self.ctl]:
            if p and p.poll() is None:
                try:
                    p.kill()
                except Exception:
                    pass
        if self.net:
            try:
                self.net.stop()
            except Exception:
                pass
        self.net, self.procs, self.info = None, {}, {}
        self.frozen, self.linkdown = set(), set()
        self.seen = {}
        if not silent:
            self.emit(type='stopped')

    # ----- actions called by the GUI -----
    def host(self, b):
        h = b['host']
        if h not in self.info:
            raise ValueError('unknown host ' + str(h))
        return h

    def do_run(self, b):
        if not self._start_lock.acquire(blocking=False):
            self.emit(type='log', host='c0', line='already starting — ignore extra Run click')
            return
        def run_in_bg():
            self.starting = True
            try:
                self.start(b['nodes'])
            except Exception as e:
                import traceback
                self.emit(type='log', host='c0', line='backend error: ' + str(e))
                self.emit(type='log', host='c0', line=traceback.format_exc().strip().split('\n')[-1])
                try:
                    hard_cleanup()
                except Exception:
                    pass
                self.emit(type='stopped')
            finally:
                self.starting = False
                self._start_lock.release()
        threading.Thread(target=run_in_bg, daemon=True).start()

    def do_stop(self, b):
        self.stop()

    def do_job(self, b):
        if not self.net:
            raise RuntimeError('network not ready yet — wait for "network ready" then try again')
        h, n = self.host({'host': b['client']}), int(b['n'])
        p = self.net.get(h).popen(['python3', '-u', os.path.join(self.a.scripts, 'simple_client.py'), str(n)],
                                  stdout=PIPE, stderr=STDOUT, text=True)
        self.pipe(h, p)          # returns immediately: every job runs as its own process, fully in parallel

    def do_kill(self, b):
        p = self.procs.get(self.host(b))
        if p and p.poll() is None:
            if b['host'] in self.frozen:
                os.kill(p.pid, signal.SIGCONT)
            p.kill()
        self.emit(type='state', host=b['host'], alive=False, frozen=False)

    def do_freeze(self, b):
        h = self.host(b)
        p = self.procs.get(h)
        if not p or p.poll() is not None:
            return
        if h in self.frozen:
            os.kill(p.pid, signal.SIGCONT)
            self.frozen.discard(h)
        else:
            os.kill(p.pid, signal.SIGSTOP)
            self.frozen.add(h)
        self.emit(type='state', host=h, alive=True, frozen=h in self.frozen)

    def do_link(self, b):
        # Toggle the s1<->host link. OVS reports PORT_STATUS to Ryu, which tells the coordinator (LINK_DOWN).
        h = self.host(b)
        if not self.net:
            raise RuntimeError('network not running')
        down = h not in self.linkdown
        self.net.configLinkStatus('s1', h, 'down' if down else 'up')
        (self.linkdown.add if down else self.linkdown.discard)(h)
        self.emit(type='link', host=h, down=down)

    def do_revive(self, b):
        self.launch(self.info[self.host(b)])

    def do_remove(self, b):
        self.do_kill(b)
        self.procs.pop(b['host'], None)

    def do_add(self, b):                       # add a client/worker while the network is running
        h = self.net.addHost(b['host'], ip=b['ip'] + '/24')
        link = self.net.addLink(h, self.sw)
        self.sw.attach(link.intf2)
        link.intf1.setIP(b['ip'], 24)
        link.intf1.ifconfig('up')
        link.intf2.ifconfig('up')
        self.info[b['host']] = dict(b, iface=link.intf2.name)
        if b['type'] == 'worker':
            self.launch(b)


B = None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def reply(self, code, body, ctype='application/json'):
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path == '/api/ping':
            return self.reply(200, {'ok': True})
        if self.path == '/events':
            q = queue.Queue()
            B.subs.append(q)
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('X-Accel-Buffering', 'no')
            self.end_headers()
            try:
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except Exception:
                pass
            try:
                while True:
                    try:
                        self.wfile.write(q.get(timeout=15).encode())
                    except queue.Empty:
                        self.wfile.write(b': keep-alive\n\n')
                    self.wfile.flush()
            except Exception:
                pass
            finally:
                B.subs.remove(q)
            return
        with open(B.a.ui, 'rb') as f:
            self.reply(200, f.read(), 'text/html; charset=utf-8')

    def do_POST(self):
        try:
            body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))) or b'{}')
            fn = getattr(B, 'do_' + self.path.rsplit('/', 1)[-1], None)
            if not fn:
                raise ValueError('unknown action')
            fn(body)
            self.reply(200, {'ok': True})
        except Exception as e:
            self.reply(200, {'ok': False, 'error': str(e)})


def main():
    global B
    ap = argparse.ArgumentParser()
    ap.add_argument('--ryu', default='ryu-manager controller.py', help='command that starts Ryu (run from this folder)')
    ap.add_argument('--scripts', default=os.path.join(HERE, 'tcp_socket'))
    ap.add_argument('--ui', default=os.path.join(HERE, 'jobqueue-sdn-ui.html'))
    ap.add_argument('--port', type=int, default=8000)
    a = ap.parse_args()
    if os.geteuid() != 0:
        sys.exit('Run with sudo (Mininet and tcpdump need root).')
    hard_cleanup(print)
    B = Bridge(a)
    print('GUI at http://localhost:%d  (Ctrl+C to quit)' % a.port)
    try:
        ThreadingHTTPServer(('0.0.0.0', a.port), Handler).serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        B.stop()


if __name__ == '__main__':
    main()
