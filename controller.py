import os
import socket

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import arp
from ryu.lib.packet import ipv4

# The coordinator (running on h2) talks to this controller through a UNIX socket.
# Mininet hosts have their own network namespace but share the filesystem,
# so a socket *file* is reachable from both sides.
CTL_SOCK = '/tmp/sdn_ctl.sock'
APP_PORT = 6000        # TCP port used by the job queue
STATS_EVERY = 2        # seconds between flow-statistics requests

# ----------------------------------------------------------------------
# D2: topology now has TWO switches (s1, s2) joined by a REDUNDANT pair
# of links, so the controller can reroute inter-switch traffic instead
# of only reporting failure up to the application.
#
# Fixed layout built by topology.py / backend.py:
#   s1 (dpid 1): port 1 -> h1 (client), port 2 -> h2 (coordinator),
#                port 3 -> s2 (PRIMARY inter-switch link),
#                port 4 -> s2 (BACKUP inter-switch link)
#   s2 (dpid 2): port 1 -> h3 (worker w1), port 2 -> h4 (worker w2),
#                port 3 -> s1 (PRIMARY inter-switch link),
#                port 4 -> s1 (BACKUP inter-switch link)
#
# Port numbers are deterministic because topology.py adds links to each
# switch in this exact order (Mininet assigns port N to the Nth link
# added to a given switch), and dpid is derived from the switch name.
# ----------------------------------------------------------------------
S1_DPID = 1
S2_DPID = 2
INTERSWITCH_DPIDS = (S1_DPID, S2_DPID)
PRIMARY_PORT = 3
BACKUP_PORT = 4
SWITCH_PORTS = {S1_DPID: {1, 2, 3, 4}, S2_DPID: {1, 2, 3, 4}}


class SimpleController(app_manager.RyuApp):

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SimpleController, self).__init__(*args, **kwargs)

        self.mac_to_port = {}     # dpid -> {mac: port}
        self.ip_to_port = {}      # dpid -> {ip: port}   (learned from ARP/IP packets)
        self.datapaths = {}       # dpid -> datapath

        # Application knowledge, filled in by messages from the coordinator
        self.coord_ip = None
        self.workers = {}         # worker_id -> {"ip", "cookie", "jobs", "last_bytes", "flows"}
        self.next_cookie = 1
        self.app_conn = None      # socket to the coordinator

        # D2: which inter-switch link is currently carrying traffic.
        # Starts on the primary link; flips to the backup if the primary
        # goes down, and flips back once it comes back up.
        self.active_inter_port = PRIMARY_PORT

        hub.spawn(self._ctl_server)
        hub.spawn(self._stats_loop)

    # ------------------------------------------------------------------
    # Basic switch setup (unchanged idea: table-miss -> controller)
    # ------------------------------------------------------------------
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        print("Switch connected! dpid =", datapath.id)
        self.datapaths[datapath.id] = datapath

        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

        if datapath.id in INTERSWITCH_DPIDS:
            print("[D2] dpid %d treated as an inter-switch port; backup port %d "
                  "held out of flooding while the primary link (port %d) is up"
                  % (datapath.id, BACKUP_PORT, PRIMARY_PORT))

    def add_flow(self, datapath, priority, match, actions, cookie=0):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        instructions = [parser.OFPInstructionActions(
            ofproto.OFPIT_APPLY_ACTIONS, actions)]

        flow_mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=priority,
            match=match,
            instructions=instructions,
            cookie=cookie
        )
        datapath.send_msg(flow_mod)

    def _delete_flows_out_port(self, datapath, out_port):
        """Flush every flow entry on this switch that outputs via out_port."""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        datapath.send_msg(parser.OFPFlowMod(
            datapath=datapath, command=ofproto.OFPFC_DELETE,
            out_port=out_port, out_group=ofproto.OFPG_ANY,
            cookie=0, cookie_mask=0,
            match=parser.OFPMatch()))

    # ------------------------------------------------------------------
    # Learning switch (also remembers which port each IP is behind)
    # ------------------------------------------------------------------
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        in_port = msg.match['in_port']
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if eth is None:
            return

        src = eth.src
        dst = eth.dst
        dpid = datapath.id

        self.mac_to_port.setdefault(dpid, {})
        self.ip_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port

        # Remember where each IP lives (needed to build application flows).
        # Note: for a remote host on the other switch, this will correctly
        # learn the *inter-switch* port as "where that IP lives", which is
        # exactly what lets _install_worker_flows work unmodified across
        # two switches.
        ip4 = pkt.get_protocol(ipv4.ipv4)
        arp_pkt = pkt.get_protocol(arp.arp)
        if ip4:
            self.ip_to_port[dpid][ip4.src] = in_port
        if arp_pkt:
            self.ip_to_port[dpid][arp_pkt.src_ip] = in_port

        print("Packet:", src, "->", dst)

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
            self.add_flow(datapath, 1, match, [parser.OFPActionOutput(out_port)])
            out_actions = [parser.OFPActionOutput(out_port)]
        else:
            out_actions = self._flood_actions(datapath, in_port)

        out = parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=out_actions,
            data=msg.data
        )
        datapath.send_msg(out)

    def _flood_actions(self, datapath, in_port):
        """
        Plain OFPP_FLOOD would send a copy out of BOTH inter-switch links,
        which loops forever since s1 and s2 are joined twice. Instead we
        flood to every port except the one the packet came in on and
        (on s1/s2) the currently-inactive inter-switch port, so only one
        of the two redundant links is ever used at a time -- a minimal
        stand-in for spanning-tree / active-backup link blocking.
        """
        parser = datapath.ofproto_parser
        dpid = datapath.id
        ports = SWITCH_PORTS.get(dpid)
        if ports is None:
            return [parser.OFPActionOutput(datapath.ofproto.OFPP_FLOOD)]

        blocked = None
        if dpid in INTERSWITCH_DPIDS:
            blocked = BACKUP_PORT if self.active_inter_port == PRIMARY_PORT else PRIMARY_PORT

        return [parser.OFPActionOutput(p) for p in sorted(ports)
                if p != in_port and p != blocked]

    # ------------------------------------------------------------------
    # APPLICATION -> SDN : the coordinator reports job-queue events
    # ------------------------------------------------------------------
    def _ctl_server(self):
        if os.path.exists(CTL_SOCK):
            os.unlink(CTL_SOCK)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(CTL_SOCK)
        os.chmod(CTL_SOCK, 0o777)
        srv.listen(5)
        print("[APP-CHANNEL] listening on", CTL_SOCK)
        while True:
            conn, _ = srv.accept()
            self.app_conn = conn
            print("[APP-CHANNEL] coordinator connected")
            hub.spawn(self._ctl_reader, conn)

    def _ctl_reader(self, conn):
        buf = b""
        while True:
            data = conn.recv(1024)
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if line.strip():
                    self._on_app_message(line.decode().strip())
        if self.app_conn is conn:
            self.app_conn = None
        print("[APP-CHANNEL] coordinator disconnected")

    def _on_app_message(self, text):
        p = text.split("|")
        print("[APP]", text)

        if p[0] == "HELLO":
            self.coord_ip = p[1]

        elif p[0] == "WORKER_UP":
            wid, ip = p[1], p[2]
            old = self.workers.get(wid)
            cookie = old["cookie"] if old else self.next_cookie
            if not old:
                self.next_cookie += 1
            self.workers[wid] = {"ip": ip, "cookie": cookie, "jobs": 0,
                                 "last_bytes": 0, "flows": False}
            self._install_worker_flows(wid)

        elif p[0] == "WORKER_DOWN":
            wid = p[1]
            if wid in self.workers:
                self._remove_worker_flows(wid)
                print("[SDN] flows for worker", wid, "removed")
                del self.workers[wid]

        elif p[0] == "JOB":
            jid, wid = p[1], p[2]
            if wid in self.workers:
                self.workers[wid]["jobs"] += 1
                print("[SDN] %s sent to %s (%d jobs so far)" % (jid, wid, self.workers[wid]["jobs"]))

    def _to_app(self, text):
        if self.app_conn:
            try:
                self.app_conn.sendall((text + "\n").encode())
            except OSError:
                self.app_conn = None

    # Application-specific flows: only TCP port 6000 between coordinator and worker.
    # They have their own cookie so their packet/byte counters = that worker's load.
    # Works unchanged across two switches because ip_to_port[dpid] learns the
    # inter-switch port as "where that remote IP lives" for a cross-switch host.
    def _install_worker_flows(self, wid):
        w = self.workers[wid]
        for dpid, dp in self.datapaths.items():
            ports = self.ip_to_port.get(dpid, {})
            w_port = ports.get(w["ip"])
            c_port = ports.get(self.coord_ip)
            if w_port is None or c_port is None:
                print("[SDN] ports for", wid, "not learned yet, will retry")
                return
            parser = dp.ofproto_parser
            to_worker = parser.OFPMatch(eth_type=0x0800, ip_proto=6,
                                        ipv4_src=self.coord_ip, ipv4_dst=w["ip"],
                                        tcp_src=APP_PORT)
            to_coord = parser.OFPMatch(eth_type=0x0800, ip_proto=6,
                                       ipv4_src=w["ip"], ipv4_dst=self.coord_ip,
                                       tcp_dst=APP_PORT)
            self.add_flow(dp, 10, to_worker, [parser.OFPActionOutput(w_port)], w["cookie"])
            self.add_flow(dp, 10, to_coord, [parser.OFPActionOutput(c_port)], w["cookie"])
            w["flows"] = True
            print("[SDN] installed job-traffic flows for %s (%s) on port %d" % (wid, w["ip"], w_port))

    def _remove_worker_flows(self, wid):
        w = self.workers[wid]
        for dp in self.datapaths.values():
            ofproto = dp.ofproto
            parser = dp.ofproto_parser
            dp.send_msg(parser.OFPFlowMod(
                datapath=dp, command=ofproto.OFPFC_DELETE,
                out_port=ofproto.OFPP_ANY, out_group=ofproto.OFPG_ANY,
                cookie=w["cookie"], cookie_mask=0xffffffffffffffff,
                match=parser.OFPMatch()))
        w["flows"] = False

    # ------------------------------------------------------------------
    # SDN -> APPLICATION : worker load from flow statistics
    # ------------------------------------------------------------------
    def _stats_loop(self):
        while True:
            hub.sleep(STATS_EVERY)
            for wid, w in self.workers.items():
                if not w["flows"]:
                    self._install_worker_flows(wid)     # retry if ports were unknown
            for dp in self.datapaths.values():
                dp.send_msg(dp.ofproto_parser.OFPFlowStatsRequest(dp))

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_handler(self, ev):
        totals = {}
        for st in ev.msg.body:
            for wid, w in self.workers.items():
                if st.cookie == w["cookie"]:
                    totals[wid] = totals.get(wid, 0) + st.byte_count
        for wid, total in totals.items():
            w = self.workers[wid]
            rate = max(0, (total - w["last_bytes"]) // STATS_EVERY)
            w["last_bytes"] = total
            self._to_app("LOAD|%s|%d|%d" % (wid, rate, total))

    # ------------------------------------------------------------------
    # SDN -> APPLICATION : a link to a worker went down
    # SDN-internal         : the PRIMARY inter-switch link went down/up
    #                        -> reroute onto the backup link (D2)
    # ------------------------------------------------------------------
    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status_handler(self, ev):
        msg = ev.msg
        dpid = msg.datapath.id
        ofproto = msg.datapath.ofproto
        port = msg.desc.port_no
        down = bool(msg.desc.state & ofproto.OFPPS_LINK_DOWN) or \
            bool(msg.desc.config & ofproto.OFPPC_PORT_DOWN)

        # --- D2: inter-switch backbone link changed state -> reroute ---
        if dpid in INTERSWITCH_DPIDS and port in (PRIMARY_PORT, BACKUP_PORT):
            self._handle_backbone_port_status(dpid, port, down)
            return

        # --- original behaviour: a host (worker) link changed state ---
        ports = self.ip_to_port.get(dpid, {})
        for wid, w in list(self.workers.items()):
            if ports.get(w["ip"]) == port:
                if down:
                    print("[SDN] link to worker %s (port %d) is DOWN" % (wid, port))
                    self._remove_worker_flows(wid)
                    self._to_app("LINK_DOWN|%s" % wid)
                else:
                    print("[SDN] link to worker %s (port %d) is UP" % (wid, port))

    def _handle_backbone_port_status(self, dpid, port, down):
        if port == PRIMARY_PORT and down and self.active_inter_port == PRIMARY_PORT:
            print("[D2-REROUTE] primary inter-switch link (s1<->s2) DOWN on dpid %d "
                  "-> rerouting onto backup link (port %d)" % (dpid, BACKUP_PORT))
            self._switch_active_link(BACKUP_PORT)
        elif port == PRIMARY_PORT and not down and self.active_inter_port == BACKUP_PORT:
            print("[D2-REROUTE] primary inter-switch link (s1<->s2) back UP on dpid %d "
                  "-> failing back from backup link" % dpid)
            self._switch_active_link(PRIMARY_PORT)
        elif port == BACKUP_PORT and down and self.active_inter_port == BACKUP_PORT:
            # We were already running on the backup and it died too -- nothing
            # left to fail over to; just log it loudly.
            print("[D2-REROUTE] BOTH inter-switch links are down -- s1 and s2 are "
                  "partitioned, no path available")
        else:
            print("[D2-REROUTE] backbone port %d on dpid %d changed state "
                  "(down=%s) but it is not the active link; no action" % (port, dpid, down))

    def _switch_active_link(self, new_active_port):
        old_active_port = self.active_inter_port
        self.active_inter_port = new_active_port

        for dpid, dp in self.datapaths.items():
            if dpid not in INTERSWITCH_DPIDS:
                continue
            # 1) Flush every flow entry (learning-switch entries AND the
            #    application-specific job-traffic flows) that was still
            #    forwarding out the now-dead port on this switch.
            self._delete_flows_out_port(dp, old_active_port)
            # 2) Forget everything we learned via the old port so the next
            #    ARP/packet re-learns the correct path over the new link.
            self.mac_to_port.setdefault(dpid, {})
            self.ip_to_port.setdefault(dpid, {})
            for table in (self.mac_to_port[dpid], self.ip_to_port[dpid]):
                for key in [k for k, v in table.items() if v == old_active_port]:
                    del table[key]

        # 3) Re-install the per-worker job-traffic flows: they will only
        #    reappear once the relevant IPs are relearned (next packet from
        #    each host), but worker_flows["flows"] is left as-is so the
        #    stats loop's retry logic re-attempts installation automatically.
        for w in self.workers.values():
            w["flows"] = False
        print("[D2-REROUTE] active inter-switch link is now port %d; flows "
              "flushed, relearning in progress" % new_active_port)
