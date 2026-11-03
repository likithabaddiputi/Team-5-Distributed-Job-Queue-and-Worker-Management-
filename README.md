# Distributed Job Queue and Worker Management over SDN
**Project 3 — SDN + Socket Programming (Jackfruit Mini Project)** · TCP sockets · Mininet · Ryu (OpenFlow 1.3)
**D2 update:** redundant two-switch backbone with automatic SDN rerouting, plus a performance evaluation harness. See §4b and §8.

## 1. Problem and objectives
Clients submit jobs over TCP; a coordinator queues them and hands them to worker nodes.
The SDN controller **watches the coordinator↔worker traffic** and **reacts to failures**:

| Project requirement | How it is met |
|---|---|
| Baseline-compared performance evaluation | `tests/test_performance.py` — throughput, Jain fairness, failover timing, and a side-by-side SDN-aware vs static baseline under congestion (§6) |
| Concurrent TCP clients | one thread per connection in the coordinator |
| Job submission + acknowledgment | `SUBMIT` → `ACK\|jobId`, later `DONE` |
| Register/monitor workers | `REGISTER`, 3 s `HEARTBEAT`, 10 s timeout |
| Job queue + scheduling | FIFO queue; job goes to the **least-loaded free worker** |
| Monitor worker/network load | Ryu polls OpenFlow flow stats every 2 s, sends `LOAD` to the coordinator |
| Dynamically control SDN paths | Ryu installs/removes per-worker flows (own cookie) when a worker joins/leaves |
| Redistribute jobs on failure | worker disconnect, heartbeat timeout **or** SDN `LINK_DOWN` → its jobs are re-queued |
| **(D2)** Path rerouting | two switches joined by a **redundant link pair**; Ryu reroutes onto the backup link when the primary backbone link fails, with the app layer never noticing |
| **(D2)** Performance evaluation | `tests/test_performance.py` — throughput, scheduler balance, failover recovery time |

## 2. Architecture
```
                 Ryu controller (controller.py)  127.0.0.1:6633
                   |  OpenFlow 1.3        \  UNIX socket /tmp/sdn_ctl.sock
                   |                       \  (app <-> SDN channel)
        h1 Client ---+                        \
        h2 Coordinator (simple_coordinator.py :6000)
                      +-- s1 --===primary===-- s2 --+-- h3 Worker w1
                      |        ===backup====         |-- h4 Worker w2
```
| Node | Role | IP | Switch |
|---|---|---|---|
| h1 | Client (`simple_client.py`) | 10.0.0.1 | s1 |
| h2 | Coordinator (`simple_coordinator.py`, TCP 6000) | 10.0.0.2 | s1 |
| h3 / h4 | Workers `w1` / `w2` (`simple_worker.py`) | 10.0.0.3 / 10.0.0.4 | s2 |
| s1, s2 | Open vSwitch, OpenFlow 1.3, joined by **two** links (primary + backup) | — | — |
| c0 | Ryu remote controller | 127.0.0.1:6633 | — |

Port numbering is fixed by the order links are added in `topology.py`/`backend.py` and relied on by `controller.py`:
`port 1` = first host, `port 2` = second host, `port 3` = primary inter-switch link, `port 4` = backup inter-switch link (same on both s1 and s2).

### Files
| File | Purpose |
|---|---|
| `topology.py` | Mininet topology — **D2: two switches (s1, s2) + redundant backbone link** |
| `controller.py` | Ryu app: learning switch (loop-free flood across the redundant links) + app-specific flows + stats + port-status + app channel + **D2: backbone failover/failback logic** |
| `tcp_socket/` | coordinator, worker, client (plain `socket` module) — **unchanged from D1** |
| `backend.py`, `jobqueue-sdn-ui.html` | GUI that runs the **real** stack and visualises it — **D2: two-switch layout + "Toggle backbone link" button** |
| `tests/test_sockets.py` | automated socket tests (no Mininet needed) — unchanged |
| `tests/test_mininet.py` | automated end-to-end test in Mininet + Ryu — **D2: added backbone-failure/reroute checks** |
| `tests/test_performance.py` | **D2:** rigorous suite — throughput/latency, Jain fairness, SDN-aware vs static baseline, failover timing, repeated-failure robustness; writes JSON + Markdown report |

## 3. Message protocol (newline-terminated text over TCP :6000)
| Message | Direction | Meaning |
|---|---|---|
| `SUBMIT\|n` | client → coord | run job with integer `n` (`\|n\| ≤ 10^9`) |
| `ACK\|jobId` | coord → client | job accepted |
| `DONE\|jobId\|result` | coord → client | result = n² |
| `ERR\|reason` / `ERR\|jobId\|reason` | coord → client | rejected (`bad_number`, `unknown_message`) / failed (`bad_input`, `too_many_worker_failures`) |
| `REGISTER\|workerId` | worker → coord | join the pool |
| `HEARTBEAT` | worker → coord | liveness, every 3 s |
| `JOB\|jobId\|n` | coord → worker | work assignment |
| `RESULT\|jobId\|r` / `ERROR\|jobId\|reason` | worker → coord | job finished / job could not be run |

**App ↔ SDN channel** (UNIX socket, same line format):
| Message | Direction | Effect |
|---|---|---|
| `HELLO\|coordIP` | app → SDN | controller learns where the coordinator is |
| `WORKER_UP\|id\|ip` / `WORKER_DOWN\|id\|ip` | app → SDN | install / remove that worker's flows |
| `JOB\|jobId\|workerId` | app → SDN | per-worker job counter |
| `LOAD\|id\|bytesPerSec\|totalBytes` | SDN → app | measured from OpenFlow flow stats; used by the scheduler |
| `LINK_DOWN\|id` | SDN → app | from OFPT_PORT_STATUS; coordinator fails the worker and re-queues its jobs |

## 4. SDN behaviour
* **Table-miss** → controller; **learning switch** at priority 1 (also records IP → port). Flooding is restricted to exclude whichever inter-switch link is currently inactive, so the two parallel s1↔s2 links never form a loop (a minimal stand-in for spanning-tree / active-backup blocking).
* **Application flows** (priority 10, TCP `:6000`, coordinator↔worker, both directions) are installed when `WORKER_UP` arrives. Each worker has its **own cookie**, so its flow byte-counters equal that worker's load. They are deleted by cookie on `WORKER_DOWN` / link down. These flows work unmodified across the two switches: `ip_to_port[dpid]` naturally learns the inter-switch port as "where that remote IP lives" for a cross-switch host.
* **Stats loop** every 2 s: `OFPFlowStatsRequest` → bytes/s per worker → `LOAD`.
  Note: the measured bytes are all coordinator↔worker TCP traffic (jobs, results **and heartbeats**), i.e. a proxy for worker activity.
* **Worker-failure path (unchanged from D1):** `ip link down` on a worker's own port → OVS `PORT_STATUS` → Ryu → `LINK_DOWN|wid` → coordinator closes that worker and re-queues its jobs.

## 4b. D2 — path rerouting over the redundant backbone
* Topology now has **two switches**, `s1` (client + coordinator) and `s2` (both workers), joined by **two parallel links**: a primary (port 3 on each side) and a backup (port 4).
* All job traffic between the coordinator and a worker therefore crosses the backbone — this is what makes rerouting observable, not just theoretical.
* `controller.py` tracks `self.active_inter_port` (starts as the primary). On `EventOFPPortStatus` for the **primary backbone port** going down, it:
  1. Flips the active link to the backup port.
  2. Flushes every flow entry on `s1`/`s2` that was still forwarding out the dead port (both the learning-switch entries and the per-worker application flows).
  3. Forgets everything that was learned via the dead port (`mac_to_port`, `ip_to_port`), so the next packet re-learns the path over the backup link.
  4. Marks every worker's application flows as needing reinstall; the existing stats-loop retry logic reinstalls them automatically once ports are relearned.
* The same logic fails back to the primary link automatically once Ryu sees it come back up (`PORT_STATUS` with `down=False`).
* **Crucially, none of this touches the app ↔ SDN channel.** The coordinator is never told `WORKER_DOWN` or `LINK_DOWN` for a backbone failure — jobs keep completing exactly as before, just over a different physical path. The worker-link failure path (§4) is completely separate and still goes through the app channel as before.
* **Gotcha to know for the viva:** Mininet's `net.configLinkStatus('s1','s2', ...)` (and the CLI's `link s1 s2 down`) affects *every* link between two nodes at once — with two parallel links that kills both simultaneously and leaves nothing to fail over to. `topology.py`, `backend.py` and `tests/test_mininet.py` all therefore reference the **primary link object directly** (`net.primary_link.intf1/intf2.ifconfig(...)`) instead.

## 5. How to run
**Prerequisites:** Ubuntu VM, Mininet, Open vSwitch, Ryu (`~/ryu-env`), Python 3, `tcpdump` (GUI only).

### Option A — manual (3 terminals)
```bash
# Terminal 1: controller
source ~/ryu-env/bin/activate && cd ~/sdn-project && ryu-manager controller.py
# Terminal 2: network
cd ~/sdn-project && sudo python3 topology.py
# Terminal 3 (inside the mininet> prompt)
mininet> xterm h2 h3 h4 h1
h2# python3 tcp_socket/simple_coordinator.py
h3# python3 tcp_socket/simple_worker.py w1
h4# python3 tcp_socket/simple_worker.py w2
h1# python3 tcp_socket/simple_client.py 7        # prints  RESULT for job1 = 49
```
Inspect the SDN state: `sudo ovs-ofctl -O OpenFlow13 dump-flows s1` (look for `cookie=0x1`, `tp_src=6000`).

**D2 — demo backbone rerouting manually:**
```
mininet> py net.primary_link.intf1.ifconfig("down"); net.primary_link.intf2.ifconfig("down")
# submit a job from h1 — it still completes; check Ryu's terminal for [D2-REROUTE] lines
# ovs-ofctl -O OpenFlow13 dump-flows s1   -> flows now show output:4 (backup), not output:3
mininet> py net.primary_link.intf1.ifconfig("up"); net.primary_link.intf2.ifconfig("up")
# Ryu fails back automatically; flows return to output:3
```
Do **not** use `mininet> link s1 s2 down` for this — see the gotcha in §4b.

### Option B — GUI (runs the same real stack)
```bash
cd ~/sdn-project && sudo python3 backend.py --ryu "$HOME/ryu-env/bin/ryu-manager controller.py"
# open http://localhost:8000 — header must say "LIVE: real Mininet + Ryu + tcp_socket"
# (if it says "simulation", the backend is not running)
```
Wait for **network ready** (~6 s), then use *Send job*, *Kill*, *Freeze*, **Link down** on a worker, and the new **Backbone link** button to demo D2 rerouting live.

## 6. Tests
```bash
python3 tests/test_sockets.py                     # 8 socket tests, ~25 s, no root, no Mininet
sudo python3 tests/test_mininet.py                 # end-to-end (Ryu must already be running)
python3 tests/test_performance.py                  # D2: rigorous performance suite (no root needed)
```

| # | Scenario | Expected | Test |
|---|---|---|---|
| T1 | 5 concurrent clients, 2 workers | all 5 get n² | `test_five_concurrent_clients…` |
| T2 | App events reach SDN | `HELLO`, `WORKER_UP`, `JOB` seen by controller | `test_app_events_reach…` |
| T3 | SDN load influences scheduling | job goes to the worker with the lower `LOAD` | `test_scheduler_uses_load…` |
| T4 | Bad input (`abc`, `1.5`, empty, huge) | `ERR`, workers stay alive, system still works | `test_non_numeric_job…` |
| T5 | Garbage protocol messages | `ERR`, connection closed | `test_malformed_protocol…` |
| T6 | Worker killed mid-job | job re-queued, client still gets the answer | `test_killed_worker…` |
| T7 | SDN `LINK_DOWN` | worker failed, job re-run elsewhere | `test_sdn_link_down…` |
| T8 | Worker keeps dying | job failed after 3 attempts (`too_many_worker_failures`) | `test_worker_that_always_dies…` |
| M1–M8 | Mininet: pingAll (2 switches), flows installed, job result, non-zero `LOAD`, worker-link-down → failover, bad input | see `tests/test_mininet.py` | end-to-end |
| **M9–M12 (D2)** | **Primary backbone down → job still completes; no `WORKER_DOWN`/`LINK_DOWN` sent to app; flows move to backup port; backbone restored → job still completes** | see `tests/test_mininet.py` | end-to-end |

### Performance evaluation (`tests/test_performance.py`)

The suite runs the **real** TCP coordinator/worker/client processes (no simulation) and produces both a JSON dump and a Markdown report:

| Experiment | What is measured | Why it matters for the rubric |
|---|---|---|
| **P1 Throughput & latency** | jobs/s + p50/p95 latency at 1 / 3 / 6 / 10 concurrent clients (3 repeats, mean ± stdev) | Shows the system scales under load |
| **P2 Fairness** | Jain’s fairness index of job distribution across workers | Quantifies load-balancing quality |
| **P3 Baseline comparison** | Same congested-w1 scenario run **with** and **without** SDN `LOAD` feedback | Directly compares the proposed approach against a static baseline |
| **P4 Failover timing** | detect→re-queue, detect→re-assign, kill→client-result (4 repeats) | Quantifies recovery speed under worker failure |
| **P5 Repeated-failure robustness** | Job is correctly aborted after `MAX_ATTEMPTS` consecutive worker deaths | Proves the system does not re-queue forever |

Artefacts written on every run:
- `/tmp/sdn_perf_report.json` — raw numbers
- `/tmp/sdn_perf_report.md` — ready-to-paste summary table for the report / viva

Typical observed result for P3: under heavy synthetic load on `w1`, the static baseline still sends a large fraction of jobs to the congested worker; the SDN-aware scheduler steers most jobs away from it.

Manual SDN-failure demo (Option A): `mininet> link s2 h3 down` → coordinator prints `controller reports the link to w1 is DOWN`, then `FAILURE` / `RECOVERY`. For the **backbone** failure demo instead, see §4b / §5.

## 7. Limitations / remaining work
* Backbone failover currently assumes a fixed, known port layout (`PRIMARY_PORT`/`BACKUP_PORT` constants in `controller.py`) rather than general topology discovery (`ryu.topology.api` / LLDP) — fine for this fixed two-switch lab topology, but wouldn't generalize to an arbitrary mesh without extension.
* Jobs are `n → n²` with a simulated delay; the coordinator itself is still a single point of failure (not addressed by this project).
* Flood-based loop avoidance on the redundant links is a minimal active/backup block, not a full spanning-tree protocol.
