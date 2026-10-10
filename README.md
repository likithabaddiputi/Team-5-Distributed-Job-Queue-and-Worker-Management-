# Distributed Job Queue and Worker Management over SDN
**Project 3 — SDN + Socket Programming (Jackfruit Mini Project)** · TCP sockets · Mininet · Ryu (OpenFlow 1.3)

## 1. Problem and objectives
Clients submit jobs over TCP; a coordinator queues them and hands them to worker nodes.
The SDN controller **watches the coordinator↔worker traffic** and **reacts to failures**:

| Project requirement | How it is met |
|---|---|
| Concurrent TCP clients | one thread per connection in the coordinator |
| Job submission + acknowledgment | `SUBMIT` → `ACK\|jobId`, later `DONE` |
| Register/monitor workers | `REGISTER`, 3 s `HEARTBEAT`, 10 s timeout |
| Job queue + scheduling | FIFO queue; job goes to the **least-loaded free worker** |
| Monitor worker/network load | Ryu polls OpenFlow flow stats every 2 s, sends `LOAD` to the coordinator |
| Dynamically control SDN paths | Ryu installs/removes per-worker flows (own cookie) when a worker joins/leaves |
| Redistribute jobs on failure | worker disconnect, heartbeat timeout **or** SDN `LINK_DOWN` → its jobs are re-queued |

## 2. Architecture
```
                 Ryu controller (controller.py)  127.0.0.1:6633
                   |  OpenFlow 1.3        \  UNIX socket /tmp/sdn_ctl.sock
                   |                       \  (app <-> SDN channel)
        h1 Client --+                        \
        h2 Coordinator (simple_coordinator.py :6000)
        h3 Worker w1   +-- s1 (Open vSwitch) --+
        h4 Worker w2
```
| Node | Role | IP |
|---|---|---|
| h1 | Client (`simple_client.py`) | 10.0.0.1 |
| h2 | Coordinator (`simple_coordinator.py`, TCP 6000) | 10.0.0.2 |
| h3 / h4 | Workers `w1` / `w2` (`simple_worker.py`) | 10.0.0.3 / 10.0.0.4 |
| s1 | Open vSwitch, OpenFlow 1.3 | — |
| c0 | Ryu remote controller | 127.0.0.1:6633 |

### Files
| File | Purpose |
|---|---|
| `topology.py` | Mininet topology (1 switch, 4 hosts, remote controller) |
| `controller.py` | Ryu app: learning switch + app-specific flows + stats + port-status + app channel |
| `tcp_socket/` | coordinator, worker, client (plain `socket` module) |
| `backend.py`, `jobqueue-sdn-ui.html` | optional GUI that runs the **real** stack and visualises it |
| `tests/test_sockets.py` | automated socket tests (no Mininet needed) |
| `tests/test_mininet.py` | automated end-to-end test in Mininet + Ryu |

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
* **Table-miss** → controller; **learning switch** at priority 1 (also records IP → port).
* **Application flows** (priority 10, TCP `:6000`, coordinator↔worker, both directions) are installed when `WORKER_UP` arrives. Each worker has its **own cookie**, so its flow byte-counters equal that worker's load. They are deleted by cookie on `WORKER_DOWN` / link down.
* **Stats loop** every 2 s: `OFPFlowStatsRequest` → bytes/s per worker → `LOAD`.
  Note: the measured bytes are all coordinator↔worker TCP traffic (jobs, results **and heartbeats**), i.e. a proxy for worker activity.
* **Failure path:** `ip link down` on a worker port → OVS `PORT_STATUS` → Ryu → `LINK_DOWN|wid` → coordinator closes that worker and re-queues its jobs.
* Current topology has **one switch**, so there is no alternate path; path *rerouting* is the planned D2 extension (second switch + redundant link, reroute flows on `LINK_DOWN`).

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

### Option B — GUI (runs the same real stack)
```bash
cd ~/sdn-project && sudo python3 backend.py --ryu "$HOME/ryu-env/bin/ryu-manager controller.py"
# open http://localhost:8000 — header must say "LIVE: real Mininet + Ryu + tcp_socket"
# (if it says "simulation", the backend is not running)
```
Wait for **network ready** (~6 s), then use *Send job*, *Kill*, *Freeze* and **Link down** on a worker.

## 6. Tests
```bash
python3 tests/test_sockets.py                     # 8 socket tests, ~25 s, no root, no Mininet
sudo python3 tests/test_mininet.py                # end-to-end (Ryu must already be running)
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
| M1–M8 | Mininet: pingAll, flows installed, job result, non-zero `LOAD`, `link s1 h3 down` → failover, bad input | see `tests/test_mininet.py` | end-to-end |

Manual SDN-failure demo (Option A): `mininet> link s1 h3 down` → coordinator prints `controller reports the link to w1 is DOWN`, then `FAILURE` / `RECOVERY`.

## 7. Limitations / planned work (D2)
* One switch → no alternate path yet (add `s2` + redundant link and reroute on `LINK_DOWN`).
* Jobs are `n → n²` with a simulated 2–8 s delay; the coordinator is a single point of failure.
* Performance evaluation (throughput, recovery time, static vs. load-aware scheduling) is D2.
