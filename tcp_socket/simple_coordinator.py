import os
import socket
import threading
import time

HOST = '0.0.0.0'
PORT = 6000
MY_IP = '10.0.0.2'                 # this host's address (sent to the SDN controller)
CTL_SOCK = '/tmp/sdn_ctl.sock'     # UNIX socket of the Ryu controller
HB_TIMEOUT = 10                    # seconds without HEARTBEAT -> worker is considered failed

# Each worker is a dictionary:
# {"id": "w1", "conn": <socket>, "ip": "10.0.0.3", "busy": False, "hb": time of last heartbeat}
workers = []

# Jobs waiting for a free worker: {"job_id": "job1", "n": "7"}
job_queue = []

job_counter = 0
lock = threading.Lock()

job_by_id = {}      # job_id -> client socket
job_payload = {}    # job_id -> the number sent by the client
job_worker = {}     # job_id -> worker id currently running it

worker_load = {}    # worker id -> bytes/second measured by the SDN controller

ctl = {"sock": None}
ctl_lock = threading.Lock()


def show(tag, text):
    print(("[" + tag + "]").ljust(12), text)
    print()


def make_reader(conn):
    # Returns a function that gives ONE message per call.
    # Bytes that arrive after the first newline are kept for the next call,
    # so two messages that arrive together are not lost.
    state = {"buf": b""}

    def read_line():
        while b"\n" not in state["buf"]:
            part = conn.recv(1024)
            if not part:
                return None
            state["buf"] += part
        line, state["buf"] = state["buf"].split(b"\n", 1)
        return line.decode()

    return read_line


def send_line(conn, text):
    conn.send((text + "\n").encode())


# ---------------------------------------------------------------------
# Channel to the SDN controller
# ---------------------------------------------------------------------
def notify(text):
    # Application -> SDN: tell the controller about a job-queue event.
    # If the controller is not running, the job queue still works.
    with ctl_lock:
        try:
            if ctl["sock"] is None:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.connect(CTL_SOCK)
                ctl["sock"] = s
                s.send(("HELLO|" + MY_IP + "\n").encode())
                threading.Thread(target=listen_controller, args=(s,), daemon=True).start()
            ctl["sock"].send((text + "\n").encode())
        except OSError:
            ctl["sock"] = None


def listen_controller(s):
    # SDN -> Application: load figures and link-failure events.
    read = make_reader(s)
    while True:
        msg = read()
        if msg is None:
            break
        parts = msg.split("|")
        if parts[0] == "LOAD":
            worker_load[parts[1]] = int(parts[2])
        elif parts[0] == "LINK_DOWN":
            show("SDN", "controller reports the link to " + parts[1] + " is DOWN")
            with lock:
                target = [w for w in workers if w["id"] == parts[1]]
            for w in target:
                try:
                    w["conn"].close()
                except OSError:
                    pass
                fail_worker(w, "unreachable (link down)")
    with ctl_lock:
        ctl["sock"] = None


# ---------------------------------------------------------------------
# Failure handling (used by disconnect, heartbeat timeout and link-down)
# ---------------------------------------------------------------------
def fail_worker(worker, why):
    stuck_jobs = []
    with lock:
        if worker not in workers:
            return                      # already handled
        workers.remove(worker)
        for jid, wid in list(job_worker.items()):
            if wid == worker["id"]:
                stuck_jobs.append(jid)
        for jid in stuck_jobs:
            del job_worker[jid]
            job_queue.append({"job_id": jid, "n": job_payload[jid]})

    show("FAILURE", "Worker " + worker["id"] + " " + why)
    for jid in stuck_jobs:
        show("RECOVERY", jid + " was running on " + worker["id"] + " -> put back in the queue")
    notify("WORKER_DOWN|" + worker["id"] + "|" + worker["ip"])


def handle_worker(conn, addr, worker_id, read_line):
    worker = {"id": worker_id, "conn": conn, "ip": addr[0], "busy": False, "hb": time.time()}
    with lock:
        workers.append(worker)
        total = len(workers)
    show("WORKER", worker_id + " connected from " + str(addr) + "   (workers online: " + str(total) + ")")
    notify("WORKER_UP|" + worker_id + "|" + addr[0])

    while True:
        try:
            msg = read_line()
        except OSError:
            break
        if msg is None:
            break

        parts = msg.split("|")

        if parts[0] == "HEARTBEAT":
            worker["hb"] = time.time()

        elif parts[0] == "RESULT":
            job_id = parts[1]
            result = parts[2]

            with lock:
                worker["busy"] = False
                client_conn = job_by_id.pop(job_id, None)
                job_worker.pop(job_id, None)

            show("RESULT", job_id + " completed by " + worker_id + "   (answer = " + result + ")")

            if client_conn:
                try:
                    send_line(client_conn, "DONE|" + job_id + "|" + result)
                    client_conn.close()
                except OSError:
                    show("WARNING", "Client for " + job_id + " already left")

    fail_worker(worker, "disconnected!")


def scheduler():
    # Runs forever in the background, giving queued jobs to free workers.
    while True:
        time.sleep(0.5)

        # 1) workers that stopped sending heartbeats are treated as failed
        now = time.time()
        with lock:
            stale = [w for w in workers if now - w["hb"] > HB_TIMEOUT]
        for w in stale:
            try:
                w["conn"].close()
            except OSError:
                pass
            fail_worker(w, "silent for " + str(HB_TIMEOUT) + "s (heartbeat timeout)")

        # 2) give the next job to the LEAST LOADED free worker (load comes from the SDN controller)
        with lock:
            if len(job_queue) == 0:
                continue

            free = [w for w in workers if not w["busy"]]
            if not free:
                continue

            free_worker = min(free, key=lambda w: worker_load.get(w["id"], 0))
            job = job_queue.pop(0)
            free_worker["busy"] = True
            job_worker[job["job_id"]] = free_worker["id"]

        try:
            send_line(free_worker["conn"], "JOB|" + job["job_id"] + "|" + job["n"])
            load = worker_load.get(free_worker["id"], 0)
            show("SCHEDULER", job["job_id"] + " -> assigned to worker " + free_worker["id"]
                 + "   (SDN-measured load: " + str(load) + " B/s)")
            notify("JOB|" + job["job_id"] + "|" + free_worker["id"])
        except OSError:
            show("WARNING", "Could not reach " + free_worker["id"] + " (its failure will be handled)")


def main():
    global job_counter

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(20)

    print("=" * 50)
    print("   JOB QUEUE COORDINATOR")
    print("   Listening on port", PORT)
    print("=" * 50)
    print()

    threading.Thread(target=scheduler, daemon=True).start()

    while True:
        conn, addr = server.accept()
        read_line = make_reader(conn)

        # The first message tells us whether this is a worker or a client.
        first_msg = read_line()
        if first_msg is None:
            conn.close()
            continue

        parts = first_msg.split("|")

        if parts[0] == "REGISTER":
            t = threading.Thread(target=handle_worker, args=(conn, addr, parts[1], read_line))
            t.start()

        elif parts[0] == "SUBMIT":
            n_value = parts[1]
            with lock:
                job_counter += 1
                job_id = "job" + str(job_counter)
                job_queue.append({"job_id": job_id, "n": n_value})
                job_by_id[job_id] = conn
                job_payload[job_id] = n_value

            send_line(conn, "ACK|" + job_id)
            show("CLIENT", "New job " + job_id + " received from " + str(addr) + "   (n = " + n_value + ")")


if __name__ == "__main__":
    main()
