import os
import socket
import threading
import time

HOST = '0.0.0.0'
PORT = int(os.environ.get('COORD_PORT', '6000'))
MY_IP = os.environ.get('COORD_IP', '10.0.0.2')                # this host's address (sent to the SDN controller)
CTL_SOCK = os.environ.get('SDN_CTL_SOCK', '/tmp/sdn_ctl.sock')  # UNIX socket of the Ryu controller
HB_TIMEOUT = int(os.environ.get('HB_TIMEOUT', '10'))          # seconds without HEARTBEAT -> worker failed
MAX_ATTEMPTS = 3          # a job that loses its worker this many times is failed instead of re-queued forever
MAX_ABS_N = 10 ** 9       # accepted range for the job number n

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
job_attempts = {}   # job_id -> how many times a worker was lost while running it

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
        return line.decode(errors="replace").strip()

    return read_line


def send_line(conn, text):
    conn.sendall((text + "\n").encode())


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
                s.sendall(("HELLO|" + MY_IP + "\n").encode())
                threading.Thread(target=listen_controller, args=(s,), daemon=True).start()
            ctl["sock"].sendall((text + "\n").encode())
        except OSError:
            ctl["sock"] = None


def listen_controller(s):
    # SDN -> Application: load figures and link-failure events.
    read = make_reader(s)
    while True:
        try:
            msg = read()
        except OSError:
            break
        if msg is None:
            break
        parts = msg.split("|")
        if parts[0] == "LOAD" and len(parts) >= 3:
            try:
                worker_load[parts[1]] = int(parts[2])
            except ValueError:
                pass
        elif parts[0] == "LINK_DOWN" and len(parts) >= 2:
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
    requeued, dropped = [], []
    with lock:
        if worker not in workers:
            return                      # already handled
        workers.remove(worker)
        stuck = [jid for jid, wid in job_worker.items() if wid == worker["id"]]
        for jid in stuck:
            del job_worker[jid]
            job_attempts[jid] = job_attempts.get(jid, 0) + 1
            if job_attempts[jid] >= MAX_ATTEMPTS:
                dropped.append((jid, job_by_id.pop(jid, None)))
                job_payload.pop(jid, None)
            else:
                job_queue.append({"job_id": jid, "n": job_payload[jid]})
                requeued.append(jid)

    show("FAILURE", "Worker " + worker["id"] + " " + why)
    for jid in requeued:
        show("RECOVERY", jid + " was running on " + worker["id"] + " -> put back in the queue")
    for jid, client_conn in dropped:
        show("GIVE-UP", jid + " lost its worker " + str(MAX_ATTEMPTS) + " times -> failed")
        if client_conn:
            try:
                send_line(client_conn, "ERR|" + jid + "|too_many_worker_failures")
                client_conn.close()
            except OSError:
                pass
    notify("WORKER_DOWN|" + worker["id"] + "|" + worker["ip"])


def finish_job(worker, job_id, reply):
    # A worker reported RESULT or ERROR for job_id. Ignore reports for jobs it does not own.
    with lock:
        if job_worker.get(job_id) != worker["id"]:
            return False
        worker["busy"] = False
        client_conn = job_by_id.pop(job_id, None)
        job_worker.pop(job_id, None)
        job_payload.pop(job_id, None)
        job_attempts.pop(job_id, None)
    if client_conn:
        try:
            send_line(client_conn, reply)
            client_conn.close()
        except OSError:
            show("WARNING", "Client for " + job_id + " already left")
    return True


def handle_worker(conn, addr, worker_id, read_line):
    worker = {"id": worker_id, "conn": conn, "ip": addr[0], "busy": False, "hb": time.time()}

    # A worker re-registering with an id that is still listed replaces the old entry.
    with lock:
        old = [w for w in workers if w["id"] == worker_id]
    for w in old:
        try:
            w["conn"].close()
        except OSError:
            pass
        fail_worker(w, "replaced by a new registration")

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

        elif parts[0] == "RESULT" and len(parts) >= 3:
            job_id, result = parts[1], parts[2]
            if finish_job(worker, job_id, "DONE|" + job_id + "|" + result):
                show("RESULT", job_id + " completed by " + worker_id + "   (answer = " + result + ")")
            else:
                show("WARNING", "ignored stale RESULT for " + job_id + " from " + worker_id)

        elif parts[0] == "ERROR" and len(parts) >= 3:
            job_id, reason = parts[1], parts[2]
            if finish_job(worker, job_id, "ERR|" + job_id + "|" + reason):
                show("JOB-ERROR", job_id + " failed on " + worker_id + "   (" + reason + ")")

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


def handle_client(conn, addr, parts):
    global job_counter
    n_value = parts[1].strip() if len(parts) > 1 else ""
    try:
        n = int(n_value)
        if abs(n) > MAX_ABS_N:
            raise ValueError("out of range")
    except ValueError:
        # Reject bad input here so it can never reach (and crash) a worker.
        show("REJECTED", "bad SUBMIT from " + str(addr) + "   (n = " + repr(n_value[:30]) + ")")
        try:
            send_line(conn, "ERR|bad_number")
        except OSError:
            pass
        conn.close()
        return

    with lock:
        job_counter += 1
        job_id = "job" + str(job_counter)
        job_queue.append({"job_id": job_id, "n": str(n)})
        job_by_id[job_id] = conn
        job_payload[job_id] = str(n)

    try:
        send_line(conn, "ACK|" + job_id)
    except OSError:
        pass
    show("CLIENT", "New job " + job_id + " received from " + str(addr) + "   (n = " + str(n) + ")")


def handle_connection(conn, addr):
    read_line = make_reader(conn)

    # The first message tells us whether this is a worker or a client.
    try:
        first_msg = read_line()
    except OSError:
        first_msg = None
    if first_msg is None:
        conn.close()
        return

    parts = first_msg.split("|")

    if parts[0] == "REGISTER" and len(parts) >= 2 and parts[1]:
        handle_worker(conn, addr, parts[1], read_line)
    elif parts[0] == "SUBMIT":
        handle_client(conn, addr, parts)
    else:
        show("REJECTED", "unknown first message from " + str(addr) + ": " + repr(first_msg[:30]))
        try:
            send_line(conn, "ERR|unknown_message")
        except OSError:
            pass
        conn.close()


def main():
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
        # one thread per connection so simultaneous clients never wait for each other
        threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()
