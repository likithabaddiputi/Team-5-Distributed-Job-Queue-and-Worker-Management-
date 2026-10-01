import socket
import threading
import time

HOST = '0.0.0.0'
PORT = 6000

# List of workers. Each worker is stored as a dictionary.
# Example: {"id": "w1", "conn": <socket>, "busy": False}
workers = []

# Jobs waiting for a free worker.
# Each job is a dictionary: {"job_id": "job1", "n": "7"}
job_queue = []

job_counter = 0
lock = threading.Lock()   # so two threads don't change the shared lists at the same time

job_by_id = {}      # job_id -> client socket (so we know who to reply to)
job_payload = {}    # job_id -> the number sent by the client
job_worker = {}     # job_id -> worker id currently running it


def show(tag, text):
    # Prints one event in a neat format, followed by a blank line.
    print(("[" + tag + "]").ljust(12), text)
    print()


def recv_line(conn):
    # recv() only gives raw bytes, and one message may arrive in pieces.
    # So keep reading until we see a newline, which marks the end of a message.
    data = b""
    while b"\n" not in data:
        part = conn.recv(1024)
        if not part:
            return None   # connection closed
        data += part
    line = data.split(b"\n")[0]
    return line.decode()


def send_line(conn, text):
    conn.send((text + "\n").encode())


def handle_worker(conn, addr, worker_id):
    worker = {"id": worker_id, "conn": conn, "busy": False}
    with lock:
        workers.append(worker)
        total = len(workers)
    show("WORKER", worker_id + " connected from " + str(addr) + "   (workers online: " + str(total) + ")")

    while True:
        msg = recv_line(conn)
        if msg is None:
            break

        parts = msg.split("|")

        if parts[0] == "HEARTBEAT":
            pass   # just means the worker is alive

        elif parts[0] == "RESULT":
            job_id = parts[1]
            result = parts[2]

            with lock:
                worker["busy"] = False
                client_conn = job_by_id.pop(job_id, None)
                job_worker.pop(job_id, None)   # job is finished, forget it

            show("RESULT", job_id + " completed by " + worker_id + "   (answer = " + result + ")")

            if client_conn:
                try:
                    send_line(client_conn, "DONE|" + job_id + "|" + result)
                    client_conn.close()
                except:
                    show("WARNING", "Client for " + job_id + " already left")

    # If we reach here, the worker disconnected or crashed.
    show("FAILURE", "Worker " + worker_id + " disconnected!")

    stuck_jobs = []
    with lock:
        if worker in workers:
            workers.remove(worker)
        for jid, wid in list(job_worker.items()):
            if wid == worker_id:
                stuck_jobs.append(jid)
        for jid in stuck_jobs:
            del job_worker[jid]
            job_queue.append({"job_id": jid, "n": job_payload[jid]})

    for jid in stuck_jobs:
        show("RECOVERY", jid + " was running on " + worker_id + " -> put back in the queue")


def scheduler():
    # Runs forever in the background, giving queued jobs to free workers.
    while True:
        time.sleep(0.5)

        with lock:
            if len(job_queue) == 0:
                continue

            free_worker = None
            for w in workers:
                if not w["busy"]:
                    free_worker = w
                    break

            if free_worker is None:
                continue

            job = job_queue.pop(0)
            free_worker["busy"] = True
            job_worker[job["job_id"]] = free_worker["id"]

        try:
            send_line(free_worker["conn"], "JOB|" + job["job_id"] + "|" + job["n"])
            show("SCHEDULER", job["job_id"] + " -> assigned to worker " + free_worker["id"])
        except:
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

        # The first message tells us whether this is a worker or a client.
        first_msg = recv_line(conn)
        if first_msg is None:
            conn.close()
            continue

        parts = first_msg.split("|")

        if parts[0] == "REGISTER":
            worker_id = parts[1]
            t = threading.Thread(target=handle_worker, args=(conn, addr, worker_id))
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
            # conn is NOT closed here. It stays open so we can send
            # the result back once a worker finishes the job.


if __name__ == "__main__":
    main()
