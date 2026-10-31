import os
import random
import socket
import sys
import threading
import time

COORDINATOR_HOST = os.environ.get('COORD_HOST', '10.0.0.2')
COORDINATOR_PORT = int(os.environ.get('COORD_PORT', '6000'))

JOB_TIME_MIN = int(os.environ.get('JOB_TIME_MIN', '2'))
JOB_TIME_MAX = int(os.environ.get('JOB_TIME_MAX', '8'))

send_lock = threading.Lock()     # heartbeat thread and main thread share one socket


def make_reader(conn):
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
    with send_lock:
        conn.sendall((text + "\n").encode())


def do_the_job(n_value):
    # Pretend to do some work. Replace this with real processing later.
    n = int(n_value)                      # may raise ValueError -> reported as ERROR, never crashes the worker
    time.sleep(random.randint(min(JOB_TIME_MIN, JOB_TIME_MAX), max(JOB_TIME_MIN, JOB_TIME_MAX)))
    return n * n


def send_heartbeats(conn):
    while True:
        time.sleep(3)
        try:
            send_line(conn, "HEARTBEAT")
        except OSError:
            return


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 simple_worker.py <worker_id>")
        return
    worker_id = sys.argv[1]

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(3)
    last_err = None
    for attempt in range(1, 16):
        try:
            s.connect((COORDINATOR_HOST, COORDINATOR_PORT))
            last_err = None
            break
        except OSError as e:
            last_err = e
            print("connect attempt %d/15 failed: %s" % (attempt, e))
            s.close()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3)
            time.sleep(1)
    if last_err is not None:
        print("giving up: cannot reach coordinator at %s:%s" % (COORDINATOR_HOST, COORDINATOR_PORT))
        raise last_err
    s.settimeout(None)

    read_line = make_reader(s)
    send_line(s, "REGISTER|" + worker_id)

    print("=" * 50)
    print("   WORKER", worker_id)
    print("   Connected to coordinator at", COORDINATOR_HOST + ":" + str(COORDINATOR_PORT))
    print("=" * 50)
    print()
    print("Waiting for jobs...")
    print()

    threading.Thread(target=send_heartbeats, args=(s,), daemon=True).start()

    while True:
        try:
            msg = read_line()
        except OSError:
            msg = None
        if msg is None:
            print("Lost connection to coordinator. Exiting.")
            print()
            break

        parts = msg.split("|")
        if parts[0] == "JOB" and len(parts) >= 3:
            job_id = parts[1]
            n_value = parts[2]

            print("[" + worker_id + "] Received", job_id, "(n = " + n_value + ")")
            print("[" + worker_id + "] Processing...")

            try:
                answer = do_the_job(n_value)
            except ValueError:
                send_line(s, "ERROR|" + job_id + "|bad_input")
                print("[" + worker_id + "] FAILED", job_id, "-> bad input, reported to coordinator")
                print()
                continue

            send_line(s, "RESULT|" + job_id + "|" + str(answer))
            print("[" + worker_id + "] Finished", job_id, "-> answer =", answer)
            print()
        elif parts[0] == "ERR":
            print("Coordinator refused us (" + msg + "). Exiting.")
            break


if __name__ == "__main__":
    main()
