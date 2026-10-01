import socket
import threading
import time
import sys
import random

COORDINATOR_HOST = '127.0.0.1'
COORDINATOR_PORT = 6000

JOB_TIME_MIN = 2
JOB_TIME_MAX = 8


def recv_line(conn):
    data = b""
    while b"\n" not in data:
        part = conn.recv(1024)
        if not part:
            return None
        data += part
    line = data.split(b"\n")[0]
    return line.decode()


def send_line(conn, text):
    conn.send((text + "\n").encode())


def do_the_job(n_value):
    # Pretend to do some work. Replace this with real processing later.
    time.sleep(random.randint(JOB_TIME_MIN, JOB_TIME_MAX))
    n = int(n_value)
    return n * n


def send_heartbeats(conn):
    while True:
        time.sleep(3)
        try:
            send_line(conn, "HEARTBEAT")
        except:
            return


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 simple_worker.py <worker_id>")
        return
    worker_id = sys.argv[1]

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((COORDINATOR_HOST, COORDINATOR_PORT))

    send_line(s, "REGISTER|" + worker_id)

    print("=" * 50)
    print("   WORKER", worker_id)
    print("   Connected to coordinator at", COORDINATOR_HOST + ":" + str(COORDINATOR_PORT))
    print("=" * 50)
    print()
    print("Waiting for jobs...")
    print()

    # send heartbeats in the background
    t = threading.Thread(target=send_heartbeats, args=(s,), daemon=True)
    t.start()

    while True:
        msg = recv_line(s)
        if msg is None:
            print("Lost connection to coordinator. Exiting.")
            print()
            break

        parts = msg.split("|")
        if parts[0] == "JOB":
            job_id = parts[1]
            n_value = parts[2]

            print("[" + worker_id + "] Received", job_id, "(n = " + n_value + ")")
            print("[" + worker_id + "] Processing...")

            answer = do_the_job(n_value)

            send_line(s, "RESULT|" + job_id + "|" + str(answer))
            print("[" + worker_id + "] Finished", job_id, "-> answer =", answer)
            print()


if __name__ == "__main__":
    main()
