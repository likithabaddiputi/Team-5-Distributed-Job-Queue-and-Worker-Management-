import os
import socket
import sys
import time

COORDINATOR_HOST = os.environ.get('COORD_HOST', '10.0.0.2')
COORDINATOR_PORT = int(os.environ.get('COORD_PORT', '6000'))


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
    conn.sendall((text + "\n").encode())


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 simple_client.py <number>")
        return 1
    n_value = sys.argv[1]

    print("=" * 50)
    print("   JOB CLIENT")
    print("=" * 50)
    print()

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
            print("[CLIENT] connect attempt %d/15 failed: %s" % (attempt, e))
            s.close()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3)
            time.sleep(1)
    if last_err is not None:
        print("[CLIENT] giving up: cannot reach coordinator at %s:%s" % (COORDINATOR_HOST, COORDINATOR_PORT))
        return 1
    s.settimeout(None)
    read_line = make_reader(s)

    print("[CLIENT] Submitting job (n = " + n_value + ")")
    send_line(s, "SUBMIT|" + n_value)

    ack = read_line()
    if ack is None:
        print("[CLIENT] ERROR: coordinator closed the connection without answering")
        return 1
    parts = ack.split("|")
    if parts[0] != "ACK":
        print("[CLIENT] ERROR: coordinator rejected the job (" + (parts[1] if len(parts) > 1 else ack) + ")")
        s.close()
        return 2
    job_id = parts[1]
    print("[CLIENT] Coordinator accepted it -> job ID:", job_id)
    print()
    print("[CLIENT] Waiting for result...")
    print()

    result_msg = read_line()
    s.close()
    if result_msg is None:
        print("[CLIENT] ERROR: connection lost before the result arrived")
        return 1
    parts = result_msg.split("|")
    if parts[0] == "DONE" and len(parts) >= 3:
        print("[CLIENT] RESULT for", parts[1], "=", parts[2])
        print()
        return 0
    print("[CLIENT] ERROR: job failed (" + (parts[2] if len(parts) > 2 else result_msg) + ")")
    return 3


if __name__ == "__main__":
    sys.exit(main())