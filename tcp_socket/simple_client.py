import socket
import sys

COORDINATOR_HOST = '127.0.0.1'
COORDINATOR_PORT = 6000


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


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 simple_client.py <number>")
        return
    n_value = sys.argv[1]

    print("=" * 50)
    print("   JOB CLIENT")
    print("=" * 50)
    print()

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((COORDINATOR_HOST, COORDINATOR_PORT))

    print("[CLIENT] Submitting job (n = " + n_value + ")")
    send_line(s, "SUBMIT|" + n_value)

    ack = recv_line(s)
    job_id = ack.split("|")[1]
    print("[CLIENT] Coordinator accepted it -> job ID:", job_id)
    print()
    print("[CLIENT] Waiting for result...")
    print()

    result_msg = recv_line(s)
    parts = result_msg.split("|")
    print("[CLIENT] RESULT for", parts[1], "=", parts[2])
    print()

    s.close()


if __name__ == "__main__":
    main()
