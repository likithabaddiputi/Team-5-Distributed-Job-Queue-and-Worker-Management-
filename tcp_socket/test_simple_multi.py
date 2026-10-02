import subprocess
import threading

def run_client(n):
    subprocess.run(["python3", "tcp_socket/simple_client.py", str(n)])

threads = []
for i in range(5):
    t = threading.Thread(target=run_client, args=(i,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print("All clients finished.")
