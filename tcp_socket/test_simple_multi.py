import os
import subprocess
import sys
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
CLIENT = os.path.join(HERE, "simple_client.py")


def run_client(n):
    subprocess.run([sys.executable, CLIENT, str(n)])


threads = []
for i in range(5):
    t = threading.Thread(target=run_client, args=(i,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print("All clients finished.")
