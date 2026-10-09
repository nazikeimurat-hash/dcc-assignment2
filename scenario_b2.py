import os
import threading
import time

import clocks
from client import CounterClient
from server import serve

os.makedirs("logs", exist_ok=True)
clocks.LOG_FILE = "logs/b2_trace.log"
open(clocks.LOG_FILE, "w").close()  # ескі логты тазалау

server, port = serve(0, name="replica-A")
addr = f"localhost:{port}"


def run_client1():
    c = CounterClient(addr, name="client-1", verbose=True)
    c.incr("x", 1)
    c.incr("x", 1)


def run_client2():
    c = CounterClient(addr, name="client-2", verbose=True)
    c.incr("y", 1)
    c.incr("y", 1)


def run_client3():
    c = CounterClient(addr, name="client-3", verbose=True)
    for _ in range(3):
        c.get("x")
        time.sleep(0.05)


threads = [threading.Thread(target=f) for f in (run_client1, run_client2, run_client3)]
for t in threads:
    t.start()
for t in threads:
    t.join()

server.stop(0)
print("\nЛог сақталды: logs/b2_trace.log")