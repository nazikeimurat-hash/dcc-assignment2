import argparse
import math
import statistics
import subprocess
import sys
import threading
import time
import uuid

import grpc

from client import CounterClient
from replicated import ReplicatedClient

ROOT = "."


def start_replicas(n):
    import socket
    procs, addrs = [], []
    for i in range(n):
        s = socket.socket()
        s.bind(("localhost", 0))
        port = s.getsockname()[1]
        s.close()
        p = subprocess.Popen(
            [sys.executable, "server.py", "--port", str(port), "--name", f"replica-{i}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        procs.append(p)
        addr = f"localhost:{port}"
        ch = grpc.insecure_channel(addr)
        grpc.channel_ready_future(ch).result(timeout=15)
        ch.close()
        addrs.append(addr)
    return procs, addrs


def percentile(sorted_vals, p):
    # тапсырма бойынша: сұрыпталған тізімде ceil(p*n) индексіндегі мән
    n = len(sorted_vals)
    idx = min(math.ceil(p * n), n) - 1  # 1-based -> 0-based
    return sorted_vals[idx]


def run(make_client, clients, total):
    per_client = total // clients
    latencies = []
    lock = threading.Lock()

    def worker():
        c = make_client()
        local = []
        for _ in range(per_client):
            t0 = time.perf_counter()
            c.incr("bench", 1)
            local.append((time.perf_counter() - t0) * 1000)
        with lock:
            latencies.extend(local)

    t_start = time.perf_counter()
    threads = [threading.Thread(target=worker) for _ in range(clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t_start
    latencies.sort()
    return {
        "median": statistics.median(latencies),
        "p95": percentile(latencies, 0.95),
        "n": len(latencies),
        "throughput": len(latencies) / elapsed,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=2000)
    args = parser.parse_args()

    procs, addrs = start_replicas(3)
    try:
        # Warm-up
        warm = CounterClient(addrs[0])
        for _ in range(50):
            warm.incr("warm", 1)

        rows = []
        for label, make, clients in [
            ("Single replica, 1 client", lambda: CounterClient(addrs[0], timeout=10), 1),
            ("Single replica, 16 clients", lambda: CounterClient(addrs[0], timeout=10), 16),
            ("Quorum (3 replicas), 1 client", lambda: ReplicatedClient(addrs, timeout=10), 1),
            ("Quorum (3 replicas), 16 clients", lambda: ReplicatedClient(addrs, timeout=10), 16),
        ]:
            r = run(make, clients, args.requests)
            rows.append((label, r))
            print(f"done: {label}", flush=True)

        print()
        print("| Configuration | Median latency (ms) | p95 latency (ms) | Requests | Throughput (req/s) |")
        print("|---|---|---|---|---|")
        for label, r in rows:
            print(f"| {label} | {r['median']:.2f} | {r['p95']:.2f} | {r['n']} | {r['throughput']:.0f} |")
    finally:
        for p in procs:
            p.kill()
            p.wait()