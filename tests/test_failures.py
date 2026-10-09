import os
import socket
import subprocess
import sys
import threading
import time
from concurrent import futures

import grpc
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import counter_pb2_grpc
from client import CounterClient
from replicated import ReplicatedClient
from server import CounterServicer


def free_port():
    s = socket.socket()
    s.bind(("localhost", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture
def process_replicas():
    """3 replica-ны бөлек процесс ретінде іске қосады (subprocess.Popen)."""
    procs = []


    def _start(delays=(0, 0, 0)):
        addrs = []
        for i, d in enumerate(delays):
            port = free_port()
            p = subprocess.Popen(
                [sys.executable, "server.py", "--port", str(port),
                 "--name", f"replica-{i}", "--delay-ms", str(d)],
                cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            procs.append(p)
            addr = f"localhost:{port}"
            ch = grpc.insecure_channel(addr)
            grpc.channel_ready_future(ch).result(timeout=15)  # сервер дайын болғанша күтеміз
            ch.close()
            addrs.append(addr)
        return addrs, procs

    yield _start
    for p in procs:
        p.kill()
        p.wait()


@pytest.fixture
def inproc_replicas():
    servers, servicers = [], []

    def _start(n=3):
        addrs = []
        for i in range(n):
            sv = CounterServicer(name=f"replica-{i}")

            server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
            counter_pb2_grpc.add_CounterServicer_to_server(sv, server)
            port = server.add_insecure_port("localhost:0")
            server.start()
            servers.append(server)
            servicers.append(sv)
            addrs.append(f"localhost:{port}")
        return addrs, servicers

    yield _start
    for s in servers:
        s.stop(0)


def test_crash_mid_request_still_commits(process_replicas):
    # replica-1 баяу (800 мс), сондықтан жазу "ұшып жүргенде" оны өлтіре аламыз
    addrs, procs = process_replicas(delays=(0, 800, 0))
    rc = ReplicatedClient(addrs, timeout=0.5)
    result = {}

    def do_write():
        try:
            result["r"] = rc.incr("x", 1)
        except Exception as e:  # ештеңе сыртқа шықпауы керек
            result["error"] = e

    t = threading.Thread(target=do_write)
    t.start()
    time.sleep(0.2)
    procs[1].kill()  # жазу әлі жүріп жатқанда replica өледі
    t.join(timeout=30)


    assert "error" not in result           # caller-ге exception шықпады
    assert result["r"].committed is True   # қалған 2 ack жеткілікті
    assert result["r"].acks == 2


def test_request_duplication_moves_value_once(process_replicas):
    addrs, _ = process_replicas()
    rc = ReplicatedClient(addrs, timeout=2.0)

    r1 = rc.incr("x", 1, key="dup-key")
    r2 = rc.incr("x", 1, key="dup-key")  # сол key

    assert r1.committed and not r1.was_duplicate
    assert r2.committed and r2.was_duplicate
    for a in addrs:  # әр replica-да мән бір-ақ рет өзгерген
        assert CounterClient(a).get("x").value == 1


def test_induced_timeout_retry_moves_once(inproc_replicas):
    addrs, servicers = inproc_replicas()
    servicers[2]._delay = 1.0  # replica-2 клиент deadline-ынан (0.4 с) баяу
    rc = ReplicatedClient(addrs, timeout=0.4)
    result = {}

    t = threading.Thread(target=lambda: result.update(r=rc.incr("x", 1)))
    t.start()
    time.sleep(0.5)
    servicers[2]._delay = 0  # retry келгенде replica жылдам жауап береді
    t.join(timeout=30)
    time.sleep(1.2)  # бірінші (timeout болған) сұраныс аяқталып үлгерсін

    assert result["r"].committed is True

    assert result["r"].acks == 3
    assert [s._values for s in servicers] == [{"x": 1}] * 3  # дәл бір рет