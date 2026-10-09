import os
import sys
import threading
import time
from concurrent import futures

import grpc
import pytest

# tests/ папкасынан түбірдегі server.py, client.py файлдарын табу үшін
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import counter_pb2
import counter_pb2_grpc
from client import CounterClient
from replicated import ReplicatedClient
from server import CounterServicer


@pytest.fixture
def make_server():
    """Серверді ephemeral портта іске қосады, тест біткен соң тоқтатады."""
    started = []

    def _make(delay_ms=0):
        servicer = CounterServicer(delay_ms)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
        port = server.add_insecure_port("localhost:0")  # 0 = бос портты өзі таңдайды
        server.start()
        started.append(server)
        return servicer, port

    yield _make
    for s in started:
        s.stop(0)


def test_increment_applies_delta(make_server):
    _, port = make_server()
    client = CounterClient(f"localhost:{port}")

    r = client.incr("x", 7, key="k-1")

    assert r.new_value == 7
    assert not r.was_duplicate
    assert client.get("x").value == 7


def test_duplicate_key_not_reapplied(make_server):
    _, port = make_server()
    client = CounterClient(f"localhost:{port}")

    r1 = client.incr("x", 5, key="k-1")
    r2 = client.incr("x", 5, key="k-1")  # сол key-мен қайталау

    assert r1.new_value == 5
    assert r2.new_value == 5 and r2.was_duplicate  # 10 емес
    assert client.get("x").value == 5


def test_concurrent_increments_exact(make_server):
    _, port = make_server()

    def worker():
        c = CounterClient(f"localhost:{port}", timeout=10.0)
        for _ in range(1000):
            c.incr("x", 1)  # әр шақыруға жаңа uuid key

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    client = CounterClient(f"localhost:{port}")
    assert client.get("x").value == 2000


def test_get_missing_counter(make_server):
    _, port = make_server()
    client = CounterClient(f"localhost:{port}")

    r = client.get("does-not-exist")

    assert r.found is False


def test_retry_after_timeout_is_safe(make_server):
    servicer, port = make_server(delay_ms=1000)  # сервер әдейі баяу
    client = CounterClient(f"localhost:{port}")
    request = counter_pb2.IncrementRequest(
        counter_id="x", delta=1, idempotency_key="k-retry"
    )

    # 1-әрекет: клиент 0.3 с күтеді, сервер 1 с ұйықтайды -> timeout
    with pytest.raises(grpc.RpcError) as exc:
        client._stub.Increment(request, timeout=0.3)
    assert exc.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED

    # Серверді жылдамдатып, сол key-мен қайталаймыз
    servicer._delay = 0
    client.incr("x", 1, key="k-retry")

    time.sleep(1.5)  # бірінші (timeout болған) сұраныс аяқталып үлгерсін
    assert client.get("x").value == 1  # екі рет емес, бір-ақ рет артты

@pytest.fixture
def make_replicas():
    servers = []
    servicers = []

    def _make(n=3):
        addrs = []
        for i in range(n):
            servicer = CounterServicer(name=f"replica-{i}")
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
            counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
            port = server.add_insecure_port("localhost:0")
            server.start()
            servers.append(server)
            servicers.append(servicer)
            addrs.append(f"localhost:{port}")
        return addrs, servers, servicers

    yield _make
    for s in servers:
        s.stop(0)


def test_majority_commit_two_acks(make_replicas):
    addrs, servers, _ = make_replicas()
    servers[1].stop(0)  # бір replica өшті
    rc = ReplicatedClient(addrs, timeout=0.5)

    r = rc.incr("x", 1)

    assert r.committed is True
    assert r.acks == 2



def test_no_commit_below_majority(make_replicas):
    addrs, servers, servicers = make_replicas()
    servers[1].stop(0)
    servers[2].stop(0)  # екі replica өшті
    rc = ReplicatedClient(addrs, timeout=0.5)

    r = rc.incr("x", 1)

    assert r.committed is False  # клиентке "commit" деп көрсетілмейді
    assert r.acks == 1
    # Аномалия: тірі replica жазуды қолданып қойған, бірақ ол commit емес
    assert servicers[0]._values.get("x") == 1


def test_replicas_converge(make_replicas):
    addrs, _, servicers = make_replicas()
    rc = ReplicatedClient(addrs, timeout=2.0)

    for _ in range(20):
        assert rc.incr("x", 1).committed

    assert [s._values for s in servicers] == [{"x": 20}] * 3