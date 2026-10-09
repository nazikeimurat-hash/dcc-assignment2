import uuid
from concurrent import futures
from dataclasses import dataclass

import grpc

from client import CounterClient


@dataclass
class QuorumResult:
    committed: bool
    value: int
    acks: int
    total: int
    was_duplicate: bool


class ReplicatedClient:
    """Жазуды барлық replica-ға жібереді; majority (>=2/3) ack болса ғана commit."""

    def __init__(self, addresses, timeout=2.0, name="client-1", verbose=False):
        self._clients = [
            CounterClient(a, timeout=timeout, name=name, verbose=verbose)
            for a in addresses
        ]
        self._total = len(addresses)
        self._majority = self._total // 2 + 1
        self._pool = futures.ThreadPoolExecutor(max_workers=self._total)

    def incr(self, counter_id, delta, key=None):
        if key is None:
            key = str(uuid.uuid4())  # бір логикалық әрекетке бір key
        futs = [
            self._pool.submit(c.incr, counter_id, delta, key) for c in self._clients
        ]
        replies = []
        for f in futs:
            try:
                replies.append(f.result())
            except grpc.RpcError:
                pass  # бұл replica жауап бермеді
        acks = len(replies)
        committed = acks >= self._majority
        value = replies[0].new_value if replies else 0
        dup = bool(replies) and all(r.was_duplicate for r in replies)
        return QuorumResult(committed, value, acks, self._total, dup)

    def get(self, counter_id):
        # кез келген бір replica-дан оқимыз (тапсырма солай рұқсат етеді)
        for c in self._clients:
            try:
                return c.get(counter_id)
            except grpc.RpcError:
                continue
        raise RuntimeError("no replica available")