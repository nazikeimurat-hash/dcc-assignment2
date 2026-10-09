import argparse
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock

RETRY_CODES = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)


class CounterClient:
    def __init__(self, address, timeout=2.0, name="client-1", verbose=False):
        self._channel = grpc.insecure_channel(address)
        self._stub = counter_pb2_grpc.CounterStub(self._channel)
        self._timeout = timeout
        self.clock = LamportClock(name)
        self._verbose = verbose

    def _log(self, msg):
        if self._verbose:
            self.clock.log(msg)

    def incr(self, counter_id, delta, key=None):
        # Key бір логикалық әрекетке БІР рет жасалады, retry кезінде өзгермейді
        if key is None:
            key = str(uuid.uuid4())
        backoff = 0.2
        for attempt in range(4):  # 1 алғашқы әрекет + 3 retry
            t = self.clock.tick()
            request = counter_pb2.IncrementRequest(
                counter_id=counter_id, delta=delta,
                idempotency_key=key, lamport_time=t,
            )
            self._log(f"SEND Increment(counter={counter_id}, delta={delta}) L={t}")
            try:
                reply = self._stub.Increment(request, timeout=self._timeout)
                t = self.clock.receive(reply.lamport_time)
                self._log(f"RECV IncrementReply(new_value={reply.new_value}) "
                          f"L={t} (received L={reply.lamport_time})")
                return reply
            except grpc.RpcError as e:
                if e.code() not in RETRY_CODES or attempt == 3:
                    raise
                time.sleep(backoff)
                backoff *= 2  # 0.2 -> 0.4 -> 0.8

    def get(self, counter_id):
        t = self.clock.tick()
        request = counter_pb2.GetRequest(counter_id=counter_id, lamport_time=t)
        self._log(f"SEND Get(counter={counter_id}) L={t}")
        reply = self._stub.Get(request, timeout=self._timeout)
        t = self.clock.receive(reply.lamport_time)
        self._log(f"RECV GetReply(value={reply.value}) L={t} "
                  f"(received L={reply.lamport_time})")
        return reply

    def close(self):
        self._channel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default="client-1")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_incr = sub.add_parser("incr")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1)
    p_incr.add_argument("--key", default=None)

    p_get = sub.add_parser("get")
    p_get.add_argument("counter_id")

    args = parser.parse_args()
    client = CounterClient(f"localhost:{args.port}", name=args.name, verbose=True)

    if args.cmd == "incr":
        r = client.incr(args.counter_id, args.by, args.key)
        dup = "yes" if r.was_duplicate else "no"
        print(f"OK committed value={r.new_value} (duplicate: {dup})")
    else:
        r = client.get(args.counter_id)
        if r.found:
            print(f"value={r.value}")
        else:
            print("counter not found")