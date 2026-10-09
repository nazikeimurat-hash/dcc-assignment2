import argparse
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc

RETRY_CODES = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)


class CounterClient:
    def __init__(self, address, timeout=2.0):
        self._channel = grpc.insecure_channel(address)
        self._stub = counter_pb2_grpc.CounterStub(self._channel)
        self._timeout = timeout

    def incr(self, counter_id, delta, key=None):
        # Key бір логикалық әрекетке БІР рет жасалады, retry кезінде өзгермейді
        if key is None:
            key = str(uuid.uuid4())
        request = counter_pb2.IncrementRequest(
            counter_id=counter_id, delta=delta, idempotency_key=key
        )
        backoff = 0.2
        for attempt in range(4):  # 1 алғашқы әрекет + 3 retry
            try:
                return self._stub.Increment(request, timeout=self._timeout)
            except grpc.RpcError as e:
                if e.code() not in RETRY_CODES or attempt == 3:
                    raise
                time.sleep(backoff)
                backoff *= 2  # 0.2 -> 0.4 -> 0.8

    def get(self, counter_id):
        request = counter_pb2.GetRequest(counter_id=counter_id)
        return self._stub.Get(request, timeout=self._timeout)

    def close(self):
        self._channel.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_incr = sub.add_parser("incr")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1)
    p_incr.add_argument("--key", default=None)

    p_get = sub.add_parser("get")
    p_get.add_argument("counter_id")

    args = parser.parse_args()
    client = CounterClient(f"localhost:{args.port}")

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