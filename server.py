import argparse
import threading
import time
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, delay_ms=0, name="replica"):
        self._lock = threading.Lock()
        self._values = {}   # counter_id -> int
        self._seen = {}     # idempotency_key -> (counter_id, resulting value)
        self._delay = delay_ms / 1000.0
        self.clock = LamportClock(name)

    def Increment(self, request, context):
        if self._delay:
            time.sleep(self._delay)
        c = self.clock
        t = c.receive(request.lamport_time)
        c.log(f"RECV Increment(counter={request.counter_id}, delta={request.delta}) "
              f"L={t} (received L={request.lamport_time})")
        with self._lock:
            if request.idempotency_key in self._seen:
                _, value = self._seen[request.idempotency_key]
                dup = True
            else:

                value = self._values.get(request.counter_id, 0) + request.delta
                self._values[request.counter_id] = value
                self._seen[request.idempotency_key] = (request.counter_id, value)
                dup = False
                t = c.tick()
                c.log(f"APPLY counter={request.counter_id} -> {value} L={t}")
        t = c.tick()
        c.log(f"SEND IncrementReply(new_value={value}) L={t}")
        return counter_pb2.IncrementReply(
            new_value=value, was_duplicate=dup, lamport_time=t
        )

    def Get(self, request, context):
        c = self.clock
        t = c.receive(request.lamport_time)
        c.log(f"RECV Get(counter={request.counter_id}) L={t} "
              f"(received L={request.lamport_time})")
        with self._lock:
            found = request.counter_id in self._values
            value = self._values.get(request.counter_id, 0)
        t = c.tick()
        c.log(f"SEND GetReply(value={value}, found={found}) L={t}")
        return counter_pb2.GetReply(value=value, found=found, lamport_time=t)


def serve(port, delay_ms=0, name="replica"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(delay_ms, name), server
    )
    bound_port = server.add_insecure_port(f"localhost:{port}")
    server.start()

    return server, bound_port


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--name", default="replica-A")
    args = parser.parse_args()

    server, port = serve(args.port, args.delay_ms, args.name)
    print(f"Server started on port {port}", flush=True)
    server.wait_for_termination()