# Replicated Counter Service

gRPC counter service with Lamport clocks, majority-quorum writes (3 replicas),
automated tests, and a latency benchmark. Commands below are for Windows PowerShell.

## Setup

```
python -m venv .venv
.venv\Scripts\activate
pip install grpcio grpcio-tools pytest
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```

## Start one replica

```
python server.py --port 50051 --name replica-A
```

Client (second terminal):

```
python client.py --name client-1 incr likes:post-42 --by 1
python client.py get likes:post-42
python client.py incr x --by 5 --key abc
```

## Start three replicas

Run each command in its own terminal:


```
python server.py --port 50051 --name replica-A
python server.py --port 50052 --name replica-B
python server.py --port 50053 --name replica-C
```

Quorum client (Python):

```python
from replicated import ReplicatedClient
rc = ReplicatedClient(["localhost:50051", "localhost:50052", "localhost:50053"])
print(rc.incr("likes:post-42", 1))
```

## Run the test suite

```
python -m pytest tests -v
```

Expected: 11 passed (8 in `tests/test_counter.py`, 3 in `tests/test_failures.py`).
Tests start their own servers on ephemeral ports; no manual setup is needed.

## Reproduce the Lamport trace (Task B2)

```
python scenario_b2.py
```

Writes `logs/b2_trace.log`.

## Run the benchmark (Task C4)


```
python perf_benchmark.py
```

Prints the median/p95 table. Results from the submitted run are in `logs/perf_results.txt`.