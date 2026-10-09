import sys
from replicated import ReplicatedClient

addrs = ["localhost:50051", "localhost:50052", "localhost:50053"]
rc = ReplicatedClient(addrs, timeout=1.0)
key = sys.argv[1] if len(sys.argv) > 1 else None
r = rc.incr("likes:post-42", 1, key)
print(f"committed={r.committed} value={r.value} acks={r.acks}/{r.total} duplicate={r.was_duplicate}")