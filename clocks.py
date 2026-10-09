import threading


class LamportClock:
    """Lamport логикалық сағаты. Thread-safe."""

    def __init__(self, name):
        self.name = name
        self._time = 0
        self._lock = threading.Lock()

    def tick(self):
        """Жергілікті оқиға алдында: сағатты 1-ге арттыр."""
        with self._lock:
            self._time += 1
            return self._time

    def receive(self, received_time):
        """Хабар алғанда: max(own, received) + 1."""
        with self._lock:
            self._time = max(self._time, received_time) + 1
            return self._time

    def log(self, message):
        print(f"[{self.name}] {message}", flush=True)