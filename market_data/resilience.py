"""Retries, circuit breaker, single-flight coalescing and a bounded upstream gate.

All four are clock/sleep-injectable so tests run instantly and deterministically.
"""
import random
import threading
import time
from concurrent.futures import Future

from .errors import MarketDataError


class RetryPolicy:
    """Exponential backoff with full jitter, for transient and throttled failures only."""

    def __init__(self, attempts: int = 3, throttled_attempts: int = 2, base: float = 0.6,
                 cap: float = 6.0, sleep=time.sleep, rand=random.random):
        self.attempts = attempts
        self.throttled_attempts = throttled_attempts
        self.base = base
        self.cap = cap
        self.sleep = sleep
        self.rand = rand

    def delay(self, attempt: int, throttled: bool) -> float:
        base = self.base * (4 if throttled else 1)
        ceiling = min(self.cap, base * 2 ** attempt)
        return ceiling / 2 + self.rand() * ceiling / 2

    def run(self, fn, stats: dict):
        attempt = 0
        while True:
            try:
                return fn()
            except MarketDataError as err:
                limit = self.throttled_attempts if err.category == "throttled" else self.attempts
                attempt += 1
                if not err.auto_retry or attempt >= limit:
                    raise
                stats["retries"] = stats.get("retries", 0) + 1
                self.sleep(self.delay(attempt - 1, err.category == "throttled"))


class CircuitBreaker:
    """closed -> open after `threshold` consecutive upstream failures; open rejects
    calls until the cooldown passes; half-open lets exactly one probe through.
    A failed probe reopens with a doubled cooldown (capped)."""

    def __init__(self, threshold: int = 5, cooldown: float = 30.0, max_cooldown: float = 300.0,
                 clock=time.monotonic):
        self.threshold = threshold
        self.base_cooldown = cooldown
        self.max_cooldown = max_cooldown
        self.clock = clock
        self._lock = threading.Lock()
        self.failures = 0
        self.state = "closed"
        self.opened_at = 0.0
        self.cooldown = cooldown
        self._probe = False

    def retry_after(self) -> float:
        with self._lock:
            if self.state != "open":
                return 0.0
            return max(0.0, self.opened_at + self.cooldown - self.clock())

    def allow(self) -> bool:
        with self._lock:
            if self.state == "closed":
                return True
            if self.state == "open" and self.clock() >= self.opened_at + self.cooldown:
                self.state = "half_open"
                self._probe = False
            if self.state == "half_open" and not self._probe:
                self._probe = True
                return True
            return False

    def success(self):
        with self._lock:
            self.failures = 0
            self.state = "closed"
            self.cooldown = self.base_cooldown
            self._probe = False

    def failure(self):
        with self._lock:
            if self.state == "half_open":
                self.cooldown = min(self.max_cooldown, self.cooldown * 2)
                self._open()
                return
            self.failures += 1
            if self.failures >= self.threshold:
                self._open()

    def _open(self):
        self.state = "open"
        self.opened_at = self.clock()
        self._probe = False

    def snapshot(self) -> dict:
        return {"state": self.state, "consecutiveFailures": self.failures,
                "retryAfterSeconds": round(self.retry_after(), 1)}


class SingleFlight:
    """Concurrent calls with the same key share one execution and its result."""

    def __init__(self):
        self._lock = threading.Lock()
        self._calls: dict = {}

    def do(self, key, fn):
        with self._lock:
            future = self._calls.get(key)
            leader = future is None
            if leader:
                future = Future()
                self._calls[key] = future
        if not leader:
            return future.result(), True
        try:
            result = fn()
            future.set_result(result)
            return result, False
        except BaseException as exc:
            future.set_exception(exc)
            raise
        finally:
            with self._lock:
                self._calls.pop(key, None)


class UpstreamGate:
    """Bounds concurrent upstream (yfinance) work; excess waits in a queue up to
    `queue_timeout` seconds, then fails fast as unavailable."""

    def __init__(self, limit: int = 4, queue_timeout: float = 30.0):
        self.limit = limit
        self.queue_timeout = queue_timeout
        self._sem = threading.BoundedSemaphore(limit)
        self._lock = threading.Lock()
        self.active = 0
        self.peak = 0

    def run(self, fn):
        if not self._sem.acquire(timeout=self.queue_timeout):
            raise MarketDataError("PROVIDER_ERROR", "Market-data service is busy; try again shortly.",
                                  "unavailable", retryable=True)
        with self._lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            return fn()
        finally:
            with self._lock:
                self.active -= 1
            self._sem.release()
