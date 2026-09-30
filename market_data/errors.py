"""Typed market-data failures.

Codes deliberately match Portfolio Lab's `ErrorCode` union so the TypeScript side
maps them without translation. `category` drives retries, the circuit breaker and
logging; it never leaves the service.
"""

# category -> (retry automatically?, counts toward the circuit breaker?)
CATEGORIES = {
    "transient": (True, True),  # timeout, connection reset, temporary 5xx
    "throttled": (True, True),  # upstream rate limiting
    "unavailable": (False, False),  # circuit open or queue full: fail fast
    "not_found": (False, False),  # unknown or delisted ticker
    "no_history": (False, False),  # nothing in the requested range
    "unsupported": (False, False),  # outside the USD U.S. equity/ETF universe
    "malformed": (False, True),  # upstream returned something unusable
    "invalid": (False, False),  # the request itself is wrong
    "provider": (False, True),  # other upstream failure, not known to be transient
}


class MarketDataError(Exception):
    def __init__(self, code: str, message: str, category: str, retryable: bool | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.category = category
        auto_retry, _ = CATEGORIES[category]
        self.retryable = auto_retry if retryable is None else retryable

    @property
    def auto_retry(self) -> bool:
        return CATEGORIES[self.category][0]

    @property
    def trips_circuit(self) -> bool:
        return CATEGORIES[self.category][1]

    def to_json(self, ticker: str | None = None) -> dict:
        out = {"code": self.code, "message": self.message, "retryable": self.retryable}
        if ticker:
            out["ticker"] = ticker
        return out


def invalid(message: str) -> MarketDataError:
    return MarketDataError("INVALID_INPUT", message, "invalid", retryable=False)
