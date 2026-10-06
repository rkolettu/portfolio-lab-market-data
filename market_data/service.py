"""Market-data service: retrieval, normalization, caching, provider health.

It never computes returns or any portfolio statistic; it serves normalized daily
adjusted closes and raw quote observations with provenance.
"""
import datetime as dt
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from . import cache as cache_mod
from .errors import MarketDataError
from .resilience import CircuitBreaker, RetryPolicy, SingleFlight, UpstreamGate
from .sessions import final_session, now_ny

log = logging.getLogger("market_data")

# Refresh overlap and the tolerance for "the same observation". Yahoo re-derives
# adjclose on every request and rounds it to float32, so identical requests differ
# by up to ~1e-6 relative; a dividend or split rescales earlier values by far more
# (even a $0.01 dividend on a $500 share is 2e-5), and so does a corrected close.
OVERLAP_DAYS = 14
TOLERANCE = 5e-6
QUOTE_TTL = 60.0
NEGATIVE_TTL = 3600.0
# An OTC history is refused for a window when more than this share of its
# sessions (and more than STALE_MIN_SESSIONS) had no trades: Yahoo repeats the
# last price on those days, which would understate volatility and correlation.
STALE_SHARE = 0.10
STALE_MIN_SESSIONS = 5
COMMON = ["SPY", "QQQ", "IWM", "BND", "GLD"]


def _utc(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class MarketDataService:
    def __init__(self, provider, cache=None, *, breaker=None, retry=None, gate=None,
                 now=now_ny, wall=time.time, mono=time.monotonic, workers: int = 8):
        self.provider = provider
        self.cache = cache or cache_mod.HistoryCache()
        self.breaker = breaker or CircuitBreaker()
        self.retry = retry or RetryPolicy()
        self.gate = gate or UpstreamGate()
        self.flight = SingleFlight()
        self.now, self.wall, self.mono = now, wall, mono
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="market-data")
        self._negative: dict = {}
        self._quotes: dict = {}
        self._lock = threading.Lock()
        self.started = mono()
        self.prewarm_state = {"state": "idle", "done": 0, "total": 0}

    # ---- upstream -------------------------------------------------------------
    def _call(self, fn, stats: dict):
        """One upstream operation behind the circuit breaker, gate and retry policy."""
        if not self.breaker.allow():
            raise MarketDataError(
                "PROVIDER_ERROR",
                f"Market-data provider is temporarily unavailable; retry in "
                f"{max(1, round(self.breaker.retry_after()))} s.",
                "unavailable", retryable=True)
        started = self.mono()
        try:
            result = self.retry.run(lambda: self.gate.run(fn), stats)
        except MarketDataError as err:
            if err.trips_circuit:
                self.breaker.failure()
            elif err.category != "unavailable":
                self.breaker.success()  # the provider answered; the request was the problem
            raise
        finally:
            stats["upstreamMs"] = stats.get("upstreamMs", 0) + round((self.mono() - started) * 1000)
            stats["upstreamCalls"] = stats.get("upstreamCalls", 0) + 1
        self.breaker.success()
        return result

    def _key(self, ticker: str) -> tuple:
        return (self.provider.name, ticker, "1d", self.provider.convention)

    def _full(self, ticker: str, target: dt.date, stats: dict) -> cache_mod.Entry:
        raw = self._call(lambda: self.provider.history(ticker, None, target + dt.timedelta(days=1)), stats)
        rows = [r for r in raw["rows"] if r[0] <= target.isoformat()]
        dates, closes = cache_mod.build(rows)
        now = self.wall()
        stats["full"] = stats.get("full", 0) + 1
        untraded = cache_mod.ordinals(d for d in raw.get("untraded", ()) if d <= target.isoformat())
        return cache_mod.Entry(raw["meta"], dates, closes, _utc(now), _utc(now), self.mono(), target,
                               untraded=untraded)

    def _incremental(self, ticker: str, entry: cache_mod.Entry, target: dt.date,
                     stats: dict) -> cache_mod.Entry:
        last = entry.last_date
        if last is None:
            return self._full(ticker, target, stats)
        since = last - dt.timedelta(days=OVERLAP_DAYS)
        raw = self._call(lambda: self.provider.history(ticker, since, target + dt.timedelta(days=1)), stats)
        rows = [r for r in raw["rows"] if r[0] <= target.isoformat()]
        fresh = {day: price for day, price in rows if day <= last.isoformat()}
        old_dates, old_closes = entry.window(since, last)
        same = set(fresh) == set(old_dates) and all(
            abs(fresh[d] / p - 1) <= TOLERANCE for d, p in zip(old_dates, old_closes))
        if not same:
            # Dividend/split re-basing or a corrected close: never splice across it.
            stats["corrections"] = stats.get("corrections", 0) + 1
            log.info("market_data correction", extra={"fields": {"ticker": ticker, "since": since.isoformat()}})
            return self._full(ticker, target, stats)
        dates, closes = cache_mod.array("i", entry.dates), cache_mod.array("d", entry.closes)
        for day, price in rows:
            if day > last.isoformat():  # cached values win for overlapping sessions
                dates.append(dt.date.fromisoformat(day).toordinal())
                closes.append(price)
        added = cache_mod.ordinals(d for d in raw.get("untraded", ()) if last.isoformat() < d <= target.isoformat())
        stats["incremental"] = stats.get("incremental", 0) + 1
        return cache_mod.Entry(raw["meta"], dates, closes, entry.fetched_at, _utc(self.wall()),
                               self.mono(), target, untraded=entry.untraded | added)

    def _ensure(self, ticker: str, target: dt.date, stats: dict) -> tuple[cache_mod.Entry, str | None]:
        """Entry complete through `target`, or the cached entry marked stale when the
        provider cannot be reached. One upstream fetch per key at a time."""
        key = self._key(ticker)

        def load():
            entry = self.cache.get(key)
            if entry is not None and entry.checked_through >= target:
                return entry, None, "hit"
            try:
                fresh = (self._incremental(ticker, entry, target, stats) if entry is not None
                         else self._full(ticker, target, stats))
            except MarketDataError as err:
                if entry is not None and err.category in {"transient", "throttled", "unavailable", "provider",
                                                          "malformed"}:
                    return entry, err.message, "stale"
                raise
            self.cache.put(key, fresh)
            return fresh, None, ("refresh" if entry is not None else "miss")

        (entry, stale, how), shared = self.flight.do(key, load)
        stats.setdefault("cache", {}).setdefault("shared" if shared else how, 0)
        stats["cache"]["shared" if shared else how] += 1
        return entry, stale

    # ---- public ---------------------------------------------------------------
    def history(self, ticker: str, start: dt.date, end: dt.date, stats: dict) -> dict:
        blocked = self._negative.get(ticker)
        if blocked and blocked[0] > self.mono():
            raise blocked[1]
        target = min(end, final_session(self.now()))
        entry = self.cache.get(self._key(ticker))
        stale = None
        if entry is not None and entry.checked_through >= target:
            stats.setdefault("cache", {}).setdefault("hit", 0)
            stats["cache"]["hit"] += 1
        else:
            try:
                entry, stale = self._ensure(ticker, target, stats)
            except MarketDataError as err:
                if err.category in {"not_found", "unsupported"}:
                    self._negative[ticker] = (self.mono() + NEGATIVE_TTL, err)
                raise
        dates, closes = entry.window(start, min(end, target))
        if not dates:
            raise MarketDataError("INSUFFICIENT_HISTORY",
                                  f"{ticker} has no history in the requested range; it may have listed later.",
                                  "no_history")
        meta = entry.meta
        if entry.untraded:
            lo, hi = dt.date.fromisoformat(dates[0]).toordinal(), dt.date.fromisoformat(dates[-1]).toordinal()
            untraded = sum(1 for o in entry.untraded if lo <= o <= hi)
            if untraded > max(STALE_MIN_SESSIONS, STALE_SHARE * len(dates)):
                # Per window, so never negatively cached: a recent window may be fine.
                raise MarketDataError(
                    "UNSUPPORTED_ASSET",
                    f"{ticker} trades over the counter too thinly for daily analysis: {untraded} of "
                    f"{len(dates)} sessions in this period had no trades, so its prices are stale.",
                    "unsupported")
        return {
            "ok": True, "ticker": ticker, "currency": meta["currency"], "exchange": meta["exchange"],
            "instrument": meta["instrument"], "firstTradeDate": meta.get("firstTradeDate"),
            "dates": dates, "adjustedClose": closes,
            "provenance": {
                "fetchedAt": entry.refreshed_at, "fullHistoryFetchedAt": entry.fetched_at,
                "lastSuccessfulRefresh": entry.refreshed_at,
                "cacheAgeSeconds": round(max(0.0, self.mono() - entry.refreshed_mono), 1),
                "checkedThrough": entry.checked_through.isoformat(), "observationDate": dates[-1],
                "stale": stale is not None, **({"staleReason": stale} if stale else {}),
            },
        }

    def history_batch(self, tickers: list[str], start: dt.date, end: dt.date, stats: dict) -> list[dict]:
        def one(ticker):
            try:
                return self.history(ticker, start, end, stats)
            except MarketDataError as err:
                stats.setdefault("errors", {}).setdefault(err.category, 0)
                stats["errors"][err.category] += 1
                return {"ok": False, "ticker": ticker, "error": err.to_json(ticker)}
        return list(self.pool.map(one, tickers))  # input order, independent of completion

    def quote(self, ticker: str, stats: dict) -> dict:
        with self._lock:
            hit = self._quotes.get(ticker)
        if hit and hit[0] > self.mono():
            stats.setdefault("cache", {}).setdefault("hit", 0)
            stats["cache"]["hit"] += 1
            return hit[1]
        today = self.now().date()

        def load():
            raw = self._call(lambda: self.provider.quote(ticker, today), stats)
            meta = raw["meta"]
            value = {"ok": True, "ticker": ticker, "currency": meta["currency"], "exchange": meta["exchange"],
                     "instrument": meta["instrument"], "regularMarketPrice": raw["regularMarketPrice"],
                     "regularMarketTime": raw["regularMarketTime"], "recentCloses": raw["recentCloses"],
                     "retrievedAt": _utc(self.wall())}
            with self._lock:
                self._quotes[ticker] = (self.mono() + QUOTE_TTL, value)
            return value
        value, _ = self.flight.do(("quote", ticker), load)
        return value

    def quote_batch(self, tickers: list[str], stats: dict) -> list[dict]:
        def one(ticker):
            try:
                return self.quote(ticker, stats)
            except MarketDataError as err:
                stats.setdefault("errors", {}).setdefault(err.category, 0)
                stats["errors"][err.category] += 1
                return {"ok": False, "ticker": ticker, "error": err.to_json(ticker)}
        return list(self.pool.map(one, tickers))

    def refresh(self, tickers: list[str], stats: dict) -> dict:
        """Bring cached histories up to the latest final session (incremental), one at a
        time to keep upstream traffic gentle."""
        target = final_session(self.now())
        out = {}
        for t in tickers:
            try:
                before = stats.get("upstreamCalls", 0)
                self._negative.pop(t, None)
                self._ensure(t, target, stats)
                out[t] = "refreshed" if stats.get("upstreamCalls", 0) > before else "current"
            except MarketDataError as err:
                out[t] = err.code
        return out

    def prewarm(self, tickers=COMMON):
        """Full histories for the common universe: covers the sample portfolio, the
        default benchmark and every stress window. Sequential and in the background,
        so readiness is never delayed."""
        self.prewarm_state = {"state": "running", "done": 0, "total": len(tickers)}
        stats: dict = {}
        for t in tickers:
            try:
                self._ensure(t, final_session(self.now()), stats)
            except MarketDataError as err:
                log.info("market_data prewarm failed", extra={"fields": {"ticker": t, "error": err.category}})
            self.prewarm_state["done"] += 1
        self.prewarm_state["state"] = "done"
        log.info("market_data prewarm", extra={"fields": {"symbols": len(tickers), **stats}})

    def health(self) -> dict:
        return {
            "status": "degraded" if self.breaker.state != "closed" else "ok",
            "provider": {"name": self.provider.name, "label": self.provider.label,
                         "version": getattr(self.provider, "version", "unknown")},
            "circuit": self.breaker.snapshot(),
            "cache": self.cache.stats(),
            "concurrency": {"limit": self.gate.limit, "active": self.gate.active, "peak": self.gate.peak},
            "prewarm": dict(self.prewarm_state),
            "finalSession": final_session(self.now()).isoformat(),
            "uptimeSeconds": round(self.mono() - self.started),
        }
