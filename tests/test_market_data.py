"""Portfolio Lab Market Data API. Upstream Yahoo is always faked: these tests
never touch the network, so CI does not depend on Yahoo availability."""
import datetime as dt
import io
import json
import logging
import threading
import time

import numpy as np
import pandas as pd
import pytest

from market_data import provider as provider_mod
from market_data import routes
from market_data.errors import MarketDataError
from market_data.resilience import CircuitBreaker, RetryPolicy, UpstreamGate
from market_data.service import MarketDataService, TOLERANCE
from market_data.sessions import NY

KEY = "k" * 40
FRI, MON = dt.date(2026, 9, 25), dt.date(2026, 9, 28)


def weekdays(start: dt.date, end: dt.date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += dt.timedelta(days=1)


def series(start: dt.date, end: dt.date, base=100.0):
    return [(d.isoformat(), base + i * 0.25) for i, d in enumerate(weekdays(start, end))]


def meta(ticker, **over):
    return {"symbol": ticker, "currency": "USD", "exchange": "PCX", "instrument": "ETF",
            "timezone": "America/New_York", "firstTradeDate": "1993-01-29", **over}


class FakeProvider:
    name, label, convention, version = "fake", "Fake upstream", "total_return_aware_adjusted", "test"
    adjustment = {"method": "fake"}

    def __init__(self):
        self.series, self.meta, self.fail, self.calls = {}, {}, {}, []
        self.delay, self.active, self.peak = 0.0, 0, 0
        self.lock = threading.Lock()

    def add(self, ticker, rows, **m):
        self.series[ticker] = rows
        self.meta[ticker] = meta(ticker, **m)

    def _enter(self, kind, *args):
        with self.lock:
            self.calls.append((kind, *args))
            self.active += 1
            self.peak = max(self.peak, self.active)

    def _leave(self):
        with self.lock:
            self.active -= 1

    def _maybe_fail(self, ticker):
        queue = self.fail.get(ticker)
        if queue:
            err = queue.pop(0)
            if err is not None:
                raise err
        if ticker not in self.series:
            raise MarketDataError("TICKER_NOT_FOUND", f"No history found for {ticker}.", "not_found")

    def history(self, ticker, start, end_exclusive):
        self._enter("history", ticker, start, end_exclusive)
        try:
            if self.delay:
                time.sleep(self.delay)
            self._maybe_fail(ticker)
            rows = [(d, p) for d, p in self.series[ticker]
                    if (start is None or d >= start.isoformat()) and d < end_exclusive.isoformat()]
            return {"meta": self.meta[ticker], "rows": rows}
        finally:
            self._leave()

    def quote(self, ticker, today):
        self._enter("quote", ticker)
        try:
            self._maybe_fail(ticker)
            last = self.series[ticker][-1]
            return {"meta": self.meta[ticker], "regularMarketPrice": last[1] + 1,
                    "regularMarketTime": "2026-09-28T19:59:00Z",
                    "recentCloses": [{"date": d, "close": p} for d, p in self.series[ticker][-3:]]}
        finally:
            self._leave()

    def history_calls(self, ticker=None):
        return [c for c in self.calls if c[0] == "history" and (ticker is None or c[1] == ticker)]


class Clock:
    def __init__(self, when):
        self.when = when
        self.mono_t = 1000.0

    def now(self):
        return self.when

    def mono(self):
        return self.mono_t

    def wall(self):
        return self.when.timestamp()

    def at(self, day, hh, mm=0):
        self.when = dt.datetime(day.year, day.month, day.day, hh, mm, tzinfo=NY)


def make(fake, clock, *, limit=4, threshold=3, attempts=3):
    return MarketDataService(
        fake, breaker=CircuitBreaker(threshold=threshold, cooldown=30, clock=clock.mono),
        retry=RetryPolicy(attempts=attempts, sleep=lambda s: None),
        gate=UpstreamGate(limit=limit, queue_timeout=5), now=clock.now, wall=clock.wall, mono=clock.mono)


@pytest.fixture
def world():
    fake = FakeProvider()
    fake.add("SPY", series(dt.date(2020, 1, 1), FRI))
    fake.add("QQQ", series(dt.date(2020, 1, 1), FRI, 200))
    clock = Clock(dt.datetime(2026, 9, 25, 18, 0, tzinfo=NY))  # Friday after the close
    return fake, clock, make(fake, clock)


# ---- service ----------------------------------------------------------------

def test_full_history_cached_once_and_sliced(world):
    fake, clock, svc = world
    stats = {}
    a = svc.history("SPY", dt.date(2026, 9, 1), FRI, stats)
    b = svc.history("SPY", dt.date(2021, 1, 4), dt.date(2021, 1, 8), stats)
    assert fake.history_calls("SPY") == [("history", "SPY", None, dt.date(2026, 9, 26))]  # one full fetch
    assert a["dates"][0] == "2026-09-01" and a["dates"][-1] == FRI.isoformat()
    assert b["dates"] == ["2021-01-04", "2021-01-05", "2021-01-06", "2021-01-07", "2021-01-08"]
    assert stats["cache"] == {"miss": 1, "hit": 1}
    assert len(set(a["dates"])) == len(a["dates"])


def test_session_in_progress_is_never_cached_or_served(world):
    fake, clock, svc = world
    fake.series["SPY"].append((MON.isoformat(), 999.0))  # Monday's bar while Monday trades
    clock.at(MON, 11)
    out = svc.history("SPY", dt.date(2026, 9, 20), MON, {})
    assert out["dates"][-1] == FRI.isoformat()
    assert out["provenance"]["checkedThrough"] == FRI.isoformat()


def test_incremental_refresh_fetches_only_the_tail(world):
    fake, clock, svc = world
    svc.history("SPY", dt.date(2026, 1, 2), FRI, {})
    fake.series["SPY"].append((MON.isoformat(), 777.0))
    clock.at(MON, 17)
    stats = {}
    out = svc.history("SPY", dt.date(2026, 9, 1), MON, stats)
    tail = fake.history_calls("SPY")[-1]
    assert tail[2] == FRI - dt.timedelta(days=14)  # tail plus overlap, not the full history
    assert out["dates"][-2:] == [FRI.isoformat(), MON.isoformat()]
    assert out["adjustedClose"][-1] == 777.0
    assert stats["incremental"] == 1 and "full" not in stats
    assert len(set(out["dates"])) == len(out["dates"])


def test_overlap_noise_within_tolerance_is_not_a_correction(world):
    fake, clock, svc = world
    svc.history("SPY", dt.date(2026, 1, 2), FRI, {})
    before = svc.history("SPY", dt.date(2026, 9, 14), FRI, {})["adjustedClose"]
    # Yahoo re-derives adjclose per request (float32): tiny noise, not a correction.
    fake.series["SPY"] = [(d, p * (1 + TOLERANCE / 5)) for d, p in fake.series["SPY"]] + [(MON.isoformat(), 500.0)]
    clock.at(MON, 17)
    stats = {}
    after = svc.history("SPY", dt.date(2026, 9, 14), MON, stats)
    assert "corrections" not in stats and stats["incremental"] == 1
    assert after["adjustedClose"][:-1] == before  # cached values win on overlap: deterministic


def test_dividend_rebasing_triggers_a_full_refetch(world):
    fake, clock, svc = world
    svc.history("SPY", dt.date(2026, 1, 2), FRI, {})
    # A new dividend rescales every earlier adjusted close; never splice across it.
    fake.series["SPY"] = [(d, p * 0.995) for d, p in fake.series["SPY"]] + [(MON.isoformat(), 500.0)]
    clock.at(MON, 17)
    stats = {}
    out = svc.history("SPY", dt.date(2026, 9, 14), MON, stats)
    assert stats["corrections"] == 1 and stats["full"] == 1
    assert fake.history_calls("SPY")[-1][2] is None  # full history again
    assert out["adjustedClose"][0] == pytest.approx(dict(fake.series["SPY"])["2026-09-14"])


def test_single_flight_coalesces_concurrent_requests(world):
    fake, clock, svc = world
    fake.delay = 0.15
    barrier = threading.Barrier(6)
    results = []

    def go():
        barrier.wait()
        results.append(svc.history("SPY", dt.date(2026, 9, 1), FRI, {}))
    threads = [threading.Thread(target=go) for _ in range(6)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(fake.history_calls("SPY")) == 1
    assert len(results) == 6 and all(r["dates"] == results[0]["dates"] for r in results)


def test_transient_errors_retry_with_backoff_but_missing_tickers_do_not(world):
    fake, clock, svc = world
    fake.fail["SPY"] = [MarketDataError("TIMEOUT", "t", "transient"), MarketDataError("PROVIDER_ERROR", "r", "transient")]
    stats = {}
    out = svc.history("SPY", dt.date(2026, 9, 1), FRI, stats)
    assert out["ok"] and len(fake.history_calls("SPY")) == 3 and stats["retries"] == 2
    with pytest.raises(MarketDataError) as e:
        svc.history("ZZZZ", dt.date(2026, 9, 1), FRI, {})
    assert e.value.code == "TICKER_NOT_FOUND" and len(fake.history_calls("ZZZZ")) == 1
    with pytest.raises(MarketDataError):  # negative cache: no second upstream call
        svc.history("ZZZZ", dt.date(2026, 9, 1), FRI, {})
    assert len(fake.history_calls("ZZZZ")) == 1


def test_backoff_grows_with_jitter_and_is_capped():
    policy = RetryPolicy(base=0.5, cap=4.0, rand=lambda: 1.0)
    assert [policy.delay(n, False) for n in range(5)] == [0.5, 1.0, 2.0, 4.0, 4.0]
    assert RetryPolicy(base=0.5, cap=4.0, rand=lambda: 0.0).delay(1, False) == 0.5
    assert policy.delay(0, True) == 2.0  # throttling backs off harder


def test_circuit_opens_serves_cache_then_recovers(world):
    fake, clock, svc = world
    svc = make(fake, clock, threshold=3, attempts=1)
    svc.history("SPY", dt.date(2026, 1, 2), FRI, {})  # cached before the outage
    for t in ["A", "B", "C"]:
        fake.add(t, series(dt.date(2026, 1, 1), FRI))
        fake.fail[t] = [MarketDataError("TIMEOUT", "t", "transient")]
        with pytest.raises(MarketDataError):
            svc.history(t, dt.date(2026, 9, 1), FRI, {})
    assert svc.breaker.state == "open"
    calls = len(fake.calls)
    fake.add("D", series(dt.date(2026, 1, 1), FRI))
    with pytest.raises(MarketDataError) as e:  # uncached: typed, and no upstream traffic
        svc.history("D", dt.date(2026, 9, 1), FRI, {})
    assert "temporarily unavailable" in e.value.message and e.value.retryable
    fake.series["SPY"].append((MON.isoformat(), 1.0))
    clock.at(MON, 17)
    stale = svc.history("SPY", dt.date(2026, 9, 1), MON, {})  # cached history still served
    assert stale["provenance"]["stale"] and stale["dates"][-1] == FRI.isoformat()
    assert len(fake.calls) == calls
    clock.mono_t += 31  # cooldown passes: one half-open probe, which succeeds
    fresh = svc.history("SPY", dt.date(2026, 9, 1), MON, {})
    assert svc.breaker.state == "closed" and fresh["dates"][-1] == MON.isoformat()


def test_failed_half_open_probe_reopens_with_longer_cooldown():
    clock = Clock(dt.datetime(2026, 9, 25, 18, tzinfo=NY))
    b = CircuitBreaker(threshold=1, cooldown=10, clock=clock.mono)
    b.failure()
    assert not b.allow()
    clock.mono_t += 11
    assert b.allow() and not b.allow()  # exactly one probe
    b.failure()
    assert b.state == "open" and b.cooldown == 20


def test_concurrency_is_bounded(world):
    fake, clock, _ = world
    svc = make(fake, clock, limit=2)
    fake.delay = 0.05
    tickers = [f"T{i}" for i in range(8)]
    for t in tickers:
        fake.add(t, series(dt.date(2026, 1, 1), FRI))
    out = svc.history_batch(tickers, dt.date(2026, 9, 1), FRI, {})
    assert all(r["ok"] for r in out) and fake.peak <= 2 and svc.gate.peak <= 2


def test_pre_listing_and_partial_history(world):
    fake, clock, svc = world
    fake.add("NEW", series(dt.date(2024, 6, 3), FRI), firstTradeDate="2024-06-03")
    with pytest.raises(MarketDataError) as e:
        svc.history("NEW", dt.date(2020, 1, 1), dt.date(2023, 12, 29), {})
    assert e.value.code == "INSUFFICIENT_HISTORY"
    out = svc.history("NEW", dt.date(2024, 1, 2), dt.date(2024, 6, 7), {})
    assert out["dates"][0] == "2024-06-03" and out["firstTradeDate"] == "2024-06-03"


def test_batch_keeps_failures_typed_and_in_order(world):
    fake, clock, svc = world
    out = svc.history_batch(["QQQ", "ZZZZ", "SPY"], dt.date(2026, 9, 1), FRI, {})
    assert [r["ticker"] for r in out] == ["QQQ", "ZZZZ", "SPY"]
    assert out[1] == {"ok": False, "ticker": "ZZZZ", "error": {
        "code": "TICKER_NOT_FOUND", "message": "No history found for ZZZZ.", "retryable": False, "ticker": "ZZZZ"}}
    assert out[0]["ok"] and out[2]["ok"]


def test_quotes_are_cached_briefly_and_failures_are_isolated(world):
    fake, clock, svc = world
    q = svc.quote_batch(["SPY", "ZZZZ"], {})
    assert q[0]["ok"] and q[0]["retrievedAt"] and q[0]["regularMarketTime"] == "2026-09-28T19:59:00Z"
    assert q[0]["recentCloses"][-1] == {"date": FRI.isoformat(), "close": fake.series["SPY"][-1][1]}
    assert not q[1]["ok"] and q[1]["error"]["code"] == "TICKER_NOT_FOUND"
    svc.quote_batch(["SPY"], {})
    assert len([c for c in fake.calls if c[0] == "quote" and c[1] == "SPY"]) == 1
    clock.mono_t += 61
    svc.quote_batch(["SPY"], {})
    assert len([c for c in fake.calls if c[0] == "quote" and c[1] == "SPY"]) == 2
    assert svc.history("SPY", dt.date(2026, 9, 1), FRI, {})["ok"]  # history unaffected


def test_cache_eviction_is_bounded():
    from market_data.cache import Entry, HistoryCache, build
    cache = HistoryCache(max_rows=10, max_entries=5)
    for i in range(4):
        dates, closes = build([(f"2026-01-0{d}", 1.0) for d in range(1, 5)])
        cache.put(i, Entry({}, dates, closes, "", "", 0.0, dt.date(2026, 1, 4)))
    assert cache.stats()["rows"] <= 10 and cache.get(0) is None and cache.get(3) is not None


def test_refresh_updates_common_symbols_incrementally(world):
    fake, clock, svc = world
    svc.history("SPY", dt.date(2026, 1, 2), FRI, {})
    fake.series["SPY"].append((MON.isoformat(), 5.0))
    clock.at(MON, 17)
    out = svc.refresh(["SPY"], {})
    assert out == {"SPY": "refreshed"} and fake.history_calls("SPY")[-1][2] is not None


# ---- real provider normalization (yfinance faked) ------------------------------

def frame(rows, **cols):
    index = pd.DatetimeIndex([pd.Timestamp(d).tz_localize(NY) for d, _ in rows])
    return pd.DataFrame({"Adj Close": [p for _, p in rows], "Close": [p for _, p in rows], **cols}, index=index)


def yf_meta(ticker="SPY", **over):
    return {"symbol": ticker, "currency": "USD", "exchangeName": "PCX", "instrumentType": "ETF",
            "exchangeTimezoneName": "America/New_York",
            "firstTradeDate": pd.Timestamp("1993-01-29 09:30", tz=NY), **over}


def test_provider_uses_adjclose_explicitly_and_never_fills(monkeypatch):
    calls = {}

    class T:
        def __init__(self, ticker):
            self.history_metadata = yf_meta(ticker)

        def history(self, **kw):
            calls.update(kw)
            return frame([("2026-09-22", 100.0), ("2026-09-23", np.nan), ("2026-09-24", 102.0)])
    monkeypatch.setattr(provider_mod.yf, "Ticker", T)
    out = provider_mod.YFinanceProvider().history("SPY", None, dt.date(2026, 9, 26))
    assert out["rows"] == [("2026-09-22", 100.0), ("2026-09-24", 102.0)]  # gap kept, not filled
    assert out["meta"]["firstTradeDate"] == "1993-01-29"
    assert calls["auto_adjust"] is False and calls["back_adjust"] is False and calls["repair"] is False
    assert calls["interval"] == "1d" and calls["start"] == "1900-01-01" and "period" not in calls
    assert calls["raise_errors"] is True and calls["keepna"] is False


def test_provider_rejects_non_us_assets_and_symbol_mismatch(monkeypatch):
    for m, code in [({"currency": "EUR"}, "UNSUPPORTED_ASSET"), ({"instrumentType": "MUTUALFUND"}, "UNSUPPORTED_ASSET"),
                    ({"exchangeName": "LSE"}, "UNSUPPORTED_ASSET"), ({"symbol": "SPYX"}, "MALFORMED_DATA")]:
        class T:
            def __init__(self, ticker, m=m):
                self.history_metadata = yf_meta(ticker, **m)

            def history(self, **kw):
                return frame([("2026-09-22", 100.0)])
        monkeypatch.setattr(provider_mod.yf, "Ticker", T)
        with pytest.raises(MarketDataError) as e:
            provider_mod.YFinanceProvider().history("SPY", None, dt.date(2026, 9, 26))
        assert e.value.code == code


def test_exception_classification():
    def err(name, text):
        return type(name, (Exception,), {})(text)
    c = provider_mod.classify
    assert c(err("YFRateLimitError", "Too Many Requests"), "SPY").category == "throttled"
    assert c(err("YFPricesMissingError", "Data doesn't exist for startDate = 1"), "SPY").code == "INSUFFICIENT_HISTORY"
    assert c(err("YFTzMissingError", "possibly delisted; no timezone found"), "X").code == "TICKER_NOT_FOUND"
    assert c(err("Timeout", "Operation timed out after 15000 ms"), "SPY").category == "transient"
    assert c(err("ConnectionError", "Connection reset by peer"), "SPY").category == "transient"
    assert c(err("HTTPError", "HTTP Error 503: Service Unavailable"), "SPY").category == "transient"
    assert c(err("ValueError", "something else"), "SPY").category == "provider"


# ---- routes -------------------------------------------------------------------

@pytest.fixture
def client(monkeypatch, world):
    monkeypatch.setenv("MARKET_DATA_PREWARM", "0")
    import app as backend
    from fastapi.testclient import TestClient
    fake, clock, svc = world
    monkeypatch.setattr(routes, "_service", svc)
    monkeypatch.setenv("PORTFOLIO_LAB_SERVICE_KEY", KEY)
    return TestClient(backend.app), fake


def post(client, path, body, key=KEY, **headers):
    h = {"X-Portfolio-Lab-Service-Key": key, **headers} if key else headers
    return client.post(f"/api/market-data/{path}", content=json.dumps(body), headers=h)


def test_routes_require_the_service_key(client, monkeypatch):
    c, _ = client
    body = {"tickers": ["SPY"], "startDate": "2026-09-01", "endDate": "2026-09-25"}
    assert post(c, "history", body, key=None).status_code == 401
    assert post(c, "history", body, key="wrong" * 10).status_code == 401
    assert c.get("/api/market-data/health").status_code == 401
    assert post(c, "history", body).status_code == 200
    monkeypatch.setenv("PORTFOLIO_LAB_SERVICE_KEY", "short")  # misconfigured: fail closed
    assert post(c, "history", body, key="short").status_code == 503
    assert c.get("/healthz").json() == {"status": "ok"}  # platform liveness: public, reveals nothing


def test_history_route_batch_contract(client):
    c, _ = client
    r = post(c, "history", {"tickers": ["SPY", "zzzz", "QQQ"], "startDate": "2026-09-21", "endDate": "2026-09-25"},
             **{"Accept-Encoding": "gzip", "X-Request-Id": "req-12345678"})
    assert r.status_code == 200 and r.headers["content-encoding"] == "gzip"
    body = r.json()
    assert body["requestId"] == "req-12345678"
    assert body["provider"]["convention"] == "total_return_aware_adjusted"
    assert [x["ticker"] for x in body["results"]] == ["SPY", "ZZZZ", "QQQ"]
    assert body["results"][0]["dates"] == ["2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25"]
    assert body["results"][1]["error"]["code"] == "TICKER_NOT_FOUND"


@pytest.mark.parametrize("body,fragment", [
    ({"tickers": ["SPY", "SPY"], "startDate": "2026-09-01", "endDate": "2026-09-25"}, "Duplicate"),
    ({"tickers": [f"T{i}" for i in range(22)], "startDate": "2026-09-01", "endDate": "2026-09-25"}, "At most 21"),
    ({"tickers": ["BAD.L"], "startDate": "2026-09-01", "endDate": "2026-09-25"}, "U.S.-listed"),
    ({"tickers": ["SPY"], "startDate": "2026-13-01", "endDate": "2026-09-25"}, "YYYY-MM-DD"),
    ({"tickers": ["SPY"], "startDate": "2026-09-25", "endDate": "2026-09-01"}, "precede"),
    ({"tickers": ["SPY"], "startDate": "2026-09-01", "endDate": "2099-01-01"}, "future"),
    ({"tickers": ["SPY"], "startDate": "1901-01-01", "endDate": "2026-09-25"}, "lookback"),
    ({"tickers": []}, "non-empty"),
])
def test_history_route_validates_input(client, body, fragment):
    c, _ = client
    r = post(c, "history", body)
    assert r.status_code == 400 and fragment in r.json()["error"]["message"]


def test_body_limit_and_json(client):
    c, _ = client
    assert post(c, "history", {"tickers": ["SPY"], "pad": "x" * 9000}).status_code == 413
    r = c.post("/api/market-data/history", content=b"not json", headers={"X-Portfolio-Lab-Service-Key": KEY})
    assert r.status_code == 400


def test_maximum_portfolio_is_one_request(client):
    c, fake = client
    tickers = [f"T{i}" for i in range(21)]
    for t in tickers:
        fake.add(t, series(dt.date(2026, 1, 1), FRI))
    r = post(c, "history", {"tickers": tickers, "startDate": "2026-09-01", "endDate": "2026-09-25"})
    assert r.status_code == 200 and all(x["ok"] for x in r.json()["results"])


def test_quote_route_and_health(client):
    c, _ = client
    q = post(c, "quote", {"tickers": ["SPY", "ZZZZ"]}).json()
    assert q["results"][0]["ok"] and q["results"][1]["error"]["code"] == "TICKER_NOT_FOUND"
    h = c.get("/api/market-data/health", headers={"X-Portfolio-Lab-Service-Key": KEY}).json()
    assert h["status"] == "ok" and h["circuit"]["state"] == "closed" and "cache" in h
    r = post(c, "refresh", {})
    assert r.status_code == 200 and set(r.json()["results"]) == {"SPY", "QQQ", "IWM", "BND", "GLD"}


def test_request_logs_carry_metrics_but_never_the_key_or_prices(client):
    c, _ = client
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(routes._JsonFormatter())
    routes.log.addHandler(handler)
    try:
        post(c, "history", {"tickers": ["SPY"], "startDate": "2026-09-01", "endDate": "2026-09-25"})
    finally:
        routes.log.removeHandler(handler)
    line = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert line["route"] == "history" and line["symbols"] == 1 and line["status"] == 200
    assert {"requestId", "cache", "circuit", "ms", "provider"} <= set(line)
    assert KEY not in buf.getvalue() and "adjustedClose" not in buf.getvalue()
