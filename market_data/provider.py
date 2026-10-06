"""Upstream adapter: Yahoo Finance through yfinance.

This is the only module that knows about Yahoo or yfinance. It returns plain,
provider-neutral records; everything above it (cache, service, routes) would be
unchanged for another upstream.

Adjustment convention (qualified 2026-09-30 against Yahoo's chart API directly):

* `auto_adjust=False`, `back_adjust=False`: keep Yahoo's own `adjclose` series
  (the "Adj Close" column) instead of yfinance's rescaled OHLC. `adjclose` is
  Yahoo's split- AND dividend-adjusted close, the total-return-aware proxy
  Portfolio Lab's direct adapter already uses; ETF distributions are dividends.
* `repair=False`: yfinance's heuristic price/dividend "repairs" rewrite history;
  the value served must be the upstream value, not a reconstruction.
* `actions=True`: dividends/splits are requested so Yahoo computes adjclose from
  them; the event columns themselves are not returned.
* `keepna=False`, `prepost=False`, `rounding=False`, `interval="1d"`.
* An explicit `start`, never `period="max"`: Yahoo answers `range=max` with
  MONTHLY bars even for `interval=1d` (verified: 405 bars vs 8,474 daily).
* Rows with a missing adjusted close are dropped, never filled. No forward fill,
  interpolation or bridging anywhere.
* OTC securities: Yahoo repeats the last price on sessions with no trades. Those
  sessions are reported (`untraded`) so a mostly-stale history can be refused for
  the requested window instead of passing as a near-riskless series.

Yahoo's adjclose is float32-rounded and its adjustment factor is recomputed per
request: identical requests differ by up to ~1e-6 relative. That is an upstream
property; cache comparisons use a tolerance well above it.
"""
import datetime as dt
import math
import warnings

import yfinance as yf

from .errors import MarketDataError
from .sessions import NY

NAME = "yahoo-yfinance"
LABEL = "Yahoo Finance via yfinance (unofficial)"
CONVENTION = "total_return_aware_adjusted"
ADJUSTMENT = {
    "method": "yahoo_adjclose",
    "splits": True,
    "dividends": True,
    "distributions": "treated as dividends",
    "fill": "none",
}
US_EXCHANGES = {"PCX", "NMS", "NGM", "NCM", "NYQ", "ASE", "BTS", "BATS"}
# OTC Markets tiers as Yahoo names them: OTCQX, OTCQB, Pink and OTC ID. Large
# foreign issuers (Nestlé, Roche, Tencent) trade in the U.S. only here.
OTC_EXCHANGES = {"OQX", "OQB", "PNK", "OID"}
EARLIEST = dt.date(1900, 1, 1)

# Per-call `raise_errors=True` is deprecated in favour of a process-wide switch;
# the per-call form keeps error behaviour local to these calls, so only that
# deprecation warning is silenced.
warnings.filterwarnings("ignore", message="'raise_errors' deprecated", category=DeprecationWarning)


def _date(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value, NY).date().isoformat()
    if hasattr(value, "tz_convert"):  # pandas Timestamp
        return (value.tz_convert(NY) if value.tzinfo else value).date().isoformat()
    return None


def _iso(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value, dt.timezone.utc).isoformat().replace("+00:00", "Z")
    if hasattr(value, "tz_convert"):
        ts = value.tz_convert("UTC") if value.tzinfo else value.tz_localize("UTC")
        return ts.to_pydatetime().isoformat().replace("+00:00", "Z")
    return None


def classify(exc: Exception, ticker: str) -> MarketDataError:
    """Map yfinance / HTTP exceptions onto typed failures."""
    if isinstance(exc, MarketDataError):
        return exc
    name = type(exc).__name__
    text = str(exc)
    low = text.lower()
    if name == "YFRateLimitError" or "too many requests" in low or " 429" in text:
        return MarketDataError("RATE_LIMIT", "Market-data provider is rate limiting; try again shortly.", "throttled")
    if name == "YFPricesMissingError" and "doesn't exist for startdate" in low:
        return MarketDataError("INSUFFICIENT_HISTORY",
                               f"{ticker} has no history in the requested range; it may have listed later.",
                               "no_history")
    if name in {"YFTzMissingError", "YFTickerMissingError", "YFPricesMissingError"} or "404" in text \
            or "not found" in low or "delisted" in low:
        return MarketDataError("TICKER_NOT_FOUND", f"No history found for {ticker}.", "not_found")
    if name == "YFInvalidPeriodError":
        return MarketDataError("INVALID_INPUT", "Invalid history range.", "invalid")
    if "timeout" in low or "timed out" in low or name in {"Timeout", "ReadTimeout", "ConnectTimeout"}:
        return MarketDataError("TIMEOUT", "Market-data provider timed out.", "transient")
    if any(s in low for s in ("connection", "reset by peer", "temporarily", "502", "503", "504", "500 ")) \
            or name in {"ConnectionError", "RemoteDisconnected", "ChunkedEncodingError"}:
        return MarketDataError("PROVIDER_ERROR", "Market-data provider connection failed.", "transient")
    return MarketDataError("PROVIDER_ERROR", "Market-data provider request failed.", "provider", retryable=True)


def _meta(ticker: str, meta: dict) -> dict:
    symbol = str(meta.get("symbol") or "").upper()
    out = {
        "symbol": symbol,
        "currency": meta.get("currency"),
        "exchange": meta.get("exchangeName"),
        "instrument": meta.get("instrumentType"),
        "timezone": meta.get("exchangeTimezoneName"),
        "firstTradeDate": _date(meta.get("firstTradeDate")),
    }
    if symbol != ticker:
        raise MarketDataError("MALFORMED_DATA", f"Provider returned {symbol or 'no symbol'} for {ticker}.", "malformed")
    if (out["currency"] != "USD" or out["timezone"] != "America/New_York"
            or out["instrument"] not in {"EQUITY", "ETF"}
            or out["exchange"] not in US_EXCHANGES | OTC_EXCHANGES):
        raise MarketDataError("UNSUPPORTED_ASSET",
                              f"{ticker} is outside the supported USD U.S.-traded equity/ETF universe.",
                              "unsupported")
    return out


class YFinanceProvider:
    name = NAME
    label = LABEL
    convention = CONVENTION
    adjustment = ADJUSTMENT

    def __init__(self, timeout: float = 15):
        self.timeout = timeout
        self.version = getattr(yf, "__version__", "unknown")

    def _history(self, ticker: str, start: dt.date, end_exclusive: dt.date):
        tk = yf.Ticker(ticker)
        frame = tk.history(start=start.isoformat(), end=end_exclusive.isoformat(), interval="1d",
                           auto_adjust=False, back_adjust=False, repair=False, keepna=False,
                           actions=True, prepost=False, rounding=False, timeout=self.timeout,
                           raise_errors=True)
        return frame, tk.history_metadata or {}

    def history(self, ticker: str, start: dt.date | None, end_exclusive: dt.date) -> dict:
        """Daily adjusted closes from `start` (or the full history) up to, not including,
        `end_exclusive`. Returns {"meta": ..., "rows": [(iso_date, adj_close), ...],
        "untraded": [iso_date, ...]}, the last listing the OTC sessions with zero
        volume (always empty for exchange-listed securities)."""
        try:
            frame, meta = self._history(ticker, start or EARLIEST, end_exclusive)
        except Exception as exc:  # noqa: BLE001 - classified below
            raise classify(exc, ticker) from exc
        info = _meta(ticker, meta)
        if frame is None or frame.empty:
            return {"meta": info, "rows": [], "untraded": []}
        if "Adj Close" not in frame.columns:
            raise MarketDataError("MALFORMED_DATA", "Adjusted history is missing; raw close cannot replace it.",
                                  "malformed")
        rows: dict[str, float] = {}
        untraded: set[str] = set()
        otc = info["exchange"] in OTC_EXCHANGES and "Volume" in frame.columns
        volumes = frame["Volume"] if otc else None
        for stamp, value in frame["Adj Close"].items():
            if value is None or (isinstance(value, float) and math.isnan(value)):
                continue  # missing adjusted close: dropped, never filled
            price = float(value)
            if not math.isfinite(price) or price <= 0:
                raise MarketDataError("MALFORMED_DATA", f"Invalid adjusted price for {ticker}.", "malformed")
            day = stamp.tz_convert(NY).date().isoformat() if stamp.tzinfo else stamp.date().isoformat()
            if day in rows and rows[day] != price:
                raise MarketDataError("MALFORMED_DATA", f"Conflicting duplicate prices for {ticker}.", "malformed")
            rows[day] = price
            if otc and volumes[stamp] == 0:
                untraded.add(day)
        return {"meta": info, "rows": sorted(rows.items()), "untraded": sorted(untraded)}

    def quote(self, ticker: str, today: dt.date) -> dict:
        """Latest regular-market price and the recent raw (unadjusted) daily closes."""
        try:
            frame, meta = self._history(ticker, today - dt.timedelta(days=10), today + dt.timedelta(days=1))
        except Exception as exc:  # noqa: BLE001
            raise classify(exc, ticker) from exc
        info = _meta(ticker, meta)
        closes = []
        if frame is not None and not frame.empty and "Close" in frame.columns:
            for stamp, value in frame["Close"].items():
                if value is None or (isinstance(value, float) and math.isnan(value)) or float(value) <= 0:
                    continue
                closes.append({"date": stamp.tz_convert(NY).date().isoformat(), "close": float(value)})
        price = meta.get("regularMarketPrice")
        return {
            "meta": info,
            "regularMarketPrice": float(price) if isinstance(price, (int, float)) and price > 0 else None,
            "regularMarketTime": _iso(meta.get("regularMarketTime")),
            "recentCloses": closes,
        }
