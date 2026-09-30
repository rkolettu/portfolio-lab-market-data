"""Market-data routes, mounted at /api/market-data.

Server-to-server only. Every route requires the shared secret in
`X-Portfolio-Lab-Service-Key`; without PORTFOLIO_LAB_SERVICE_KEY configured the
routes fail closed (503). The key is compared in constant time and never logged.
"""
import datetime as dt
import gzip
import hmac
import json
import logging
import os
import re
import sys
import time
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import Response
from starlette.concurrency import run_in_threadpool

from .errors import MarketDataError, invalid
from .sessions import now_ny
from .service import COMMON

router = APIRouter(prefix="/api/market-data")
SYMBOL = re.compile(r"^[A-Z][A-Z0-9]{0,9}(?:-[A-Z])?$")
MAX_HISTORY = 21  # 20 risky holdings + one benchmark
MAX_QUOTES = 22
MAX_BODY = 8192
LOOKBACK_YEARS = 51  # Portfolio Lab caps MAX at 50 years, plus its 10-day lead-in
KEY_HEADER = "x-portfolio-lab-service-key"

log = logging.getLogger("market_data")


class _JsonFormatter(logging.Formatter):
    def format(self, record):
        body = {"ts": dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds"),
                "event": record.getMessage(), **getattr(record, "fields", {})}
        return json.dumps(body, separators=(",", ":"))


if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(_JsonFormatter())
    log.addHandler(_h)
    log.setLevel(logging.INFO)
    log.propagate = False

_service = None


def get_service():
    global _service
    if _service is None:
        from .provider import YFinanceProvider
        from .service import MarketDataService
        _service = MarketDataService(YFinanceProvider())
    return _service


def set_service(service):  # tests
    global _service
    _service = service


def configured_key() -> str | None:
    key = os.environ.get("PORTFOLIO_LAB_SERVICE_KEY", "")
    return key if len(key) >= 32 else None


def _json(body: dict, request: Request, status: int = 200) -> Response:
    data = json.dumps(body, separators=(",", ":"), allow_nan=False).encode()
    headers = {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}
    if len(data) > 1024 and "gzip" in request.headers.get("accept-encoding", ""):
        data = gzip.compress(data, compresslevel=5)
        headers["Content-Encoding"] = "gzip"
        headers["Vary"] = "Accept-Encoding"
    return Response(data, status_code=status, media_type="application/json", headers=headers)


def _error(request, status, code, message, ctx):
    ctx["status"] = status
    ctx["error"] = code
    return _json({"ok": False, "error": {"code": code, "message": message, "retryable": status >= 500}},
                 request, status)


async def _guard(request: Request, ctx: dict):
    """Auth, then a bounded JSON body. Returns (body | None, error response | None)."""
    expected = configured_key()
    if expected is None:
        return None, _error(request, 503, "UNQUALIFIED_PROVIDER", "Market-data service is not configured.", ctx)
    if not hmac.compare_digest(request.headers.get(KEY_HEADER, "").encode(), expected.encode()):
        return None, _error(request, 401, "PERMISSION", "Missing or invalid service key.", ctx)
    if request.method == "GET":
        return {}, None
    if int(request.headers.get("content-length") or 0) > MAX_BODY:
        return None, _error(request, 413, "INVALID_INPUT", "Request body exceeds the input limit.", ctx)
    raw = await request.body()
    if len(raw) > MAX_BODY:
        return None, _error(request, 413, "INVALID_INPUT", "Request body exceeds the input limit.", ctx)
    try:
        body = json.loads(raw or b"{}")
    except ValueError:
        return None, _error(request, 400, "INVALID_INPUT", "Request must be valid JSON.", ctx)
    if not isinstance(body, dict):
        return None, _error(request, 400, "INVALID_INPUT", "Request must be a JSON object.", ctx)
    return body, None


def _tickers(value, limit: int) -> list[str]:
    if not isinstance(value, list) or not value:
        raise invalid("tickers must be a non-empty list.")
    if len(value) > limit:
        raise invalid(f"At most {limit} symbols per request.")
    out = []
    for t in value:
        if not isinstance(t, str) or not SYMBOL.match(t.strip().upper()):
            raise invalid("Enter U.S.-listed tickers, for example SPY or BRK-B.")
        out.append(t.strip().upper())
    if len(set(out)) != len(out):
        raise invalid("Duplicate symbols are not allowed.")
    return out


def _date(value, name: str) -> dt.date:
    if isinstance(value, str) and len(value) == 10:
        try:
            return dt.date.fromisoformat(value)
        except ValueError:
            pass
    raise invalid(f"{name} must be a YYYY-MM-DD date.")


def _finish(ctx: dict, stats: dict, started: float):
    ctx.update({k: v for k, v in stats.items() if k != "cache"})
    ctx["cache"] = stats.get("cache", {})
    ctx["ms"] = round((time.monotonic() - started) * 1000)
    service = _service
    if service is not None:
        ctx["circuit"] = service.breaker.state
    log.info("market_data request", extra={"fields": ctx})


def _context(request: Request, route: str) -> dict:
    rid = request.headers.get("x-request-id", "")
    return {"requestId": rid if re.fullmatch(r"[A-Za-z0-9-]{8,64}", rid) else uuid.uuid4().hex[:12],
            "route": route, "provider": "yahoo-yfinance"}


@router.post("/history")
async def history(request: Request):
    ctx, stats, started = _context(request, "history"), {}, time.monotonic()
    try:
        body, err = await _guard(request, ctx)
        if err:
            return err
        tickers = _tickers(body.get("tickers"), MAX_HISTORY)
        start, end = _date(body.get("startDate"), "startDate"), _date(body.get("endDate"), "endDate")
        today = now_ny().date()
        if start > end:
            raise invalid("startDate must precede endDate.")
        if end > today:
            raise invalid("endDate cannot be in the future.")
        if start < today - dt.timedelta(days=round(365.25 * LOOKBACK_YEARS)):
            raise invalid(f"startDate exceeds the {LOOKBACK_YEARS}-year lookback limit.")
        ctx["symbols"] = len(tickers)
        service = get_service()
        results = await run_in_threadpool(service.history_batch, tickers, start, end, stats)
        ctx["status"] = 200
        return _json({"ok": True, "requestId": ctx["requestId"],
                      "provider": {"name": service.provider.name, "label": service.provider.label,
                                   "convention": service.provider.convention,
                                   "adjustment": service.provider.adjustment,
                                   "version": getattr(service.provider, "version", "unknown")},
                      "results": results}, request)
    except MarketDataError as e:
        return _error(request, 400, e.code, e.message, ctx)
    finally:
        _finish(ctx, stats, started)


@router.post("/quote")
async def quote(request: Request):
    ctx, stats, started = _context(request, "quote"), {}, time.monotonic()
    try:
        body, err = await _guard(request, ctx)
        if err:
            return err
        tickers = _tickers(body.get("tickers"), MAX_QUOTES)
        ctx["symbols"] = len(tickers)
        service = get_service()
        results = await run_in_threadpool(service.quote_batch, tickers, stats)
        ctx["status"] = 200
        return _json({"ok": True, "requestId": ctx["requestId"],
                      "provider": {"name": service.provider.name, "label": service.provider.label},
                      "results": results}, request)
    except MarketDataError as e:
        return _error(request, 400, e.code, e.message, ctx)
    finally:
        _finish(ctx, stats, started)


@router.post("/refresh")
async def refresh(request: Request):
    ctx, stats, started = _context(request, "refresh"), {}, time.monotonic()
    try:
        body, err = await _guard(request, ctx)
        if err:
            return err
        tickers = _tickers(body.get("tickers") or COMMON, MAX_HISTORY)
        ctx["symbols"] = len(tickers)
        service = get_service()
        out = await run_in_threadpool(service.refresh, tickers, stats)
        ctx["status"] = 200
        return _json({"ok": True, "requestId": ctx["requestId"], "results": out}, request)
    except MarketDataError as e:
        return _error(request, 400, e.code, e.message, ctx)
    finally:
        _finish(ctx, stats, started)


@router.get("/health")
async def health(request: Request):
    ctx, started = _context(request, "health"), time.monotonic()
    try:
        _, err = await _guard(request, ctx)
        if err:
            return err
        ctx["status"] = 200
        return _json({"ok": True, **get_service().health()}, request)
    finally:
        _finish(ctx, {}, started)


def start_prewarm():
    """Background prewarm when the service is configured; never blocks startup."""
    if configured_key() is None or os.environ.get("MARKET_DATA_PREWARM", "1") == "0":
        return
    import threading
    tickers = [t.strip().upper() for t in os.environ.get("MARKET_DATA_PREWARM_TICKERS", "").split(",") if t.strip()]
    service = get_service()
    threading.Thread(target=service.prewarm, args=(tickers or COMMON,), daemon=True,
                     name="market-data-prewarm").start()
