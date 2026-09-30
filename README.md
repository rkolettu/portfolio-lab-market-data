# Portfolio Lab Market Data API

Standalone market-data backend for
[Portfolio Intelligence & Construction Lab](https://github.com/rkolettu/portfolio-intelligence-lab).

```
Browser → Portfolio Lab (Vercel API routes) → this service (Render, shared secret) → yfinance → Yahoo
```

The browser never calls this service. It **retrieves, normalizes, validates, caches
and reports provenance** for daily adjusted histories and current quote
observations. Every portfolio calculation (returns, Sharpe, beta, covariance, risk
contribution, stress, optimization) stays in Portfolio Lab's TypeScript engine.

```sh
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/python -m pytest tests -q
PORTFOLIO_LAB_SERVICE_KEY=$(python3 -c "import secrets;print(secrets.token_hex(32))") \
  .venv/bin/uvicorn app:app --port 8765
```

`GET /healthz` is a public liveness check for the platform (returns only
`{"status":"ok"}`); everything under `/api/market-data` needs the key.

## Deploy on Render (free)

Use the Blueprint (`render.yaml`) or create a Web Service with: runtime Python,
build `pip install -r requirements.txt`, start
`uvicorn app:app --host 0.0.0.0 --port $PORT --proxy-headers --no-server-header`,
health check `/healthz`, plan Free, env `PYTHON_VERSION=3.12.8` and
`PORTFOLIO_LAB_SERVICE_KEY` (secret).

## Routes

All routes require `X-Portfolio-Lab-Service-Key: <PORTFOLIO_LAB_SERVICE_KEY>`.
Without the key configured on the server they return 503 (fail closed); a missing
or wrong key returns 401. Responses are `no-store` JSON, gzip-compressed when the
client accepts it.

| Route | Body | Returns |
|---|---|---|
| `POST /api/market-data/history` | `{"tickers": [...≤21], "startDate": "YYYY-MM-DD", "endDate": "YYYY-MM-DD"}` | Per ticker, in request order: normalized daily adjusted closes with provenance, or a typed failure |
| `POST /api/market-data/quote` | `{"tickers": [...≤22]}` | Per ticker: latest regular-market print, its timestamp, recent raw closes, `retrievedAt` |
| `POST /api/market-data/refresh` | `{"tickers"?: [...]}` (default: SPY QQQ IWM BND GLD) | Incremental refresh result per symbol |
| `GET /api/market-data/health` | — | Provider, circuit state, cache size, concurrency, prewarm state |

Validation: at most 21 history symbols (20 holdings + benchmark), no duplicates,
U.S. ticker syntax (`SPY`, `BRK-B`), ISO dates, start ≤ end, end not in the
future, start within 51 years, request body ≤ 8 KiB.

A failure for one symbol never removes it: it comes back as
`{"ok": false, "ticker": "...", "error": {"code", "message", "retryable"}}` with
Portfolio Lab's error codes (`TICKER_NOT_FOUND`, `INSUFFICIENT_HISTORY`,
`UNSUPPORTED_ASSET`, `RATE_LIMIT`, `TIMEOUT`, `PROVIDER_ERROR`, `MALFORMED_DATA`).

## Adjustment convention

`yfinance` is called explicitly, never with defaults: `interval="1d"`,
`auto_adjust=False`, `back_adjust=False`, `repair=False`, `actions=True`,
`keepna=False`, `prepost=False`, `rounding=False`, and an explicit `start`. The value
served is Yahoo's own `adjclose` (the "Adj Close" column): split- **and**
dividend-adjusted, ETF distributions included as dividends. This is the same
series Portfolio Lab's direct research adapter reads.

- `period="max"` / `range=max` is never used: Yahoo answers it with **monthly**
  bars even for `interval=1d` (verified: 405 bars instead of 8,474 daily).
- Missing adjusted closes are dropped, never filled: no forward fill,
  interpolation or bridging.
- Only final sessions are cached or served: a weekday's bar counts once it is past
  16:20 New York time.
- Yahoo re-derives `adjclose` per request in float32, so identical requests
  differ by up to ~1e-6 relative. Comparisons use a tolerance of 5e-6, well
  above that noise and below the smallest real adjustment.

## Cache and refresh

- One entry per `(provider, ticker, interval, convention)` holding the complete
  known history (4-byte date + 8-byte price per session). Any requested range is a
  slice of that entry, not its own cache line.
- LRU, bounded to 3,000,000 sessions (~36 MB) and 400 symbols.
- **Incremental refresh**: when a request needs a newer final session, only the
  tail plus a 14-day overlap is fetched. If the overlap matches (within 5e-6) the
  new sessions are appended; cached values win on the overlap, so the result is
  deterministic. If it does not match (a new dividend or split re-bases every
  earlier adjusted close, or a close was corrected) the full history is fetched
  again, so adjustment bases are never spliced.
- **Single flight**: concurrent requests for the same symbol share one upstream
  fetch.
- **Retries**: timeouts, connection resets and temporary 5xx retry up to 3 times,
  upstream throttling up to 2 times, with exponential backoff and jitter. Unknown
  tickers, missing history and invalid input never retry. Unknown and unsupported
  tickers are remembered for an hour.
- **Circuit breaker**: 5 consecutive upstream failures open the circuit for 30 s
  (doubling up to 5 min after a failed probe). While open, cached history is still
  served (marked `stale` with the reason), uncached requests fail fast with a
  typed, retryable error, and exactly one probe is let through after the cooldown.
- **Concurrency**: at most 4 concurrent yfinance calls; excess work queues for up
  to 30 s. Measured on 2026-09-30: with a warm session, throughput was flat from 1
  to 8 concurrent full-history fetches (~70 ms each; 8 was slightly slower), so 4
  bounds burst load on Yahoo and on Render's small CPU without costing latency.
- **Prewarm**: on startup, in a background thread (readiness never waits), the
  full histories of SPY, QQQ, IWM, BND and GLD are loaded. Full histories cover
  the sample portfolio, the default benchmark and every stress window (GFC,
  COVID, 2022).
- **No keep-warm**: the Render free instance sleeps after 15 idle minutes and its
  in-memory cache is lost. Portfolio Lab sends a nonblocking wake request when its
  landing page loads and shows "Waking market-data service…" if Analyze is
  clicked first. After a wake, prewarm refills the common symbols in seconds.

## Environment

| Variable | Required | Meaning |
|---|---|---|
| `PORTFOLIO_LAB_SERVICE_KEY` | yes | Shared secret, at least 32 characters (same value as Portfolio Lab's `MARKET_DATA_SERVICE_KEY`). Without it every data route returns 503. |
| `MARKET_DATA_PREWARM` | no | `0` disables the startup prewarm. Default on. |
| `MARKET_DATA_PREWARM_TICKERS` | no | Comma-separated override of the prewarm list. |
| `PYTHON_VERSION` | Render | `3.12.8` (set in `render.yaml`). |

Generate a key with `python3 -c "import secrets; print(secrets.token_hex(32))"`.
It exists only in Render, Vercel and GitHub secrets. It is never logged: every
request logs one JSON line with request id, route, symbol count, cache
hits/misses, upstream calls and time, retries, circuit state and error
categories, and never prices or the key.

## Tests

`tests/test_market_data.py` (33 tests) fakes yfinance entirely; CI never
depends on Yahoo. Run `python -m pytest tests -q`; CI runs them on every push.
