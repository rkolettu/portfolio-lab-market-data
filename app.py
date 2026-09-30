"""Portfolio Lab Market Data API.

Run: uvicorn app:app --host 0.0.0.0 --port $PORT

Server-to-server only: Portfolio Lab's API routes call it with a shared secret.
It retrieves, normalizes, validates and caches daily adjusted histories and
current quote observations; every finance calculation stays in Portfolio Lab.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from market_data.routes import router, start_prewarm


@asynccontextmanager
async def lifespan(_app):
    # Common histories load in the background after each (cold) start; the
    # service answers health checks immediately and never waits for them.
    start_prewarm()
    yield


app = FastAPI(title="Portfolio Lab Market Data", lifespan=lifespan,
              docs_url=None, redoc_url=None, openapi_url=None)
app.include_router(router)


@app.get("/healthz")
def healthz():
    """Unauthenticated liveness for the platform health check. Reveals nothing."""
    return {"status": "ok"}
