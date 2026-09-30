"""Which daily bars are final.

During U.S. trading hours Yahoo's newest daily bar is the session in progress: a
moving intraday price, not a close. Only sessions whose close has passed (16:20
New York, a few minutes after the 16:00 close for the final print) are cached or
returned. Market holidays are not modelled here; on a holiday there is simply no
new bar, and the caller's exchange calendar decides what is missing.
"""
import datetime as dt
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
CLOSE_FINAL = dt.time(16, 20)


def now_ny() -> dt.datetime:
    return dt.datetime.now(NY)


def final_session(now: dt.datetime) -> dt.date:
    """Most recent weekday whose close is final at `now` (New York time)."""
    local = now.astimezone(NY) if now.tzinfo else now
    d = local.date()
    if d.weekday() >= 5 or local.time() < CLOSE_FINAL:
        d -= dt.timedelta(days=1)
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    return d
