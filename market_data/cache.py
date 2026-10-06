"""Bounded in-memory history cache.

One entry per (provider, ticker, interval, adjustment convention) holding the
ticker's complete known daily history as compact arrays (4-byte date ordinal +
8-byte price per session), so any requested date range is a slice of one entry
rather than its own cache line. Entries are immutable once stored: refreshes
build a new entry and swap it in, so readers never see a half-merged series.
Least-recently-used entries are evicted when either bound is exceeded.
"""
import bisect
import datetime as dt
import threading
from array import array
from collections import OrderedDict
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Entry:
    meta: dict
    dates: array  # 'i' date ordinals, strictly increasing
    closes: array  # 'd' adjusted closes, aligned with dates
    fetched_at: str  # last full-history download (UTC ISO)
    refreshed_at: str  # last successful upstream contact (UTC ISO)
    refreshed_mono: float  # monotonic clock at refreshed_at
    checked_through: dt.date  # final session this entry is known complete through
    stats: dict = field(default_factory=dict)
    untraded: frozenset = frozenset()  # date ordinals of OTC sessions with no trades

    @property
    def rows(self) -> int:
        return len(self.dates)

    @property
    def last_date(self) -> dt.date | None:
        return dt.date.fromordinal(self.dates[-1]) if self.dates else None

    def window(self, start: dt.date, end: dt.date) -> tuple[list[str], list[float]]:
        lo = bisect.bisect_left(self.dates, start.toordinal())
        hi = bisect.bisect_right(self.dates, end.toordinal())
        return ([dt.date.fromordinal(o).isoformat() for o in self.dates[lo:hi]],
                list(self.closes[lo:hi]))


def ordinals(days) -> frozenset:
    return frozenset(dt.date.fromisoformat(d).toordinal() for d in days)


def build(rows: list[tuple[str, float]]) -> tuple[array, array]:
    dates, closes = array("i"), array("d")
    for day, price in rows:
        dates.append(dt.date.fromisoformat(day).toordinal())
        closes.append(price)
    return dates, closes


class HistoryCache:
    def __init__(self, max_rows: int = 3_000_000, max_entries: int = 400):
        self.max_rows = max_rows
        self.max_entries = max_entries
        self._lock = threading.Lock()
        self._entries: "OrderedDict[tuple, Entry]" = OrderedDict()
        self._rows = 0

    def get(self, key) -> Entry | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                self._entries.move_to_end(key)
            return entry

    def put(self, key, entry: Entry):
        with self._lock:
            old = self._entries.pop(key, None)
            if old is not None:
                self._rows -= old.rows
            if entry.rows > self.max_rows:
                return  # larger than the whole cache: served, not retained
            while self._entries and (len(self._entries) >= self.max_entries
                                     or self._rows + entry.rows > self.max_rows):
                _, evicted = self._entries.popitem(last=False)
                self._rows -= evicted.rows
            self._entries[key] = entry
            self._rows += entry.rows

    def stats(self) -> dict:
        with self._lock:
            return {"entries": len(self._entries), "rows": self._rows, "maxRows": self.max_rows,
                    "approxBytes": self._rows * 12}
