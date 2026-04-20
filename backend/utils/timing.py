"""Lightweight timing tracker for request performance monitoring."""

import logging
import time
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class _Entry:
    name: str
    ms: float
    cache_hit: bool = False


class TimingTracker:
    def __init__(self):
        self._entries: List[_Entry] = []
        self._start = time.perf_counter()

    def record(self, name: str, start: float, cache_hit: bool = False):
        ms = round((time.perf_counter() - start) * 1000, 1)
        self._entries.append(_Entry(name, ms, cache_hit))
        logger.debug('{"op":"%s","ms":%.1f,"cache":%s}', name, ms, str(cache_hit).lower())

    def total_ms(self) -> float:
        return round((time.perf_counter() - self._start) * 1000, 1)

    def to_dict(self) -> dict:
        return {
            "total_ms": self.total_ms(),
            "operations": [
                {"name": e.name, "ms": e.ms, "cache_hit": e.cache_hit}
                for e in self._entries
            ],
        }
