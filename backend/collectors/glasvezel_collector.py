"""
Glasvezel beschikbaarheid collector via whitelabeled.nl API.

Haalt internet-beschikbaarheid op per adres: glasvezel, kabel en DSL.
Gebruikt de API achter glasvezelcheck.nl (whitelabeled.nl widget).

Data source: https://glasvezelcheck.nl/
"""

from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


@dataclass
class GlasvezelResult:
    """Result from internet availability lookup."""

    postcode: str
    huisnummer: int
    glasvezel_beschikbaar: Optional[bool] = None
    glasvezel_snelheid: Optional[int] = None  # Mbit/s
    glasvezel_provider: Optional[str] = None
    kabel_beschikbaar: Optional[bool] = None
    kabel_snelheid: Optional[int] = None  # Mbit/s
    kabel_provider: Optional[str] = None
    dsl_snelheid: Optional[int] = None  # Mbit/s
    max_snelheid: Optional[int] = None  # Mbit/s
    adres_gevonden: bool = False
    fetch_date: datetime = field(default_factory=datetime.now)
    source: str = "glasvezelcheck.nl"
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "postcode": self.postcode,
            "huisnummer": self.huisnummer,
            "glasvezel_beschikbaar": self.glasvezel_beschikbaar,
            "glasvezel_snelheid": self.glasvezel_snelheid,
            "glasvezel_provider": self.glasvezel_provider,
            "kabel_beschikbaar": self.kabel_beschikbaar,
            "kabel_snelheid": self.kabel_snelheid,
            "kabel_provider": self.kabel_provider,
            "dsl_snelheid": self.dsl_snelheid,
            "max_snelheid": self.max_snelheid,
            "adres_gevonden": self.adres_gevonden,
            "fetch_date": self.fetch_date.isoformat(),
            "source": self.source,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GlasvezelResult":
        """Create from dictionary."""
        fetch_date = data.get("fetch_date")
        if isinstance(fetch_date, str):
            fetch_date = datetime.fromisoformat(fetch_date)
        elif fetch_date is None:
            fetch_date = datetime.now()

        return cls(
            postcode=data.get("postcode", ""),
            huisnummer=data.get("huisnummer", 0),
            glasvezel_beschikbaar=data.get("glasvezel_beschikbaar"),
            glasvezel_snelheid=data.get("glasvezel_snelheid"),
            glasvezel_provider=data.get("glasvezel_provider"),
            kabel_beschikbaar=data.get("kabel_beschikbaar"),
            kabel_snelheid=data.get("kabel_snelheid"),
            kabel_provider=data.get("kabel_provider"),
            dsl_snelheid=data.get("dsl_snelheid"),
            max_snelheid=data.get("max_snelheid"),
            adres_gevonden=data.get("adres_gevonden", False),
            fetch_date=fetch_date,
            source=data.get("source", "glasvezelcheck.nl"),
            error=data.get("error"),
        )


@dataclass
class GlasvezelCollector:
    """
    Collector for internet availability via whitelabeled.nl API.

    Parameters
    ----------
    min_delay : float
        Minimum delay between requests in seconds (default: 2.0)
    max_delay : float
        Maximum delay between requests in seconds (default: 3.0)
    cache_dir : Path, optional
        Directory for caching results
    cache_days : int
        Number of days to cache results (default: 7)
    """

    min_delay: float = 2.0
    max_delay: float = 3.0
    cache_dir: Optional[Path] = None
    cache_days: int = 7
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    API_BASE = "https://api-internet.whitelabeled.nl/v1"
    WEBSITE_ID = "evzvryREire4CbxN-251"

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json",
            "Accept-Language": "nl-NL,nl;q=0.9",
            "Referer": "https://glasvezelcheck.nl/",
        }

    def _rate_limit(self) -> None:
        now = time.perf_counter()
        elapsed = now - self._last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.perf_counter()

    def _get_cache_path(self, postcode: str, huisnummer: int) -> Optional[Path]:
        if not self.cache_dir:
            return None
        clean_pc = postcode.replace(" ", "").upper()
        return self.cache_dir / f"{clean_pc}_{huisnummer}.json"

    def _load_from_cache(self, postcode: str, huisnummer: int) -> Optional[GlasvezelResult]:
        cache_path = self._get_cache_path(postcode, huisnummer)
        if not cache_path or not cache_path.exists():
            return None
        try:
            data = json.loads(cache_path.read_text())
            result = GlasvezelResult.from_dict(data)
            age = (datetime.now() - result.fetch_date).days
            if age <= self.cache_days:
                logger.debug("Cache hit for %s %s (age: %d days)", postcode, huisnummer, age)
                return result
            logger.debug("Cache expired for %s %s (age: %d days)", postcode, huisnummer, age)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning("Cache read error for %s %s: %s", postcode, huisnummer, e)
        return None

    def _save_to_cache(self, result: GlasvezelResult) -> None:
        cache_path = self._get_cache_path(result.postcode, result.huisnummer)
        if not cache_path:
            return
        try:
            cache_path.write_text(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
        except OSError as e:
            logger.warning("Cache write error: %s", e)

    def get_beschikbaarheid(self, postcode: str, huisnummer: int) -> GlasvezelResult:
        """
        Check internet availability for an address.

        Parameters
        ----------
        postcode : str
            Dutch postcode (e.g. "2511AB")
        huisnummer : int
            House number

        Returns
        -------
        GlasvezelResult
            Internet availability details
        """
        clean_pc = postcode.replace(" ", "").upper()

        # Check cache
        cached = self._load_from_cache(clean_pc, huisnummer)
        if cached:
            return cached

        self._rate_limit()

        url = f"{self.API_BASE}/compare/{self.WEBSITE_ID}/{clean_pc}/{huisnummer}"

        try:
            response = self.session.get(url, headers=self._get_headers(), timeout=15)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            logger.error("API request failed for %s %s: %s", clean_pc, huisnummer, e)
            return GlasvezelResult(
                postcode=clean_pc,
                huisnummer=huisnummer,
                error=f"API request failed: {e}",
            )
        except (ValueError, KeyError) as e:
            logger.error("Invalid API response for %s %s: %s", clean_pc, huisnummer, e)
            return GlasvezelResult(
                postcode=clean_pc,
                huisnummer=huisnummer,
                error=f"Invalid response: {e}",
            )

        location = data.get("location", {})
        result = self._parse_location(clean_pc, huisnummer, location)
        self._save_to_cache(result)
        return result

    def _parse_location(self, postcode: str, huisnummer: int, location: Dict[str, Any]) -> GlasvezelResult:
        """Parse the location object from the API response."""
        max_fiber = location.get("max_fiber")
        fiber_network = location.get("fiber_network")
        fiber_summary = location.get("fiber_summary", "")

        max_cable = location.get("max_cable")
        cable_network = location.get("cable_network")

        max_dsl = location.get("max_dsl")
        max_speed = location.get("max_speed")

        glasvezel_beschikbaar = fiber_summary == "AVAILABLE" or (max_fiber is not None and max_fiber > 0)

        # Cable network is a string or dict
        kabel_provider = None
        if isinstance(cable_network, dict):
            kabel_provider = ", ".join(cable_network.keys()) if cable_network else None
        elif isinstance(cable_network, str):
            kabel_provider = cable_network

        # Fiber network can also be dict with provider speeds
        glasvezel_provider = None
        if isinstance(fiber_network, dict):
            glasvezel_provider = ", ".join(fiber_network.keys()) if fiber_network else None
        elif isinstance(fiber_network, str):
            glasvezel_provider = fiber_network

        return GlasvezelResult(
            postcode=postcode,
            huisnummer=huisnummer,
            glasvezel_beschikbaar=glasvezel_beschikbaar,
            glasvezel_snelheid=int(max_fiber) if max_fiber else None,
            glasvezel_provider=glasvezel_provider,
            kabel_beschikbaar=max_cable is not None and max_cable > 0,
            kabel_snelheid=int(max_cable) if max_cable else None,
            kabel_provider=kabel_provider,
            dsl_snelheid=int(max_dsl) if max_dsl else None,
            max_snelheid=int(max_speed) if max_speed else None,
            adres_gevonden=location.get("address_found", False),
        )


def create_glasvezel_collector(cache_dir: Optional[Path] = None) -> GlasvezelCollector:
    """Factory function with default cache directory."""
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "glasvezel"
    return GlasvezelCollector(cache_dir=cache_dir)
