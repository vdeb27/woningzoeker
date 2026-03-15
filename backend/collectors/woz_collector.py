"""
WOZ Waardeloket collector.

Fetches WOZ (property tax) values from wozwaardeloket.nl.
WOZ values are updated annually (reference date January 1st).
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
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
class WOZResult:
    """Result from WOZ value lookup."""

    postcode: str
    huisnummer: int
    huisletter: Optional[str] = None
    toevoeging: Optional[str] = None
    woz_waarde: Optional[int] = None
    peildatum: Optional[str] = None  # e.g., "2024-01-01"
    peiljaar: Optional[int] = None
    adres: Optional[str] = None
    woonplaats: Optional[str] = None
    object_type: Optional[str] = None
    oppervlakte: Optional[int] = None
    fetch_date: datetime = field(default_factory=datetime.now)
    source: str = "wozwaardeloket.nl"
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "postcode": self.postcode,
            "huisnummer": self.huisnummer,
            "huisletter": self.huisletter,
            "toevoeging": self.toevoeging,
            "woz_waarde": self.woz_waarde,
            "peildatum": self.peildatum,
            "peiljaar": self.peiljaar,
            "adres": self.adres,
            "woonplaats": self.woonplaats,
            "object_type": self.object_type,
            "oppervlakte": self.oppervlakte,
            "fetch_date": self.fetch_date.isoformat(),
            "source": self.source,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WOZResult":
        """Create from dictionary."""
        fetch_date = data.get("fetch_date")
        if isinstance(fetch_date, str):
            fetch_date = datetime.fromisoformat(fetch_date)
        elif fetch_date is None:
            fetch_date = datetime.now()

        return cls(
            postcode=data.get("postcode", ""),
            huisnummer=data.get("huisnummer", 0),
            huisletter=data.get("huisletter"),
            toevoeging=data.get("toevoeging"),
            woz_waarde=data.get("woz_waarde"),
            peildatum=data.get("peildatum"),
            peiljaar=data.get("peiljaar"),
            adres=data.get("adres"),
            woonplaats=data.get("woonplaats"),
            object_type=data.get("object_type"),
            oppervlakte=data.get("oppervlakte"),
            fetch_date=fetch_date,
            source=data.get("source", "wozwaardeloket.nl"),
            error=data.get("error"),
        )


@dataclass
class WOZCollector:
    """
    Collector for WOZ (property tax) values.

    Scrapes wozwaardeloket.nl with rate limiting and caching.
    WOZ values change annually, so cache is valid for the entire year.

    Parameters
    ----------
    min_delay : float
        Minimum delay between requests in seconds (default: 2.0)
    max_delay : float
        Maximum delay between requests in seconds (default: 4.0)
    cache_dir : Path, optional
        Directory for caching results (default: data/cache/woz)
    cache_days : int
        Number of days to cache results (default: 365, since WOZ is annual)
    """

    min_delay: float = 2.0
    max_delay: float = 4.0
    cache_dir: Optional[Path] = None
    cache_days: int = 365
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    # PDOK for address lookup, Kadaster LV-WOZ for WOZ values
    PDOK_API_URL = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"
    WOZ_API_URL = "https://api.kadaster.nl/lvwoz/wozwaardeloket-api/v1/wozwaarde/nummeraanduiding"

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with random user agent."""
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def _rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        now = time.perf_counter()
        elapsed = now - self._last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.perf_counter()

    def _get_cache_path(
        self,
        postcode: str,
        huisnummer: int,
        huisletter: Optional[str] = None,
        toevoeging: Optional[str] = None,
    ) -> Optional[Path]:
        """Get cache file path for an address."""
        if not self.cache_dir:
            return None

        # Create filename from address components
        pc = postcode.replace(" ", "").upper()
        filename = f"woz_{pc}_{huisnummer}"
        if huisletter:
            filename += f"_{huisletter}"
        if toevoeging:
            filename += f"_{toevoeging}"
        filename += ".json"

        return self.cache_dir / filename

    def _load_from_cache(
        self,
        postcode: str,
        huisnummer: int,
        huisletter: Optional[str] = None,
        toevoeging: Optional[str] = None,
    ) -> Optional[WOZResult]:
        """Load cached result if valid."""
        cache_path = self._get_cache_path(postcode, huisnummer, huisletter, toevoeging)
        if not cache_path or not cache_path.exists():
            return None

        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            result = WOZResult.from_dict(data)

            # Check if cache is still valid
            cache_age = datetime.now() - result.fetch_date
            if cache_age > timedelta(days=self.cache_days):
                return None

            return result

        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    def _save_to_cache(self, result: WOZResult) -> None:
        """Save result to cache."""
        cache_path = self._get_cache_path(
            result.postcode,
            result.huisnummer,
            result.huisletter,
            result.toevoeging,
        )
        if not cache_path:
            return

        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)
        except IOError:
            pass  # Cache write failure is not critical

    def _lookup_address_id(
        self,
        postcode: str,
        huisnummer: int,
        huisletter: Optional[str] = None,
        toevoeging: Optional[str] = None,
    ) -> Optional[str]:
        """
        Look up the address ID using PDOK locatieserver.

        Returns the nummeraanduiding ID needed for WOZ lookup.
        """
        # Format search query
        pc = postcode.replace(" ", "")
        query = f"{pc} {huisnummer}"
        if huisletter:
            query += huisletter
        if toevoeging:
            query += f" {toevoeging}"

        params = {
            "q": query,
            "fq": "type:adres",
            "rows": 1,
            "fl": "id,weergavenaam,nummeraanduiding_id,postcode,huisnummer,huisletter,huisnummertoevoeging",
        }

        try:
            self._rate_limit()
            logger.info(f"Looking up address ID for: {query}")
            response = self.session.get(
                self.PDOK_API_URL,
                params=params,
                headers=self._get_headers(),
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            docs = data.get("response", {}).get("docs", [])
            if docs:
                num_id = docs[0].get("nummeraanduiding_id")
                logger.info(f"Found nummeraanduiding_id: {num_id}")
                return num_id
            else:
                logger.warning(f"No address found for query: {query}")

        except requests.RequestException as e:
            logger.error(f"Error looking up address: {e}")

        return None

    def _fetch_woz_value(self, nummeraanduiding_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch WOZ value for a nummeraanduiding ID.

        Uses the Kadaster LV-WOZ API (same as WOZ Waardeloket website).
        """
        try:
            self._rate_limit()

            url = f"{self.WOZ_API_URL}/{nummeraanduiding_id}"
            logger.info(f"Fetching WOZ from Kadaster: {url}")

            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=30,
            )
            logger.info(f"WOZ response status: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                logger.info(f"WOZ data received for {nummeraanduiding_id}")
                return data
            else:
                logger.warning(f"WOZ response error: {response.status_code} - {response.text[:200]}")

        except requests.RequestException as e:
            logger.error(f"Error fetching WOZ value: {e}")

        return None

    def get_woz_value(
        self,
        postcode: str,
        huisnummer: int,
        huisletter: Optional[str] = None,
        toevoeging: Optional[str] = None,
        use_cache: bool = True,
    ) -> WOZResult:
        """
        Get WOZ value for an address.

        Parameters
        ----------
        postcode : str
            Dutch postcode (e.g., "2511 AB" or "2511AB")
        huisnummer : int
            House number
        huisletter : str, optional
            House letter (e.g., "A")
        toevoeging : str, optional
            House number suffix (e.g., "2", "bis")
        use_cache : bool
            Whether to use cached results (default: True)

        Returns
        -------
        WOZResult
            Object containing WOZ value and metadata
        """
        pc = postcode.replace(" ", "").upper()

        # Try cache first
        if use_cache:
            cached = self._load_from_cache(pc, huisnummer, huisletter, toevoeging)
            if cached:
                return cached

        result = WOZResult(
            postcode=pc,
            huisnummer=huisnummer,
            huisletter=huisletter,
            toevoeging=toevoeging,
        )

        # Step 1: Look up address ID
        nummeraanduiding_id = self._lookup_address_id(
            pc, huisnummer, huisletter, toevoeging
        )

        if not nummeraanduiding_id:
            result.error = "Adres niet gevonden"
            return result

        # Step 2: Fetch WOZ value
        woz_data = self._fetch_woz_value(nummeraanduiding_id)

        if not woz_data:
            result.error = "WOZ waarde niet beschikbaar"
            return result

        # Parse Kadaster LV-WOZ API response
        try:
            woz_obj = woz_data.get("wozObject", {})
            woz_waarden = woz_data.get("wozWaarden", [])

            # Get most recent WOZ value (first in list, sorted by peildatum desc)
            if woz_waarden:
                # Sort by peildatum descending to get most recent
                sorted_waarden = sorted(woz_waarden, key=lambda x: x.get("peildatum", ""), reverse=True)
                meest_recent = sorted_waarden[0]

                result.woz_waarde = int(meest_recent.get("vastgesteldeWaarde", 0))
                result.peildatum = meest_recent.get("peildatum")
                if result.peildatum:
                    year_match = re.search(r"(\d{4})", result.peildatum)
                    if year_match:
                        result.peiljaar = int(year_match.group(1))

            # Address info from wozObject
            straat = woz_obj.get("straatnaam") or woz_obj.get("openbareruimtenaam")
            huisnr = woz_obj.get("huisnummer", "")
            huisletter = woz_obj.get("huisletter") or ""
            toevoeging = woz_obj.get("huisnummertoevoeging") or ""

            if straat:
                result.adres = f"{straat} {huisnr}{huisletter}{toevoeging}".strip()

            result.woonplaats = woz_obj.get("woonplaatsnaam")
            result.oppervlakte = woz_obj.get("grondoppervlakte")

            logger.info(f"Parsed WOZ: €{result.woz_waarde} (peildatum {result.peildatum})")

        except (KeyError, ValueError, IndexError, TypeError) as e:
            logger.error(f"Error parsing WOZ data: {e}")
            result.error = f"Fout bij verwerken WOZ data: {str(e)}"

        # Cache result (even errors, to prevent repeated failed lookups)
        if use_cache:
            self._save_to_cache(result)

        return result

    def get_woz_history(
        self,
        postcode: str,
        huisnummer: int,
        huisletter: Optional[str] = None,
        toevoeging: Optional[str] = None,
    ) -> list[WOZResult]:
        """
        Get historical WOZ values for an address.

        Returns list of WOZ values from different years.
        Note: Historical data availability depends on the API.
        """
        # Historical WOZ data is typically not publicly available
        # through the Waardeloket - only current year
        # This method is a placeholder for future implementation
        current = self.get_woz_value(postcode, huisnummer, huisletter, toevoeging)
        return [current] if current.woz_waarde else []


def create_woz_collector(cache_dir: Optional[Path] = None) -> WOZCollector:
    """
    Factory function to create a WOZ collector with default cache directory.

    Parameters
    ----------
    cache_dir : Path, optional
        Cache directory. If None, uses data/cache/woz.

    Returns
    -------
    WOZCollector
        Configured collector instance
    """
    if cache_dir is None:
        # Default cache directory relative to project root
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "woz"

    return WOZCollector(cache_dir=cache_dir)
