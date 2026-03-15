"""
Energielabel collector using EP-Online (RVO).

Fetches official energy labels for buildings from the RVO EP-Online register.
Energy labels are valid for 10 years.

Data source: https://www.ep-online.nl/
API documentation: https://public.ep-online.nl/swagger/
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


@dataclass
class EnergielabelResult:
    """Result from energy label lookup."""

    postcode: str
    huisnummer: int
    huisletter: Optional[str] = None
    toevoeging: Optional[str] = None
    energielabel: Optional[str] = None  # e.g., "A", "B", "C", "A+++"
    energieindex: Optional[float] = None  # Numeric energy index
    label_type: Optional[str] = None  # "definitief" or "voorlopig"
    registratiedatum: Optional[str] = None
    geldig_tot: Optional[str] = None
    opname_datum: Optional[str] = None
    gebouwtype: Optional[str] = None
    gebouwklasse: Optional[str] = None  # "woningbouw" or "utiliteitsbouw"
    bouwjaar: Optional[int] = None
    gebruiksoppervlakte: Optional[float] = None
    adres: Optional[str] = None
    woonplaats: Optional[str] = None
    bag_id: Optional[str] = None  # BAG verblijfsobject or pand ID
    fetch_date: datetime = field(default_factory=datetime.now)
    source: str = "ep-online.nl"
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "postcode": self.postcode,
            "huisnummer": self.huisnummer,
            "huisletter": self.huisletter,
            "toevoeging": self.toevoeging,
            "energielabel": self.energielabel,
            "energieindex": self.energieindex,
            "label_type": self.label_type,
            "registratiedatum": self.registratiedatum,
            "geldig_tot": self.geldig_tot,
            "opname_datum": self.opname_datum,
            "gebouwtype": self.gebouwtype,
            "gebouwklasse": self.gebouwklasse,
            "bouwjaar": self.bouwjaar,
            "gebruiksoppervlakte": self.gebruiksoppervlakte,
            "adres": self.adres,
            "woonplaats": self.woonplaats,
            "bag_id": self.bag_id,
            "fetch_date": self.fetch_date.isoformat(),
            "source": self.source,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EnergielabelResult":
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
            energielabel=data.get("energielabel"),
            energieindex=data.get("energieindex"),
            label_type=data.get("label_type"),
            registratiedatum=data.get("registratiedatum"),
            geldig_tot=data.get("geldig_tot"),
            opname_datum=data.get("opname_datum"),
            gebouwtype=data.get("gebouwtype"),
            gebouwklasse=data.get("gebouwklasse"),
            bouwjaar=data.get("bouwjaar"),
            gebruiksoppervlakte=data.get("gebruiksoppervlakte"),
            adres=data.get("adres"),
            woonplaats=data.get("woonplaats"),
            bag_id=data.get("bag_id"),
            fetch_date=fetch_date,
            source=data.get("source", "ep-online.nl"),
            error=data.get("error"),
        )

    @property
    def is_valid(self) -> bool:
        """Check if the energy label is still valid."""
        if not self.geldig_tot:
            return True  # Assume valid if no expiry date
        try:
            expiry = datetime.fromisoformat(self.geldig_tot.replace("Z", "+00:00"))
            return datetime.now(expiry.tzinfo) < expiry
        except (ValueError, TypeError):
            return True


@dataclass
class EnergielabelCollector:
    """
    Collector for energy labels from EP-Online.

    The EP-Online public API provides access to registered energy labels
    for buildings in the Netherlands.

    Parameters
    ----------
    api_key : str, optional
        EP-Online API key from RVO. Required for authenticated access.
    min_delay : float
        Minimum delay between requests in seconds (default: 1.0)
    max_delay : float
        Maximum delay between requests in seconds (default: 2.0)
    cache_dir : Path, optional
        Directory for caching results
    cache_days : int
        Number of days to cache results (default: 30)
    """

    api_key: Optional[str] = None
    min_delay: float = 1.0
    max_delay: float = 2.0
    cache_dir: Optional[Path] = None
    cache_days: int = 30
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    # EP-Online public API
    BASE_URL = "https://public.ep-online.nl"
    API_URL = f"{BASE_URL}/api/v5"

    # Alternative: PDOK Locatieserver for address lookup
    PDOK_API_URL = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        if not self.api_key:
            logger.warning("EnergielabelCollector: no API key configured")

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers including API key authentication if configured."""
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json",
            "Accept-Language": "nl-NL,nl;q=0.9",
        }
        if self.api_key:
            # EP-Online uses Authorization header with API key directly (no Bearer prefix)
            headers["Authorization"] = self.api_key
        return headers

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

        pc = postcode.replace(" ", "").upper()
        filename = f"energielabel_{pc}_{huisnummer}"
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
    ) -> Optional[EnergielabelResult]:
        """Load cached result if valid."""
        cache_path = self._get_cache_path(postcode, huisnummer, huisletter, toevoeging)
        if not cache_path or not cache_path.exists():
            return None

        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            result = EnergielabelResult.from_dict(data)

            # Check cache validity
            cache_age = datetime.now() - result.fetch_date
            if cache_age > timedelta(days=self.cache_days):
                return None

            return result

        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    def _save_to_cache(self, result: EnergielabelResult) -> None:
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
            pass

    def _lookup_bag_id(
        self,
        postcode: str,
        huisnummer: int,
        huisletter: Optional[str] = None,
        toevoeging: Optional[str] = None,
    ) -> Optional[str]:
        """
        Look up the BAG verblijfsobject ID using PDOK locatieserver.

        EP-Online labels are linked to BAG IDs.
        """
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
            "fl": "id,adresseerbaarobject_id,nummeraanduiding_id",
        }

        try:
            self._rate_limit()
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
                # Try adresseerbaarobject_id first, then nummeraanduiding_id
                return docs[0].get("adresseerbaarobject_id") or docs[0].get("nummeraanduiding_id")

        except requests.RequestException:
            pass

        return None

    def _fetch_label_by_bag_id(self, bag_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch energy label by BAG verblijfsobject ID (fallback method).
        """
        try:
            self._rate_limit()
            logger.info(f"Fetching energielabel by BAG ID: {bag_id}")

            url = f"{self.API_URL}/PandEnergielabel/Adres/{bag_id}"
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=30,
            )

            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"EP-Online BAG lookup failed: {response.status_code}")

        except requests.RequestException as e:
            logger.error(f"Request error fetching by BAG ID: {e}")

        return None

    def _fetch_label_by_address(
        self,
        postcode: str,
        huisnummer: int,
        huisletter: Optional[str] = None,
        toevoeging: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch energy label directly by address components.
        """
        try:
            self._rate_limit()

            pc = postcode.replace(" ", "").upper()

            # EP-Online v5 endpoint for address lookup
            url = f"{self.API_URL}/PandEnergielabel/Adres"
            params = {
                "postcode": pc,
                "huisnummer": str(huisnummer),
            }
            if huisletter:
                params["huisletter"] = huisletter.upper()
            if toevoeging:
                params["huisnummertoevoeging"] = toevoeging

            headers = self._get_headers()
            logger.info(f"Fetching energielabel: {pc} {huisnummer}")

            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=30,
            )
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"EP-Online API error {response.status_code}: {response.text[:200]}")

        except requests.RequestException as e:
            logger.error(f"Request error fetching by address: {e}")

        return None

    def get_energielabel(
        self,
        postcode: str,
        huisnummer: int,
        huisletter: Optional[str] = None,
        toevoeging: Optional[str] = None,
        use_cache: bool = True,
    ) -> EnergielabelResult:
        """
        Get energy label for an address.

        Parameters
        ----------
        postcode : str
            Dutch postcode (e.g., "2511 AB" or "2511AB")
        huisnummer : int
            House number
        huisletter : str, optional
            House letter (e.g., "A")
        toevoeging : str, optional
            House number suffix
        use_cache : bool
            Whether to use cached results (default: True)

        Returns
        -------
        EnergielabelResult
            Object containing energy label and metadata
        """
        pc = postcode.replace(" ", "").upper()

        # Try cache first
        if use_cache:
            cached = self._load_from_cache(pc, huisnummer, huisletter, toevoeging)
            if cached:
                return cached

        result = EnergielabelResult(
            postcode=pc,
            huisnummer=huisnummer,
            huisletter=huisletter,
            toevoeging=toevoeging,
        )

        # Method 1: Try direct address lookup
        label_data = self._fetch_label_by_address(pc, huisnummer, huisletter, toevoeging)

        # Method 2: Try via BAG ID
        if not label_data:
            bag_id = self._lookup_bag_id(pc, huisnummer, huisletter, toevoeging)
            if bag_id:
                result.bag_id = bag_id
                label_data = self._fetch_label_by_bag_id(bag_id)

        if not label_data:
            result.error = "Energielabel niet gevonden"
            if use_cache:
                self._save_to_cache(result)
            return result

        # Parse response - handle different formats
        try:
            # If it's a list, take the first (most recent) entry
            if isinstance(label_data, list):
                if len(label_data) == 0:
                    result.error = "Geen energielabel geregistreerd"
                    if use_cache:
                        self._save_to_cache(result)
                    return result
                label_data = label_data[0]

            # Extract fields from EP-Online v5 API response
            # Field names use CamelCase with exact capitalization
            result.energielabel = label_data.get("Energieklasse")

            energieindex = label_data.get("EnergieIndex")
            if energieindex is not None:
                result.energieindex = float(energieindex)

            result.label_type = label_data.get("Soort_opname")
            result.registratiedatum = label_data.get("Registratiedatum")
            result.geldig_tot = label_data.get("Geldig_tot")
            result.opname_datum = label_data.get("Opnamedatum")
            result.gebouwtype = label_data.get("Gebouwtype")
            result.gebouwklasse = label_data.get("Gebouwklasse")

            bouwjaar = label_data.get("Bouwjaar")
            if bouwjaar:
                result.bouwjaar = int(bouwjaar)

            oppervlakte = label_data.get("Gebruiksoppervlakte_thermische_zone")
            if oppervlakte:
                result.gebruiksoppervlakte = float(oppervlakte)

            # No street address in API response, but we have postcode/huisnummer
            result.adres = None
            result.woonplaats = None

            if not result.bag_id:
                result.bag_id = label_data.get("BAGVerblijfsobjectID")

            logger.info(f"Found energielabel: {result.energielabel}")

        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Error parsing label data: {e}")
            result.error = f"Fout bij verwerken data: {str(e)}"

        if use_cache:
            self._save_to_cache(result)

        return result

    def get_multiple_labels(
        self,
        addresses: List[Dict[str, Any]],
        use_cache: bool = True,
    ) -> List[EnergielabelResult]:
        """
        Get energy labels for multiple addresses.

        Parameters
        ----------
        addresses : list
            List of dicts with keys: postcode, huisnummer, huisletter (optional), toevoeging (optional)
        use_cache : bool
            Whether to use cached results

        Returns
        -------
        list
            List of EnergielabelResult objects
        """
        results = []
        for addr in addresses:
            result = self.get_energielabel(
                postcode=addr.get("postcode", ""),
                huisnummer=addr.get("huisnummer", 0),
                huisletter=addr.get("huisletter"),
                toevoeging=addr.get("toevoeging"),
                use_cache=use_cache,
            )
            results.append(result)
        return results


def create_energielabel_collector(
    cache_dir: Optional[Path] = None,
    api_key: Optional[str] = None,
) -> EnergielabelCollector:
    """
    Factory function to create an energielabel collector with default cache directory.

    Parameters
    ----------
    cache_dir : Path, optional
        Cache directory. If None, uses data/cache/energielabel.
    api_key : str, optional
        EP-Online API key. If None, reads from EP_ONLINE_API_KEY environment variable.

    Returns
    -------
    EnergielabelCollector
        Configured collector instance
    """
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "energielabel"

    if api_key is None:
        api_key = os.environ.get("EP_ONLINE_API_KEY")

    return EnergielabelCollector(api_key=api_key, cache_dir=cache_dir)
