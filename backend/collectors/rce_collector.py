"""
RCE Rijksmonumenten collector.

Fetches monument status from api.rijksmonumenten.info (Solr-based API).
This is the official register of Dutch national monuments (rijksmonumenten).
"""

from __future__ import annotations

import json
import logging
import random
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
class RijksmonumentResult:
    """Result from Rijksmonumenten API lookup."""

    postcode: str
    huisnummer: int
    is_monument: bool = False
    monumentnummer: Optional[int] = None
    omschrijving: Optional[str] = None
    categorie: Optional[str] = None
    url: Optional[str] = None
    fetch_date: datetime = field(default_factory=datetime.now)
    source: str = "api.rijksmonumenten.info"
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "postcode": self.postcode,
            "huisnummer": self.huisnummer,
            "is_monument": self.is_monument,
            "monumentnummer": self.monumentnummer,
            "omschrijving": self.omschrijving,
            "categorie": self.categorie,
            "url": self.url,
            "fetch_date": self.fetch_date.isoformat(),
            "source": self.source,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RijksmonumentResult":
        fetch_date = data.get("fetch_date")
        if isinstance(fetch_date, str):
            fetch_date = datetime.fromisoformat(fetch_date)
        elif fetch_date is None:
            fetch_date = datetime.now()

        return cls(
            postcode=data.get("postcode", ""),
            huisnummer=data.get("huisnummer", 0),
            is_monument=data.get("is_monument", False),
            monumentnummer=data.get("monumentnummer"),
            omschrijving=data.get("omschrijving"),
            categorie=data.get("categorie"),
            url=data.get("url"),
            fetch_date=fetch_date,
            source=data.get("source", "api.rijksmonumenten.info"),
            error=data.get("error"),
        )


@dataclass
class RCECollector:
    """
    Collector for Rijksmonumenten via api.rijksmonumenten.info.

    Uses the Solr-based API to check if an address is a registered
    national monument (rijksmonument).

    Parameters
    ----------
    min_delay : float
        Minimum delay between requests in seconds (default: 2.0)
    max_delay : float
        Maximum delay between requests in seconds (default: 3.0)
    cache_dir : Path, optional
        Directory for caching results (default: data/cache/rce)
    cache_days : int
        Number of days to cache results (default: 180)
    """

    min_delay: float = 2.0
    max_delay: float = 3.0
    cache_dir: Optional[Path] = None
    cache_days: int = 180
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    SPARQL_URL = "https://api.linkeddata.cultureelerfgoed.nl/datasets/rce/cho/services/cho/sparql"

    SPARQL_QUERY_TEMPLATE = """
PREFIX ceo: <https://linkeddata.cultureelerfgoed.nl/def/ceo#>
SELECT DISTINCT ?monumentnummer ?straat ?woonplaats ?naam ?functieNaam ?aard ?omschrijving
WHERE {{
  ?monument a ceo:Rijksmonument ;
    ceo:rijksmonumentnummer ?monumentnummer ;
    ceo:heeftBasisregistratieRelatie/ceo:heeftBAGRelatie ?bag .
  ?bag ceo:postcode ?postcode ; ceo:huisnummer ?huisnummer .
  FILTER(?postcode = "{postcode}" && ?huisnummer = "{huisnummer}")
  OPTIONAL {{ ?bag ceo:openbareRuimte ?straat }}
  OPTIONAL {{ ?bag ceo:woonplaatsnaam ?woonplaats }}
  OPTIONAL {{ ?monument ceo:heeftNaam/ceo:naam ?naam }}
  OPTIONAL {{ ?monument ceo:heeftOorspronkelijkeFunctie/ceo:functieNaam ?functieNaam }}
  OPTIONAL {{ ?monument ceo:monumentAard ?aard }}
  OPTIONAL {{ ?monument ceo:heeftKennisregistratie/ceo:omschrijving ?omschrijving }}
}} LIMIT 5
"""

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    def _rate_limit(self) -> None:
        now = time.perf_counter()
        elapsed = now - self._last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.perf_counter()

    def _get_cache_key(self, postcode: str, huisnummer: int) -> str:
        pc = postcode.replace(" ", "").upper()
        return f"rce_{pc}_{huisnummer}"

    def _load_from_cache(self, postcode: str, huisnummer: int) -> Optional[RijksmonumentResult]:
        if not self.cache_dir:
            return None

        cache_path = self.cache_dir / f"{self._get_cache_key(postcode, huisnummer)}.json"
        if not cache_path.exists():
            return None

        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            result = RijksmonumentResult.from_dict(data)
            cache_age = datetime.now() - result.fetch_date
            if cache_age > timedelta(days=self.cache_days):
                return None

            return result
        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    def _save_to_cache(self, result: RijksmonumentResult) -> None:
        if not self.cache_dir:
            return

        cache_path = self.cache_dir / f"{self._get_cache_key(result.postcode, result.huisnummer)}.json"
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)
        except IOError:
            pass

    def get_monument_status(
        self,
        postcode: str,
        huisnummer: int,
        use_cache: bool = True,
    ) -> RijksmonumentResult:
        """
        Check if an address is a rijksmonument.

        Parameters
        ----------
        postcode : str
            Dutch postcode (e.g., "2513AA")
        huisnummer : int
            House number
        use_cache : bool
            Whether to use cached results (default: True)

        Returns
        -------
        RijksmonumentResult
            Monument status and details
        """
        pc = postcode.replace(" ", "").upper()

        if use_cache:
            cached = self._load_from_cache(pc, huisnummer)
            if cached:
                return cached

        result = RijksmonumentResult(postcode=pc, huisnummer=huisnummer)

        try:
            self._rate_limit()

            # Build SPARQL query
            sparql = self.SPARQL_QUERY_TEMPLATE.format(
                postcode=pc, huisnummer=huisnummer
            )

            logger.info(f"Querying RCE SPARQL for {pc} {huisnummer}")
            response = self.session.post(
                self.SPARQL_URL,
                data={"query": sparql},
                headers=self._get_headers(),
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            bindings = data.get("results", {}).get("bindings", [])

            if bindings:
                binding = bindings[0]
                result.is_monument = True

                mon_nr = binding.get("monumentnummer", {}).get("value")
                if mon_nr:
                    result.monumentnummer = int(mon_nr)

                # Build omschrijving from available fields
                omschrijving = binding.get("omschrijving", {}).get("value")
                naam = binding.get("naam", {}).get("value")
                functie_naam = binding.get("functieNaam", {}).get("value")
                straat = binding.get("straat", {}).get("value")
                woonplaats = binding.get("woonplaats", {}).get("value")

                if omschrijving:
                    # Truncate long descriptions
                    result.omschrijving = omschrijving[:300]
                    if len(omschrijving) > 300:
                        result.omschrijving += "..."
                elif naam:
                    result.omschrijving = naam
                elif functie_naam:
                    result.omschrijving = functie_naam
                elif straat and woonplaats:
                    result.omschrijving = f"{straat}, {woonplaats}"

                result.categorie = binding.get("aard", {}).get("value")

                # Build URL to monument register
                if result.monumentnummer:
                    result.url = f"https://monumentenregister.cultureelerfgoed.nl/monumenten/{result.monumentnummer}"

                logger.info(
                    f"Found rijksmonument {result.monumentnummer} at {pc} {huisnummer}"
                )
            else:
                logger.info(f"No rijksmonument found at {pc} {huisnummer}")

        except requests.RequestException as e:
            logger.error(f"Error querying RCE SPARQL: {e}")
            result.error = f"Fout bij ophalen monumentstatus: {str(e)}"

        if use_cache:
            self._save_to_cache(result)

        return result


def create_rce_collector(cache_dir: Optional[Path] = None) -> RCECollector:
    """
    Factory function to create an RCE collector with default cache directory.

    Parameters
    ----------
    cache_dir : Path, optional
        Cache directory. If None, uses data/cache/rce.

    Returns
    -------
    RCECollector
        Configured collector instance
    """
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "rce"

    return RCECollector(cache_dir=cache_dir)
