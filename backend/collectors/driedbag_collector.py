"""
3DBAG collector for building height and roof data.

Fetches 3D building attributes from api.3dbag.nl, including
roof height, ground level, roof type, and building volume.
Used for ceiling height estimation.
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
class DrieDBagResult:
    """Result from 3DBAG API lookup."""

    pand_identificatie: str
    h_dak_max: Optional[float] = None
    h_dak_min: Optional[float] = None
    h_dak_50p: Optional[float] = None
    h_dak_70p: Optional[float] = None
    h_maaiveld: Optional[float] = None
    dak_type: Optional[str] = None
    bouwlagen: Optional[int] = None
    opp_grond: Optional[float] = None
    opp_dak_plat: Optional[float] = None
    opp_dak_schuin: Optional[float] = None
    volume_lod22: Optional[float] = None
    gebouwhoogte: Optional[float] = None
    fetch_date: datetime = field(default_factory=datetime.now)
    source: str = "3dbag.nl"
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pand_identificatie": self.pand_identificatie,
            "h_dak_max": self.h_dak_max,
            "h_dak_min": self.h_dak_min,
            "h_dak_50p": self.h_dak_50p,
            "h_dak_70p": self.h_dak_70p,
            "h_maaiveld": self.h_maaiveld,
            "dak_type": self.dak_type,
            "bouwlagen": self.bouwlagen,
            "opp_grond": self.opp_grond,
            "opp_dak_plat": self.opp_dak_plat,
            "opp_dak_schuin": self.opp_dak_schuin,
            "volume_lod22": self.volume_lod22,
            "gebouwhoogte": self.gebouwhoogte,
            "fetch_date": self.fetch_date.isoformat(),
            "source": self.source,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DrieDBagResult":
        fetch_date = data.get("fetch_date")
        if isinstance(fetch_date, str):
            fetch_date = datetime.fromisoformat(fetch_date)
        elif fetch_date is None:
            fetch_date = datetime.now()

        return cls(
            pand_identificatie=data.get("pand_identificatie", ""),
            h_dak_max=data.get("h_dak_max"),
            h_dak_min=data.get("h_dak_min"),
            h_dak_50p=data.get("h_dak_50p"),
            h_dak_70p=data.get("h_dak_70p"),
            h_maaiveld=data.get("h_maaiveld"),
            dak_type=data.get("dak_type"),
            bouwlagen=data.get("bouwlagen"),
            opp_grond=data.get("opp_grond"),
            opp_dak_plat=data.get("opp_dak_plat"),
            opp_dak_schuin=data.get("opp_dak_schuin"),
            volume_lod22=data.get("volume_lod22"),
            gebouwhoogte=data.get("gebouwhoogte"),
            fetch_date=fetch_date,
            source=data.get("source", "3dbag.nl"),
            error=data.get("error"),
        )


@dataclass
class DrieDBagCollector:
    """
    Collector for 3D building data from 3DBAG.

    Fetches building height, roof type, and volume data from the
    3DBAG API (api.3dbag.nl). Buildings are looked up by BAG
    pand identificatie.

    Parameters
    ----------
    min_delay : float
        Minimum delay between requests in seconds (default: 1.0)
    max_delay : float
        Maximum delay between requests in seconds (default: 2.0)
    cache_dir : Path, optional
        Directory for caching results (default: data/cache/3dbag)
    cache_days : int
        Number of days to cache results (default: 365)
    """

    min_delay: float = 1.0
    max_delay: float = 2.0
    cache_dir: Optional[Path] = None
    cache_days: int = 365
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    BASE_URL = "https://api.3dbag.nl/collections/pand/items"

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/city+json",
        }

    def _rate_limit(self) -> None:
        now = time.perf_counter()
        elapsed = now - self._last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.perf_counter()

    def _get_cache_key(self, pand_id: str) -> str:
        safe_id = pand_id.replace(".", "_")
        return f"3dbag_{safe_id}"

    def _load_from_cache(self, pand_id: str) -> Optional[DrieDBagResult]:
        if not self.cache_dir:
            return None

        cache_path = self.cache_dir / f"{self._get_cache_key(pand_id)}.json"
        if not cache_path.exists():
            return None

        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            result = DrieDBagResult.from_dict(data)
            cache_age = datetime.now() - result.fetch_date
            if cache_age > timedelta(days=self.cache_days):
                return None

            return result
        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    def _save_to_cache(self, result: DrieDBagResult) -> None:
        if not self.cache_dir:
            return

        cache_path = self.cache_dir / f"{self._get_cache_key(result.pand_identificatie)}.json"
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)
        except IOError:
            pass

    def _normalize_pand_id(self, pand_identificatie: str) -> str:
        """Ensure pand ID has the NL.IMBAG.Pand. prefix."""
        if pand_identificatie.startswith("NL.IMBAG.Pand."):
            return pand_identificatie
        return f"NL.IMBAG.Pand.{pand_identificatie}"

    def get_building_data(
        self,
        pand_identificatie: str,
        use_cache: bool = True,
    ) -> DrieDBagResult:
        """
        Fetch 3D building data for a BAG pand.

        Parameters
        ----------
        pand_identificatie : str
            BAG pand identificatie (e.g., "0518100000285158" or
            "NL.IMBAG.Pand.0518100000285158")
        use_cache : bool
            Whether to use cached results (default: True)

        Returns
        -------
        DrieDBagResult
            Building height and roof data
        """
        full_id = self._normalize_pand_id(pand_identificatie)
        raw_id = pand_identificatie.replace("NL.IMBAG.Pand.", "")

        if use_cache:
            cached = self._load_from_cache(raw_id)
            if cached:
                return cached

        result = DrieDBagResult(pand_identificatie=raw_id)

        try:
            self._rate_limit()

            url = f"{self.BASE_URL}/{full_id}"
            logger.info(f"Fetching 3DBAG data for {full_id}")
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            # CityJSON format: data is nested under feature -> CityObjects
            feature = data.get("feature", data)
            city_objects = feature.get("CityObjects", {})
            if not city_objects:
                result.error = "Geen gebouwdata gevonden in 3DBAG"
                if use_cache:
                    self._save_to_cache(result)
                return result

            # Get the parent city object (without suffix like -0)
            # The parent object contains the building-level attributes
            obj = None
            for key, val in city_objects.items():
                if not key.endswith(("-0", "-1", "-2")):
                    obj = val
                    break
            if obj is None:
                obj = next(iter(city_objects.values()))
            attrs = obj.get("attributes", {})

            result.h_dak_max = attrs.get("b3_h_dak_max")
            result.h_dak_min = attrs.get("b3_h_dak_min")
            result.h_dak_50p = attrs.get("b3_h_dak_50p")
            result.h_dak_70p = attrs.get("b3_h_dak_70p")
            result.h_maaiveld = attrs.get("b3_h_maaiveld")
            result.dak_type = attrs.get("b3_dak_type")
            result.bouwlagen = attrs.get("b3_bouwlagen")
            result.opp_grond = attrs.get("b3_opp_grond")
            result.opp_dak_plat = attrs.get("b3_opp_dak_plat")
            result.opp_dak_schuin = attrs.get("b3_opp_dak_schuin")
            result.volume_lod22 = attrs.get("b3_volume_lod22")

            # Compute building height
            if result.h_dak_max is not None and result.h_maaiveld is not None:
                result.gebouwhoogte = round(result.h_dak_max - result.h_maaiveld, 2)

            logger.info(
                f"3DBAG data for {raw_id}: hoogte={result.gebouwhoogte}m, "
                f"dak={result.dak_type}, bouwlagen={result.bouwlagen}"
            )

        except requests.RequestException as e:
            logger.error(f"Error fetching 3DBAG data: {e}")
            result.error = f"Fout bij ophalen 3DBAG data: {str(e)}"

        if use_cache:
            self._save_to_cache(result)

        return result


def create_driedbag_collector(cache_dir: Optional[Path] = None) -> DrieDBagCollector:
    """
    Factory function to create a 3DBAG collector with default cache directory.

    Parameters
    ----------
    cache_dir : Path, optional
        Cache directory. If None, uses data/cache/3dbag.

    Returns
    -------
    DrieDBagCollector
        Configured collector instance
    """
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "3dbag"

    return DrieDBagCollector(cache_dir=cache_dir)
