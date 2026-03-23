"""
PDOK Beschermde Gebieden collector.

Fetches protected area status (beschermde stads-/dorpsgezichten and UNESCO
world heritage sites) from the RCE OGC API on PDOK.
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
class BeschermdGebiedResult:
    """Result from PDOK Beschermde Gebieden lookup."""

    latitude: float
    longitude: float
    in_beschermd_gezicht: bool = False
    gezicht_naam: Optional[str] = None
    gezicht_type: Optional[str] = None  # "stadsgezicht" or "dorpsgezicht"
    gezicht_niveau: Optional[str] = None  # "rijks" or "gemeentelijk"
    gezicht_status: Optional[str] = None
    in_unesco: bool = False
    unesco_naam: Optional[str] = None
    fetch_date: datetime = field(default_factory=datetime.now)
    source: str = "PDOK RCE Beschermde Gebieden"
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "in_beschermd_gezicht": self.in_beschermd_gezicht,
            "gezicht_naam": self.gezicht_naam,
            "gezicht_type": self.gezicht_type,
            "gezicht_niveau": self.gezicht_niveau,
            "gezicht_status": self.gezicht_status,
            "in_unesco": self.in_unesco,
            "unesco_naam": self.unesco_naam,
            "fetch_date": self.fetch_date.isoformat(),
            "source": self.source,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BeschermdGebiedResult":
        fetch_date = data.get("fetch_date")
        if isinstance(fetch_date, str):
            fetch_date = datetime.fromisoformat(fetch_date)
        elif fetch_date is None:
            fetch_date = datetime.now()

        return cls(
            latitude=data.get("latitude", 0.0),
            longitude=data.get("longitude", 0.0),
            in_beschermd_gezicht=data.get("in_beschermd_gezicht", False),
            gezicht_naam=data.get("gezicht_naam"),
            gezicht_type=data.get("gezicht_type"),
            gezicht_niveau=data.get("gezicht_niveau"),
            gezicht_status=data.get("gezicht_status"),
            in_unesco=data.get("in_unesco", False),
            unesco_naam=data.get("unesco_naam"),
            fetch_date=fetch_date,
            source=data.get("source", "PDOK RCE Beschermde Gebieden"),
            error=data.get("error"),
        )


@dataclass
class PDOKBeschermdeGebiedenCollector:
    """
    Collector for beschermde stads-/dorpsgezichten and UNESCO sites via PDOK.

    Uses the OGC API Features endpoint from RCE on PDOK to check if
    coordinates fall within a protected area.

    Parameters
    ----------
    min_delay : float
        Minimum delay between requests in seconds (default: 1.0)
    max_delay : float
        Maximum delay between requests in seconds (default: 2.0)
    cache_dir : Path, optional
        Directory for caching results
    cache_days : int
        Number of days to cache results (default: 180)
    """

    min_delay: float = 1.0
    max_delay: float = 2.0
    cache_dir: Optional[Path] = None
    cache_days: int = 180
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    BASE_URL = "https://api.pdok.nl/rce/beschermde-gebieden-cultuurhistorie/ogc/v1"
    POLYGONS_URL = f"{BASE_URL}/collections/rce_inspire_polygons/items"

    # Namespace values to distinguish types
    NS_GEZICHTEN = "nlps-stadsendorpsgezichten"
    NS_UNESCO = "nlps-werelderfgoederen"

    # Gemeentelijke beschermde stadsgezichten (Den Haag open data)
    DENHAAG_GEZICHTEN_URL = "https://ckan.dataplatform.nl/dataset/9048cc39-805f-452b-81bd-d9527beaf818/resource/d38d2d4f-d18d-4283-964d-ebb8dc9461b7/download/beschermdestadsgezichten.json"

    # Class-level cache for gemeente GeoJSON (loaded once per process)
    _gemeente_gezichten: list = None

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/geo+json",
        }

    def _rate_limit(self) -> None:
        now = time.perf_counter()
        elapsed = now - self._last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.perf_counter()

    def _get_cache_key(self, lat: float, lon: float) -> str:
        # Round to 5 decimals (~1m precision) for cache key
        return f"beschermd_{lat:.5f}_{lon:.5f}"

    def _load_from_cache(self, lat: float, lon: float) -> Optional[BeschermdGebiedResult]:
        if not self.cache_dir:
            return None

        cache_path = self.cache_dir / f"{self._get_cache_key(lat, lon)}.json"
        if not cache_path.exists():
            return None

        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            result = BeschermdGebiedResult.from_dict(data)
            cache_age = datetime.now() - result.fetch_date
            if cache_age > timedelta(days=self.cache_days):
                return None

            return result
        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    def _save_to_cache(self, result: BeschermdGebiedResult) -> None:
        if not self.cache_dir:
            return

        cache_path = self.cache_dir / f"{self._get_cache_key(result.latitude, result.longitude)}.json"
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)
        except IOError:
            pass

    def _make_bbox(self, lat: float, lon: float, buffer_m: float = 50) -> str:
        """Create a small bbox around a point for spatial query.

        buffer_m is approximate buffer in meters.
        At Dutch latitudes: 1 degree lat ~ 111km, 1 degree lon ~ 67km.
        """
        dlat = buffer_m / 111_000
        dlon = buffer_m / 67_000
        return f"{lon - dlon},{lat - dlat},{lon + dlon},{lat + dlat}"

    def _query_polygons(self, lat: float, lon: float) -> list:
        """Query the polygons collection with a bbox filter."""
        self._rate_limit()

        bbox = self._make_bbox(lat, lon)
        params = {
            "bbox": bbox,
            "f": "json",
            "limit": 20,
        }

        try:
            response = self.session.get(
                self.POLYGONS_URL,
                params=params,
                headers=self._get_headers(),
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("features", [])
        except requests.RequestException as e:
            logger.error(f"Error querying PDOK: {e}")
            return []

    def _load_gemeente_gezichten(self) -> list:
        """Load gemeentelijke beschermde stadsgezichten GeoJSON.

        Downloads once and caches in memory + on disk.
        Currently supports Den Haag; can be extended with more municipalities.
        """
        if PDOKBeschermdeGebiedenCollector._gemeente_gezichten is not None:
            return PDOKBeschermdeGebiedenCollector._gemeente_gezichten

        # Try loading from disk cache first
        disk_cache = self.cache_dir / "gemeente_gezichten.json" if self.cache_dir else None
        if disk_cache and disk_cache.exists():
            try:
                cache_age = datetime.now().timestamp() - disk_cache.stat().st_mtime
                if cache_age < 30 * 86400:  # 30 days
                    with open(disk_cache, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    PDOKBeschermdeGebiedenCollector._gemeente_gezichten = data.get("features", [])
                    logger.info(f"Loaded {len(PDOKBeschermdeGebiedenCollector._gemeente_gezichten)} gemeente gezichten from cache")
                    return PDOKBeschermdeGebiedenCollector._gemeente_gezichten
            except Exception:
                pass

        # Download from Den Haag open data
        features = []
        try:
            logger.info("Downloading gemeentelijke beschermde stadsgezichten (Den Haag)")
            response = self.session.get(
                self.DENHAAG_GEZICHTEN_URL,
                headers={"User-Agent": random.choice(USER_AGENTS)},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            features = data.get("features", [])

            # Save to disk cache
            if disk_cache:
                with open(disk_cache, "w", encoding="utf-8") as f:
                    json.dump(data, f)

            logger.info(f"Downloaded {len(features)} beschermde gezichten from Den Haag")
        except Exception as e:
            logger.error(f"Error downloading gemeente gezichten: {e}")

        PDOKBeschermdeGebiedenCollector._gemeente_gezichten = features
        return features

    def _check_gemeente_gezichten(self, lat: float, lon: float) -> Optional[Dict[str, str]]:
        """Check if point falls within a gemeentelijk beschermd stadsgezicht.

        Returns dict with naam, type, niveau if found, else None.
        """
        try:
            from shapely.geometry import Point, shape
        except ImportError:
            logger.warning("shapely not installed, skipping gemeente gezichten check")
            return None

        features = self._load_gemeente_gezichten()
        if not features:
            return None

        point = Point(lon, lat)

        for feat in features:
            props = feat.get("properties", {})
            type_str = props.get("TYPE", "")

            # Only check gemeentelijke gezichten (rijks ones are covered by PDOK RCE)
            if "Gemeentelijk" not in type_str:
                continue

            try:
                polygon = shape(feat["geometry"])
                if polygon.contains(point):
                    return {
                        "naam": props.get("Naam", ""),
                        "type": "stadsgezicht",
                        "niveau": "gemeentelijk",
                    }
            except Exception:
                continue

        return None

    def get_beschermd_gebied(
        self,
        latitude: float,
        longitude: float,
        use_cache: bool = True,
    ) -> BeschermdGebiedResult:
        """
        Check if coordinates fall within a protected area.

        Parameters
        ----------
        latitude : float
            WGS84 latitude
        longitude : float
            WGS84 longitude
        use_cache : bool
            Whether to use cached results (default: True)

        Returns
        -------
        BeschermdGebiedResult
            Protected area status
        """
        if use_cache:
            cached = self._load_from_cache(latitude, longitude)
            if cached:
                return cached

        result = BeschermdGebiedResult(latitude=latitude, longitude=longitude)

        try:
            logger.info(f"Checking beschermde gebieden for ({latitude}, {longitude})")

            # 1. Check rijksbeschermde gezichten + UNESCO via PDOK RCE
            features = self._query_polygons(latitude, longitude)

            for feat in features:
                props = feat.get("properties", {})
                namespace = props.get("namespace", "")
                naam = props.get("text", "")

                if namespace == self.NS_GEZICHTEN and not result.in_beschermd_gezicht:
                    result.in_beschermd_gezicht = True
                    result.gezicht_naam = naam
                    result.gezicht_niveau = "rijks"
                    result.gezicht_status = props.get("legalfoundationdate")

                    naam_lower = naam.lower()
                    if "dorp" in naam_lower:
                        result.gezicht_type = "dorpsgezicht"
                    else:
                        result.gezicht_type = "stadsgezicht"

                    logger.info(f"Found rijks beschermd {result.gezicht_type}: {naam}")

                elif namespace == self.NS_UNESCO and not result.in_unesco:
                    result.in_unesco = True
                    result.unesco_naam = naam
                    logger.info(f"Found UNESCO site: {naam}")

            # 2. If not in a rijks-beschermd gezicht, check gemeentelijke gezichten
            if not result.in_beschermd_gezicht:
                gem_result = self._check_gemeente_gezichten(latitude, longitude)
                if gem_result:
                    result.in_beschermd_gezicht = True
                    result.gezicht_naam = gem_result["naam"]
                    result.gezicht_type = gem_result["type"]
                    result.gezicht_niveau = gem_result["niveau"]
                    logger.info(f"Found gemeentelijk beschermd {result.gezicht_type}: {gem_result['naam']}")

        except Exception as e:
            logger.error(f"Error checking beschermde gebieden: {e}")
            result.error = f"Fout bij ophalen beschermde gebieden: {str(e)}"

        if use_cache:
            self._save_to_cache(result)

        return result


def create_pdok_beschermde_gebieden_collector(
    cache_dir: Optional[Path] = None,
) -> PDOKBeschermdeGebiedenCollector:
    """
    Factory function to create a PDOK Beschermde Gebieden collector.

    Parameters
    ----------
    cache_dir : Path, optional
        Cache directory. If None, uses data/cache/beschermde_gebieden.

    Returns
    -------
    PDOKBeschermdeGebiedenCollector
        Configured collector instance
    """
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "beschermde_gebieden"

    return PDOKBeschermdeGebiedenCollector(cache_dir=cache_dir)
