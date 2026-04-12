"""
Perceelgrens collector via PDOK Kadastrale Kaart WFS.

Fetches cadastral parcel boundaries from the PDOK WFS service.
Used to determine exact garden shape and area by subtracting
the building footprint from the parcel polygon.
"""

from __future__ import annotations

import json
import logging
import random
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
]

WFS_URL = "https://service.pdok.nl/kadaster/kadastralekaart/wfs/v5_0"


@dataclass
class PerceelgrensResult:
    """Result from PDOK Kadastrale Kaart WFS lookup."""

    perceel_polygon_rd: Optional[List[List[float]]] = None
    perceeloppervlakte: Optional[float] = None
    kadastrale_aanduiding: Optional[str] = None
    fetch_date: datetime = field(default_factory=datetime.now)
    source: str = "pdok_kadaster"
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "perceel_polygon_rd": self.perceel_polygon_rd,
            "perceeloppervlakte": self.perceeloppervlakte,
            "kadastrale_aanduiding": self.kadastrale_aanduiding,
            "fetch_date": self.fetch_date.isoformat(),
            "source": self.source,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PerceelgrensResult":
        fetch_date = data.get("fetch_date")
        if isinstance(fetch_date, str):
            fetch_date = datetime.fromisoformat(fetch_date)
        elif fetch_date is None:
            fetch_date = datetime.now()

        return cls(
            perceel_polygon_rd=data.get("perceel_polygon_rd"),
            perceeloppervlakte=data.get("perceeloppervlakte"),
            kadastrale_aanduiding=data.get("kadastrale_aanduiding"),
            fetch_date=fetch_date,
            source=data.get("source", "pdok_kadaster"),
            error=data.get("error"),
        )


@dataclass
class PerceelgrensCollector:
    """Collector for cadastral parcel boundaries from PDOK.

    Uses the Kadastrale Kaart WFS v5_0 service which is free
    and requires no API key.

    Parameters
    ----------
    min_delay : float
        Minimum delay between requests in seconds (default: 1.0)
    max_delay : float
        Maximum delay between requests in seconds (default: 2.0)
    cache_dir : Path, optional
        Directory for caching results
    cache_days : int
        Number of days to cache results (default: 90)
    """

    min_delay: float = 1.0
    max_delay: float = 2.0
    cache_dir: Optional[Path] = None
    cache_days: int = 90
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _rate_limit(self) -> None:
        now = time.perf_counter()
        elapsed = now - self._last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.perf_counter()

    def _load_from_cache(self, cache_key: str) -> Optional[PerceelgrensResult]:
        if not self.cache_dir:
            return None
        cache_path = self.cache_dir / f"{cache_key}.json"
        if not cache_path.exists():
            return None
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            result = PerceelgrensResult.from_dict(data)
            if datetime.now() - result.fetch_date > timedelta(days=self.cache_days):
                return None
            return result
        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    def _save_to_cache(self, cache_key: str, result: PerceelgrensResult) -> None:
        if not self.cache_dir:
            return
        cache_path = self.cache_dir / f"{cache_key}.json"
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)
        except IOError:
            pass

    def get_perceel(
        self,
        rd_x: float,
        rd_y: float,
        building_footprint_rd: Optional[List[List[float]]] = None,
        use_cache: bool = True,
    ) -> PerceelgrensResult:
        """Fetch the cadastral parcel containing a building.

        Parameters
        ----------
        rd_x, rd_y : float
            Building centroid in RD coordinates (EPSG:28992)
        building_footprint_rd : list, optional
            Building footprint polygon for matching the correct parcel
        use_cache : bool
            Whether to use cached results

        Returns
        -------
        PerceelgrensResult
            Parcel boundary polygon
        """
        cache_key = f"perceel_{int(rd_x)}_{int(rd_y)}"

        if use_cache:
            cached = self._load_from_cache(cache_key)
            if cached:
                return cached

        result = PerceelgrensResult()
        search_radius = 50  # meters

        try:
            self._rate_limit()

            params = {
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeName": "kadastralekaart:Perceel",
                "outputFormat": "application/json",
                "count": 20,
                "bbox": (
                    f"{rd_x - search_radius},{rd_y - search_radius},"
                    f"{rd_x + search_radius},{rd_y + search_radius},"
                    "EPSG:28992"
                ),
            }

            response = self.session.get(
                WFS_URL,
                params=params,
                headers={"User-Agent": random.choice(USER_AGENTS)},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            if not features:
                result.error = "Geen percelen gevonden"
                self._save_to_cache(cache_key, result)
                return result

            # Find the parcel that contains the building point
            best_feature = self._find_matching_parcel(
                features, rd_x, rd_y, building_footprint_rd
            )

            if best_feature:
                geom = best_feature.get("geometry", {})
                props = best_feature.get("properties", {})

                # Extract polygon coordinates
                coords = geom.get("coordinates", [])
                if geom.get("type") == "Polygon" and coords:
                    result.perceel_polygon_rd = [
                        [round(c[0], 3), round(c[1], 3)] for c in coords[0]
                    ]
                elif geom.get("type") == "MultiPolygon" and coords:
                    # Use the largest polygon
                    largest = max(coords, key=lambda p: len(p[0]))
                    result.perceel_polygon_rd = [
                        [round(c[0], 3), round(c[1], 3)] for c in largest[0]
                    ]

                result.perceeloppervlakte = props.get("kadastraleGrootteWaarde")
                sectie = props.get("sectie", "")
                nummer = props.get("perceelnummer", "")
                gemeente = props.get("kadastraleGemeenteWaarde", "")
                if sectie and nummer:
                    result.kadastrale_aanduiding = f"{gemeente} {sectie} {nummer}"

                logger.info(
                    f"Perceel gevonden: {result.kadastrale_aanduiding}, "
                    f"opp={result.perceeloppervlakte}m²"
                )
            else:
                result.error = "Geen passend perceel gevonden voor gebouw"

        except requests.RequestException as e:
            logger.error(f"Error fetching perceel data: {e}")
            result.error = f"Fout bij ophalen perceeldata: {str(e)}"

        self._save_to_cache(cache_key, result)
        return result

    @staticmethod
    def _find_matching_parcel(
        features: List[Dict[str, Any]],
        rd_x: float,
        rd_y: float,
        building_footprint_rd: Optional[List[List[float]]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Find the parcel that contains the building.

        Uses shapely for point-in-polygon and intersection tests.
        Falls back to nearest parcel if no containment match.
        """
        try:
            from shapely.geometry import Point, Polygon, shape

            point = Point(rd_x, rd_y)

            # Try point-in-polygon first
            for feat in features:
                geom = feat.get("geometry")
                if not geom:
                    continue
                try:
                    poly = shape(geom)
                    if poly.contains(point):
                        return feat
                except Exception:
                    continue

            # If building footprint provided, try intersection
            if building_footprint_rd:
                building_poly = Polygon(building_footprint_rd)
                best_overlap = 0.0
                best_feat = None
                for feat in features:
                    geom = feat.get("geometry")
                    if not geom:
                        continue
                    try:
                        poly = shape(geom)
                        overlap = poly.intersection(building_poly).area
                        if overlap > best_overlap:
                            best_overlap = overlap
                            best_feat = feat
                    except Exception:
                        continue
                if best_feat:
                    return best_feat

            # Fallback: nearest parcel centroid
            best_dist = float("inf")
            best_feat = None
            for feat in features:
                geom = feat.get("geometry")
                if not geom:
                    continue
                try:
                    poly = shape(geom)
                    dist = point.distance(poly.centroid)
                    if dist < best_dist:
                        best_dist = dist
                        best_feat = feat
                except Exception:
                    continue
            return best_feat

        except ImportError:
            # Without shapely, return the first feature
            return features[0] if features else None


def create_perceelgrens_collector(
    cache_dir: Optional[Path] = None,
) -> PerceelgrensCollector:
    """Factory function to create a perceelgrens collector."""
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "perceel"
    return PerceelgrensCollector(cache_dir=cache_dir)
