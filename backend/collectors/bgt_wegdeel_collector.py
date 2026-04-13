"""
BGT wegdeel collector voor voorkant-detectie.

Haalt wegdeel-polygonen op uit de BGT (Basisregistratie Grootschalige
Topografie) om te bepalen welke kant van een gebouw de straatkant is.
Gebruikt voor tuinoriëntatie berekening.
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

BGT_WEGDEEL_URL = "https://api.pdok.nl/lv/bgt/ogc/v1/collections/wegdeel/items"

# Relevante wegdeel functies voor voorkant-detectie
RELEVANT_FUNCTIES = {
    "rijbaan lokale weg",
    "rijbaan regionale weg",
    "woonerf",
    "voetpad",
    "inrit",
}


@dataclass
class BgtWegdeelCollector:
    """Collector voor BGT wegdeel-polygonen.

    Haalt straten, voetpaden en inritten op om te bepalen welke kant
    van een woning de straatkant (voorkant) is.
    """

    min_delay: float = 1.0
    max_delay: float = 2.0
    cache_dir: Optional[Path] = None
    cache_days: int = 365
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

    def get_roads(
        self,
        rd_x: float,
        rd_y: float,
        radius: float = 50.0,
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """Haal wegdeel-polygonen op rond een locatie.

        Parameters
        ----------
        rd_x, rd_y : float
            Middelpunt in RD-coördinaten (EPSG:28992)
        radius : float
            Zoekradius in meters (default: 50m)
        use_cache : bool
            Of gecachte resultaten gebruikt mogen worden

        Returns
        -------
        list of dict
            Elke dict bevat: polygon_coords (list of coord pairs in RD),
            functie (str)
        """
        cache_key = f"wegdeel_{int(rd_x)}_{int(rd_y)}_{int(radius)}"

        if use_cache and self.cache_dir:
            cached = self._load_from_cache(cache_key)
            if cached is not None:
                return cached

        roads = self._fetch_roads(rd_x, rd_y, radius)

        self._save_to_cache(cache_key, roads)
        return roads

    def _fetch_roads(
        self, rd_x: float, rd_y: float, radius: float
    ) -> List[Dict[str, Any]]:
        """Haal wegdelen op via BGT OGC API."""
        try:
            from pyproj import Transformer

            transformer_to_wgs = Transformer.from_crs(
                "EPSG:28992", "EPSG:4326", always_xy=True
            )
            lon_min, lat_min = transformer_to_wgs.transform(
                rd_x - radius, rd_y - radius
            )
            lon_max, lat_max = transformer_to_wgs.transform(
                rd_x + radius, rd_y + radius
            )
        except ImportError:
            logger.error("pyproj niet beschikbaar voor coördinaattransformatie")
            return []

        self._rate_limit()

        try:
            params = {
                "bbox": f"{lon_min},{lat_min},{lon_max},{lat_max}",
                "limit": 200,
            }
            response = self.session.get(
                BGT_WEGDEEL_URL,
                params=params,
                headers={"User-Agent": random.choice(USER_AGENTS)},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            transformer_to_rd = Transformer.from_crs(
                "EPSG:4326", "EPSG:28992", always_xy=True
            )

            roads = []
            for feat in data.get("features", []):
                props = feat.get("properties", {})
                functie = props.get("functie", "")
                status = props.get("status", "")

                if status != "bestaand":
                    continue
                if functie not in RELEVANT_FUNCTIES:
                    continue

                geom = feat.get("geometry", {})
                geom_type = geom.get("type", "")
                coords_raw = geom.get("coordinates", [])

                if not coords_raw:
                    continue

                # Convert WGS84 polygon coords to RD
                rd_coords = self._convert_polygon_to_rd(
                    geom_type, coords_raw, transformer_to_rd
                )
                if rd_coords:
                    roads.append({
                        "polygon_coords": rd_coords,
                        "functie": functie,
                    })

            logger.info(
                f"BGT: {len(roads)} wegdelen gevonden in straal {radius}m"
            )
            return roads

        except requests.RequestException as e:
            logger.error(f"Error fetching BGT wegdeel data: {e}")
            return []

    @staticmethod
    def _convert_polygon_to_rd(
        geom_type: str,
        coords_raw: list,
        transformer,
    ) -> Optional[List[List[float]]]:
        """Convert GeoJSON polygon coordinates to RD."""
        try:
            if geom_type == "Polygon":
                # coords_raw = [ring1, ring2, ...], ring = [[lon, lat], ...]
                if not coords_raw or not coords_raw[0]:
                    return None
                exterior = coords_raw[0]
                return [
                    list(transformer.transform(c[0], c[1]))
                    for c in exterior
                ]
            elif geom_type == "MultiPolygon":
                # Take the first polygon (largest is typically the main one)
                if not coords_raw or not coords_raw[0] or not coords_raw[0][0]:
                    return None
                exterior = coords_raw[0][0]
                return [
                    list(transformer.transform(c[0], c[1]))
                    for c in exterior
                ]
        except (IndexError, TypeError, ValueError) as e:
            logger.debug(f"Polygon conversie mislukt: {e}")
        return None

    def _load_from_cache(self, cache_key: str) -> Optional[List[Dict[str, Any]]]:
        if not self.cache_dir:
            return None
        cache_path = self.cache_dir / f"{cache_key}.json"
        if not cache_path.exists():
            return None
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                cached = json.load(f)
            cache_date = datetime.fromisoformat(cached.get("fetch_date", ""))
            if datetime.now() - cache_date < timedelta(days=self.cache_days):
                return cached.get("roads", [])
        except (json.JSONDecodeError, ValueError, IOError):
            pass
        return None

    def _save_to_cache(self, cache_key: str, roads: List[Dict[str, Any]]) -> None:
        if not self.cache_dir:
            return
        cache_path = self.cache_dir / f"{cache_key}.json"
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(
                    {"fetch_date": datetime.now().isoformat(), "roads": roads},
                    f,
                    ensure_ascii=False,
                )
        except IOError:
            pass


def create_bgt_wegdeel_collector(
    cache_dir: Optional[Path] = None,
) -> BgtWegdeelCollector:
    """Factory function voor BGT wegdeel collector."""
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "bgt_wegdelen"
    return BgtWegdeelCollector(cache_dir=cache_dir)
