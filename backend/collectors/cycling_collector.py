"""Cycling route collector using OpenRouteService Directions API."""

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# Default cache directory
CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "cycling"


@dataclass
class CyclingRoute:
    """Result of a cycling route calculation."""
    origin_lat: float
    origin_lng: float
    dest_lat: float
    dest_lng: float
    dest_naam: str
    afstand_km: float
    reistijd_min: int
    geometry: Optional[List[List[float]]] = None  # decoded polyline as [[lng, lat], ...]
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "origin_lat": self.origin_lat,
            "origin_lng": self.origin_lng,
            "dest_lat": self.dest_lat,
            "dest_lng": self.dest_lng,
            "dest_naam": self.dest_naam,
            "afstand_km": self.afstand_km,
            "reistijd_min": self.reistijd_min,
            "geometry": self.geometry,
            "error": self.error,
        }


@dataclass
class CyclingCollector:
    """Collector for cycling routes via OpenRouteService."""

    api_key: str = ""
    cache_dir: Optional[Path] = None
    cache_days: int = 7
    min_delay: float = 1.0
    max_delay: float = 2.0
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, repr=False)

    def __post_init__(self):
        if not self.api_key:
            self.api_key = os.environ.get("ORS_API_KEY", "")
        if self.cache_dir is None:
            self.cache_dir = CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({
                "Authorization": self.api_key,
                "Accept": "application/json, application/geo+json",
            })

    def _rate_limit(self):
        """Enforce minimum delay between requests."""
        import random
        elapsed = time.time() - self._last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.time()

    def _cache_key(self, origin_lat: float, origin_lng: float, dest_lat: float, dest_lng: float) -> str:
        """Generate a cache key from coordinates."""
        raw = f"{origin_lat:.5f},{origin_lng:.5f}_{dest_lat:.5f},{dest_lng:.5f}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _load_from_cache(self, key: str) -> Optional[Dict]:
        """Load cached result if still valid."""
        cache_file = self.cache_dir / f"{key}.json"
        if not cache_file.exists():
            return None
        try:
            data = json.loads(cache_file.read_text())
            cached_time = data.get("_cached_at", 0)
            if time.time() - cached_time > self.cache_days * 86400:
                return None
            return data
        except (json.JSONDecodeError, KeyError):
            return None

    def _save_to_cache(self, key: str, data: Dict):
        """Save result to cache."""
        data["_cached_at"] = time.time()
        cache_file = self.cache_dir / f"{key}.json"
        cache_file.write_text(json.dumps(data, ensure_ascii=False))

    def _decode_polyline(self, encoded: str) -> List[List[float]]:
        """Decode Google-style encoded polyline to list of [lng, lat] pairs."""
        coords = []
        index = 0
        lat = 0
        lng = 0
        while index < len(encoded):
            # Decode latitude
            shift = 0
            result = 0
            while True:
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            lat += (~(result >> 1) if (result & 1) else (result >> 1))

            # Decode longitude
            shift = 0
            result = 0
            while True:
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            lng += (~(result >> 1) if (result & 1) else (result >> 1))

            coords.append([lng / 1e5, lat / 1e5])
        return coords

    def get_route(
        self,
        origin_lat: float,
        origin_lng: float,
        dest_lat: float,
        dest_lng: float,
        dest_naam: str = "",
    ) -> CyclingRoute:
        """Calculate cycling route between two points.

        Args:
            origin_lat, origin_lng: Origin coordinates
            dest_lat, dest_lng: Destination coordinates
            dest_naam: Human-readable destination name

        Returns:
            CyclingRoute with distance, time, and optional geometry
        """
        if not self.api_key:
            return CyclingRoute(
                origin_lat=origin_lat,
                origin_lng=origin_lng,
                dest_lat=dest_lat,
                dest_lng=dest_lng,
                dest_naam=dest_naam,
                afstand_km=0,
                reistijd_min=0,
                error="ORS_API_KEY niet geconfigureerd",
            )

        cache_key = self._cache_key(origin_lat, origin_lng, dest_lat, dest_lng)
        cached = self._load_from_cache(cache_key)
        if cached:
            return CyclingRoute(
                origin_lat=origin_lat,
                origin_lng=origin_lng,
                dest_lat=dest_lat,
                dest_lng=dest_lng,
                dest_naam=dest_naam,
                afstand_km=cached["afstand_km"],
                reistijd_min=cached["reistijd_min"],
                geometry=cached.get("geometry"),
            )

        self._rate_limit()

        try:
            # ORS expects coordinates as [lng, lat]
            url = "https://api.openrouteservice.org/v2/directions/cycling-regular"
            params = {
                "start": f"{origin_lng},{origin_lat}",
                "end": f"{dest_lng},{dest_lat}",
            }

            resp = self.session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            features = data.get("features", [])
            if not features:
                return CyclingRoute(
                    origin_lat=origin_lat,
                    origin_lng=origin_lng,
                    dest_lat=dest_lat,
                    dest_lng=dest_lng,
                    dest_naam=dest_naam,
                    afstand_km=0,
                    reistijd_min=0,
                    error="Geen route gevonden",
                )

            segment = features[0]["properties"]["segments"][0]
            afstand_km = round(segment["distance"] / 1000, 1)
            reistijd_min = round(segment["duration"] / 60)

            # Extract geometry (GeoJSON LineString coordinates)
            geometry = features[0]["geometry"].get("coordinates")

            result_data = {
                "afstand_km": afstand_km,
                "reistijd_min": reistijd_min,
                "geometry": geometry,
            }
            self._save_to_cache(cache_key, result_data)

            return CyclingRoute(
                origin_lat=origin_lat,
                origin_lng=origin_lng,
                dest_lat=dest_lat,
                dest_lng=dest_lng,
                dest_naam=dest_naam,
                afstand_km=afstand_km,
                reistijd_min=reistijd_min,
                geometry=geometry,
            )

        except requests.RequestException as exc:
            logger.warning("ORS cycling route request failed: %s", exc)
            return CyclingRoute(
                origin_lat=origin_lat,
                origin_lng=origin_lng,
                dest_lat=dest_lat,
                dest_lng=dest_lng,
                dest_naam=dest_naam,
                afstand_km=0,
                reistijd_min=0,
                error=str(exc),
            )
        except (KeyError, IndexError) as exc:
            logger.warning("ORS response parsing failed: %s", exc)
            return CyclingRoute(
                origin_lat=origin_lat,
                origin_lng=origin_lng,
                dest_lat=dest_lat,
                dest_lng=dest_lng,
                dest_naam=dest_naam,
                afstand_km=0,
                reistijd_min=0,
                error=f"Onverwacht API response formaat: {exc}",
            )

    def get_routes_to_werklocaties(
        self,
        origin_lat: float,
        origin_lng: float,
        werklocaties: List[Dict[str, Any]],
    ) -> List[CyclingRoute]:
        """Calculate cycling routes from origin to all werklocaties.

        Args:
            origin_lat, origin_lng: Origin coordinates
            werklocaties: List of dicts with 'naam', 'lat', 'lng'

        Returns:
            List of CyclingRoute results
        """
        routes = []
        for wl in werklocaties:
            route = self.get_route(
                origin_lat=origin_lat,
                origin_lng=origin_lng,
                dest_lat=wl["lat"],
                dest_lng=wl["lng"],
                dest_naam=wl["naam"],
            )
            routes.append(route)
        return routes


def create_cycling_collector(cache_dir: Optional[Path] = None) -> CyclingCollector:
    """Factory function with default cache directory."""
    if cache_dir is None:
        cache_dir = CACHE_DIR
    return CyclingCollector(cache_dir=cache_dir)
