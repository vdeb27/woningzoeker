"""
OSM Overpass Collector.

Fetches nearby amenities/facilities from OpenStreetMap via the Overpass API.
Returns specific locations (supermarkets, doctors, restaurants, etc.) with
name, type, category, distance, and coordinates.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "osm_overpass"
CACHE_DURATION_SECONDS = 7 * 24 * 60 * 60  # 7 days

# OSM tag categories for the Overpass query
OSM_CATEGORIES: Dict[str, List[Dict[str, str]]] = {
    "dagelijks": [
        {"tag": "shop", "value": "supermarket", "label": "Supermarkt"},
        {"tag": "shop", "value": "bakery", "label": "Bakker"},
    ],
    "winkels": [
        {"tag": "shop", "value": "mall", "label": "Winkelcentrum"},
        {"tag": "shop", "value": "doityourself", "label": "Bouwmarkt"},
    ],
    "horeca": [
        {"tag": "amenity", "value": "restaurant", "label": "Restaurant"},
        {"tag": "amenity", "value": "cafe", "label": "Cafe"},
    ],
    "zorg": [
        {"tag": "amenity", "value": "doctors", "label": "Huisarts"},
        {"tag": "amenity", "value": "pharmacy", "label": "Apotheek"},
        {"tag": "amenity", "value": "hospital", "label": "Ziekenhuis"},
    ],
    "sport": [
        {"tag": "leisure", "value": "fitness_centre", "label": "Sportschool"},
        {"tag": "leisure", "value": "swimming_pool", "label": "Zwembad"},
    ],
    "cultuur": [
        {"tag": "amenity", "value": "library", "label": "Bibliotheek"},
        {"tag": "amenity", "value": "cinema", "label": "Bioscoop"},
    ],
}


def _haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate distance in meters between two coordinates using Haversine formula."""
    R = 6371000  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def _build_overpass_query(lat: float, lng: float, radius_m: int) -> str:
    """Build a combined Overpass QL query for all categories."""
    parts = []
    for entries in OSM_CATEGORIES.values():
        for entry in entries:
            tag = entry["tag"]
            value = entry["value"]
            parts.append(f'  node["{tag}"="{value}"](around:{radius_m},{lat},{lng});')
            parts.append(f'  way["{tag}"="{value}"](around:{radius_m},{lat},{lng});')

    query = f"""[out:json][timeout:25];
(
{chr(10).join(parts)}
);
out center;"""
    return query


def _categorize_element(tags: Dict[str, str]) -> Optional[Dict[str, str]]:
    """Determine category, type, and label for an OSM element based on its tags."""
    for categorie, entries in OSM_CATEGORIES.items():
        for entry in entries:
            if tags.get(entry["tag"]) == entry["value"]:
                return {
                    "categorie": categorie,
                    "type": entry["value"],
                    "label": entry["label"],
                }
    return None


@dataclass
class Voorziening:
    """A single nearby facility/amenity."""
    naam: str
    type: str           # OSM tag value (e.g. "supermarket")
    categorie: str      # Category (e.g. "dagelijks")
    afstand_m: int
    lat: float
    lng: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "naam": self.naam,
            "type": self.type,
            "categorie": self.categorie,
            "afstand_m": self.afstand_m,
            "lat": self.lat,
            "lng": self.lng,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Voorziening":
        return cls(
            naam=data["naam"],
            type=data["type"],
            categorie=data["categorie"],
            afstand_m=data["afstand_m"],
            lat=data["lat"],
            lng=data["lng"],
        )


@dataclass
class OverpassResult:
    """Result of an Overpass query: list of nearby facilities."""
    lat: float
    lng: float
    radius_m: int
    voorzieningen: List[Voorziening] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "lat": self.lat,
            "lng": self.lng,
            "radius_m": self.radius_m,
            "voorzieningen": [v.to_dict() for v in self.voorzieningen],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OverpassResult":
        return cls(
            lat=data["lat"],
            lng=data["lng"],
            radius_m=data["radius_m"],
            voorzieningen=[Voorziening.from_dict(v) for v in data.get("voorzieningen", [])],
        )


@dataclass
class OSMOverpassCollector:
    """Collector for nearby facilities via OSM Overpass API."""

    min_delay: float = 2.0
    max_delay: float = 3.0
    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    cache_days: int = 7
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "Woningzoeker/1.0 (housing search tool)",
            })

    def _rate_limit(self) -> None:
        elapsed = time.time() - self._last_request
        if elapsed < self.min_delay:
            import random
            delay = random.uniform(self.min_delay, self.max_delay)
            time.sleep(delay - elapsed)
        self._last_request = time.time()

    def _cache_key(self, lat: float, lng: float, radius_m: int) -> str:
        """Generate cache key from rounded coordinates."""
        lat_r = round(lat, 3)
        lng_r = round(lng, 3)
        raw = f"{lat_r}_{lng_r}_{radius_m}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _load_from_cache(self, key: str) -> Optional[OverpassResult]:
        cache_file = self.cache_dir / f"{key}.json"
        if not cache_file.exists():
            return None
        try:
            with cache_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("timestamp", 0) + (self.cache_days * 86400) < time.time():
                return None
            return OverpassResult.from_dict(data["result"])
        except (json.JSONDecodeError, IOError, KeyError, TypeError):
            return None

    def _save_to_cache(self, key: str, result: OverpassResult) -> None:
        cache_file = self.cache_dir / f"{key}.json"
        try:
            with cache_file.open("w", encoding="utf-8") as f:
                json.dump({"timestamp": time.time(), "result": result.to_dict()}, f, ensure_ascii=False)
        except IOError:
            pass

    def get_voorzieningen(self, lat: float, lng: float, radius_m: int = 1500) -> OverpassResult:
        """Fetch nearby facilities for given coordinates.

        Args:
            lat: Latitude of the location.
            lng: Longitude of the location.
            radius_m: Search radius in meters (default 1500).

        Returns:
            OverpassResult with list of Voorziening items sorted by distance.
        """
        cache_key = self._cache_key(lat, lng, radius_m)
        cached = self._load_from_cache(cache_key)
        if cached is not None:
            return cached

        self._rate_limit()

        query = _build_overpass_query(lat, lng, radius_m)
        try:
            resp = self.session.post(
                OVERPASS_URL,
                data={"data": query},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError):
            # Graceful degradation: return empty result
            return OverpassResult(lat=lat, lng=lng, radius_m=radius_m)

        voorzieningen: List[Voorziening] = []
        seen = set()  # Deduplicate by (type, name, rounded coords)

        for element in data.get("elements", []):
            tags = element.get("tags", {})
            cat_info = _categorize_element(tags)
            if cat_info is None:
                continue

            # Get coordinates (node has lat/lon directly, way has center)
            if element.get("type") == "way":
                center = element.get("center", {})
                e_lat = center.get("lat")
                e_lng = center.get("lon")
            else:
                e_lat = element.get("lat")
                e_lng = element.get("lon")

            if e_lat is None or e_lng is None:
                continue

            naam = tags.get("name", cat_info["label"])
            dedup_key = (cat_info["type"], naam, round(e_lat, 4), round(e_lng, 4))
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            afstand = int(_haversine(lat, lng, e_lat, e_lng))

            voorzieningen.append(Voorziening(
                naam=naam,
                type=cat_info["type"],
                categorie=cat_info["categorie"],
                afstand_m=afstand,
                lat=round(e_lat, 6),
                lng=round(e_lng, 6),
            ))

        voorzieningen.sort(key=lambda v: v.afstand_m)

        result = OverpassResult(
            lat=lat,
            lng=lng,
            radius_m=radius_m,
            voorzieningen=voorzieningen,
        )
        self._save_to_cache(cache_key, result)
        return result


def create_osm_overpass_collector(cache_dir: Optional[Path] = None) -> OSMOverpassCollector:
    """Factory function with default cache directory."""
    if cache_dir is None:
        cache_dir = Path(__file__).parent.parent.parent / "data" / "cache" / "osm_overpass"
    return OSMOverpassCollector(cache_dir=cache_dir)
