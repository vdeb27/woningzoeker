"""
RIVM Atlas Leefomgeving Collector.

Fetches environmental quality data via WMS GetFeatureInfo requests
for neighborhood centroids. Data includes:
- Air quality: NO2, PM2.5, PM10
- Noise levels: road, rail, aviation
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import requests


# RIVM Atlas WMS endpoint
RIVM_WMS_URL = "https://geodata.rivm.nl/geoserver/wms"

# Layers for environmental indicators
RIVM_LAYERS = {
    "no2_concentratie": "nsl:no2_concentratie_jaargemiddelde",
    "pm25_concentratie": "nsl:pm25_concentratie_jaargemiddelde",
    "pm10_concentratie": "nsl:pm10_concentratie_jaargemiddelde",
    "geluid_weg_lden": "gm:geluid_weg_lden",
    "geluid_rail_lden": "gm:geluid_rail_lden",
}

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "rivm"
CACHE_DURATION_SECONDS = 30 * 24 * 60 * 60  # 30 days

# Centroids of target municipalities (approx) for initial testing
# In production, use actual buurt centroids calculated from geometrie
DEFAULT_CENTROIDS = {
    "0518": (52.07, 4.30),   # Den Haag
    "1916": (52.08, 4.38),   # Leidschendam-Voorburg
    "0603": (52.04, 4.33),   # Rijswijk
}


@dataclass
class RIVMResult:
    """Environmental quality data for a location."""
    buurt_code: str
    no2_concentratie: Optional[float] = None  # µg/m³
    pm25_concentratie: Optional[float] = None  # µg/m³
    pm10_concentratie: Optional[float] = None  # µg/m³
    geluid_weg_lden: Optional[float] = None  # dB
    geluid_rail_lden: Optional[float] = None  # dB

    def to_dict(self) -> Dict[str, Any]:
        return {
            "buurt_code": self.buurt_code,
            "no2_concentratie": self.no2_concentratie,
            "pm25_concentratie": self.pm25_concentratie,
            "pm10_concentratie": self.pm10_concentratie,
            "geluid_weg_lden": self.geluid_weg_lden,
            "geluid_rail_lden": self.geluid_rail_lden,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RIVMResult":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class RIVMCollector:
    """
    Collector for RIVM Atlas Leefomgeving environmental data.

    Uses WMS GetFeatureInfo to query environmental indicators
    at buurt centroid locations.
    """

    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    min_delay: float = 0.5  # Rate limit between WMS requests
    _data: Dict[str, RIVMResult] = field(default_factory=dict, init=False, repr=False)
    _loaded: bool = field(default=False, init=False, repr=False)
    _last_request: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self) -> Path:
        return self.cache_dir / "rivm_data.json"

    def _rate_limit(self) -> None:
        elapsed = time.time() - self._last_request
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
        self._last_request = time.time()

    def _load_from_cache(self) -> bool:
        cache_path = self._cache_path()
        if not cache_path.exists():
            return False
        try:
            with cache_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("timestamp", 0) + CACHE_DURATION_SECONDS < time.time():
                return False
            for code, entry in data.get("buurten", {}).items():
                self._data[code] = RIVMResult.from_dict(entry)
            self._loaded = True
            return True
        except (json.JSONDecodeError, IOError, TypeError):
            return False

    def _save_to_cache(self) -> None:
        cache_data = {
            "timestamp": time.time(),
            "buurten": {code: r.to_dict() for code, r in self._data.items()},
        }
        try:
            with self._cache_path().open("w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False)
        except IOError:
            pass

    def _query_wms_point(
        self, layer: str, lat: float, lon: float
    ) -> Optional[float]:
        """Query a single WMS layer at a point using GetFeatureInfo."""
        self._rate_limit()

        # Convert lat/lon to pixel coordinates in a small bbox
        # Use a small bounding box around the point
        delta = 0.0005
        bbox = f"{lon - delta},{lat - delta},{lon + delta},{lat + delta}"

        params = {
            "SERVICE": "WMS",
            "VERSION": "1.1.1",
            "REQUEST": "GetFeatureInfo",
            "LAYERS": layer,
            "QUERY_LAYERS": layer,
            "INFO_FORMAT": "application/json",
            "SRS": "EPSG:4326",
            "BBOX": bbox,
            "WIDTH": 101,
            "HEIGHT": 101,
            "X": 50,
            "Y": 50,
        }

        try:
            response = requests.get(RIVM_WMS_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            if features:
                props = features[0].get("properties", {})
                # Try common field names
                for key in ["GRAY_INDEX", "value", "pixel_value", "concentratie", "lden"]:
                    if key in props and props[key] is not None:
                        try:
                            return float(props[key])
                        except (ValueError, TypeError):
                            pass
                # Try first numeric property
                for val in props.values():
                    if val is not None:
                        try:
                            return float(val)
                        except (ValueError, TypeError):
                            continue
        except (requests.RequestException, json.JSONDecodeError):
            pass

        return None

    def fetch_for_buurt(self, buurt_code: str, lat: float, lon: float) -> RIVMResult:
        """Fetch RIVM data for a specific buurt centroid."""
        result = RIVMResult(buurt_code=buurt_code)

        for indicator, layer in RIVM_LAYERS.items():
            value = self._query_wms_point(layer, lat, lon)
            if value is not None:
                setattr(result, indicator, value)

        self._data[buurt_code] = result
        return result

    def fetch_for_centroids(self, centroids: Dict[str, tuple]) -> None:
        """
        Fetch RIVM data for multiple buurt centroids.

        Parameters
        ----------
        centroids : dict
            Mapping of buurt_code -> (lat, lon)
        """
        for buurt_code, (lat, lon) in centroids.items():
            if buurt_code not in self._data:
                self.fetch_for_buurt(buurt_code, lat, lon)

        self._save_to_cache()

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        self._load_from_cache()
        self._loaded = True

    def get_buurt(self, buurt_code: str) -> Optional[RIVMResult]:
        """Get RIVM data for a buurt (from cache only)."""
        self._ensure_loaded()
        code = buurt_code.upper().strip()
        if not code.startswith("BU"):
            code = f"BU{code}"
        return self._data.get(code)

    def get_all(self) -> Dict[str, RIVMResult]:
        """Get all RIVM data."""
        self._ensure_loaded()
        return dict(self._data)


def create_rivm_collector(cache_dir: Optional[Path] = None) -> RIVMCollector:
    """Factory function with default cache directory."""
    if cache_dir is None:
        cache_dir = Path(__file__).parent.parent.parent / "data" / "cache" / "rivm"
    return RIVMCollector(cache_dir=cache_dir)
