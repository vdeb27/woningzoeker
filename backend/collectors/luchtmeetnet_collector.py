"""
Luchtmeetnet Collector.

Haalt luchtkwaliteitsmetingen op van het Luchtmeetnet (RIVM) voor meetstations
in de regio Den Haag. Berekent jaargemiddelden voor NO2, PM10, PM2.5 en O3.

Bron: https://api.luchtmeetnet.nl/open_api/
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from utils.geo import haversine_km


API_BASE = "https://api.luchtmeetnet.nl/open_api"

# Den Haag meetstations met hun coördinaten en beschikbare stoffen
STATIONS = {
    "NL10446": {
        "naam": "Den Haag-Bleriotlaan",
        "type": "municipal",
        "lat": 52.039023,
        "lng": 4.359376,
        "formulas": ["NO2", "PM10", "O3"],
    },
    "NL10450": {
        "naam": "Den Haag-Neherkade",
        "type": "traffic",
        "lat": 52.062537,
        "lng": 4.318551,
        "formulas": ["NO2", "PM10", "PM25", "O3"],
    },
    "NL10445": {
        "naam": "Den Haag-Amsterdamse Veerkade",
        "type": "traffic",
        "lat": 52.075071,
        "lng": 4.315872,
        "formulas": ["NO2", "PM10"],
    },
    "NL10404": {
        "naam": "Den Haag-Rebecquestraat",
        "type": "municipal",
        "lat": 52.077148,
        "lng": 4.289185,
        "formulas": ["NO2", "PM10", "PM25", "O3"],
    },
}

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "luchtmeetnet"
CACHE_DURATION_SECONDS = 7 * 24 * 60 * 60  # 7 dagen


@dataclass
class LuchtmeetnetResult:
    """Luchtkwaliteitsdata van het dichtstbijzijnde meetstation."""

    station_number: str
    station_naam: str
    station_type: str  # "municipal" of "traffic"
    station_lat: float
    station_lng: float
    distance_km: float
    within_max_distance: bool
    # Jaargemiddelden in µg/m³ (None als stof niet gemeten wordt)
    no2_avg: Optional[float] = None
    pm10_avg: Optional[float] = None
    pm25_avg: Optional[float] = None
    o3_avg: Optional[float] = None
    # Metadata
    jaar: Optional[int] = None
    meetdagen: Optional[int] = None  # aantal dagen met metingen

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LuchtmeetnetResult":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class LuchtmeetnetCollector:
    """Collector voor luchtkwaliteitsmetingen van Luchtmeetnet."""

    max_distance_km: float = 4.0
    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    _station_averages: Dict[str, Dict[str, float]] = field(
        default_factory=dict, init=False, repr=False
    )
    _loaded: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self) -> Path:
        return self.cache_dir / "station_averages.json"

    def _load_from_cache(self) -> bool:
        cache_path = self._cache_path()
        if not cache_path.exists():
            return False
        try:
            with cache_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("timestamp", 0) + CACHE_DURATION_SECONDS < time.time():
                return False
            self._station_averages = data.get("stations", {})
            self._loaded = True
            return True
        except (json.JSONDecodeError, IOError, TypeError):
            return False

    def _save_to_cache(self) -> None:
        cache_data = {
            "timestamp": time.time(),
            "stations": self._station_averages,
        }
        try:
            with self._cache_path().open("w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False)
        except IOError:
            pass

    def _fetch_yearly_average(
        self, station_number: str, formula: str
    ) -> Optional[float]:
        """Haal jaargemiddelde op voor een station en stof.

        Fetcht alle uurmetingen van het afgelopen kalenderjaar en berekent
        het gemiddelde. Pagineert door alle resultaten.
        """
        now = datetime.now(timezone.utc)
        year = now.year - 1  # Gebruik vorig kalenderjaar (compleet)
        start = f"{year}-01-01T00:00:00Z"
        end = f"{year}-12-31T23:59:59Z"

        all_values: List[float] = []
        page = 1

        while True:
            params = {
                "station_number": station_number,
                "formula": formula,
                "page": page,
                "order_by": "timestamp_measured",
                "order_direction": "asc",
                "start": start,
                "end": end,
            }
            try:
                resp = requests.get(
                    f"{API_BASE}/measurements",
                    params=params,
                    timeout=30,
                    allow_redirects=True,
                )
                resp.raise_for_status()
                data = resp.json()
            except (requests.RequestException, ValueError) as e:
                print(f"  Luchtmeetnet fout {station_number}/{formula} p{page}: {e}")
                break

            measurements = data.get("data", [])
            if not measurements:
                break

            for m in measurements:
                val = m.get("value")
                if val is not None:
                    try:
                        all_values.append(float(val))
                    except (ValueError, TypeError):
                        pass

            # Check paginering
            pagination = data.get("pagination", {})
            last_page = pagination.get("last_page", page)
            if page >= last_page:
                break
            page += 1
            time.sleep(0.2)  # Rate limiting

        if not all_values:
            return None
        return round(sum(all_values) / len(all_values), 2)

    def _fetch_all_stations(self) -> None:
        """Haal jaargemiddelden op voor alle stations en stoffen."""
        for station_id, info in STATIONS.items():
            print(f"  Luchtmeetnet: {info['naam']}...")
            station_data: Dict[str, Any] = {"jaar": datetime.now().year - 1}

            for formula in info["formulas"]:
                avg = self._fetch_yearly_average(station_id, formula)
                if avg is not None:
                    key = formula.lower().replace("25", "2.5")
                    station_data[f"{key}_avg"] = avg
                    print(f"    {formula}: {avg} µg/m³")
                time.sleep(0.5)  # Rate limiting tussen stoffen

            self._station_averages[station_id] = station_data

        self._loaded = True
        self._save_to_cache()

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        if not self._load_from_cache():
            self._fetch_all_stations()

    def get_for_location(
        self, lat: float, lng: float, max_distance_km: Optional[float] = None
    ) -> LuchtmeetnetResult:
        """Haal luchtkwaliteitsdata op voor een locatie.

        Vindt het dichtstbijzijnde station en retourneert de data.
        within_max_distance geeft aan of het station dichtbij genoeg is
        om de data als representatief te beschouwen.
        """
        self._ensure_loaded()
        max_dist = max_distance_km if max_distance_km is not None else self.max_distance_km

        # Zoek dichtstbijzijnde station
        best_station = None
        best_distance = float("inf")
        for station_id, info in STATIONS.items():
            dist = haversine_km(lat, lng, info["lat"], info["lng"])
            if dist < best_distance:
                best_distance = dist
                best_station = station_id

        if best_station is None:
            # Zou niet moeten gebeuren met hardcoded stations
            return LuchtmeetnetResult(
                station_number="",
                station_naam="",
                station_type="",
                station_lat=0,
                station_lng=0,
                distance_km=0,
                within_max_distance=False,
            )

        info = STATIONS[best_station]
        averages = self._station_averages.get(best_station, {})

        return LuchtmeetnetResult(
            station_number=best_station,
            station_naam=info["naam"],
            station_type=info["type"],
            station_lat=info["lat"],
            station_lng=info["lng"],
            distance_km=round(best_distance, 2),
            within_max_distance=best_distance <= max_dist,
            no2_avg=averages.get("no2_avg"),
            pm10_avg=averages.get("pm10_avg"),
            pm25_avg=averages.get("pm2.5_avg"),
            o3_avg=averages.get("o3_avg"),
            jaar=averages.get("jaar"),
        )

    def get_all_stations(self) -> List[Dict[str, Any]]:
        """Retourneer alle stations met hun data."""
        self._ensure_loaded()
        result = []
        for station_id, info in STATIONS.items():
            averages = self._station_averages.get(station_id, {})
            result.append({
                "station_number": station_id,
                "naam": info["naam"],
                "type": info["type"],
                "lat": info["lat"],
                "lng": info["lng"],
                "formulas": info["formulas"],
                **averages,
            })
        return result


def create_luchtmeetnet_collector(
    cache_dir: Optional[Path] = None,
    max_distance_km: float = 4.0,
) -> LuchtmeetnetCollector:
    """Factory function met default cache directory."""
    if cache_dir is None:
        cache_dir = Path(__file__).parent.parent.parent / "data" / "cache" / "luchtmeetnet"
    return LuchtmeetnetCollector(cache_dir=cache_dir, max_distance_km=max_distance_km)
