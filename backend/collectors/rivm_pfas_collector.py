"""
RIVM PFAS Collector.

Haalt PFAS bodemverontreinigingsdata op van de RIVM Atlas Leefomgeving WFS.
Zoekt bodemmonsters in de buurt van een locatie en rapporteert PFOA/PFOS
concentraties.

Bron: data.rivm.nl/geo/alo/wfs — layer rivm_20201201_pfasdef_totaal
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from utils.geo import haversine_km, rd_to_wgs84


RIVM_WFS_URL = "https://data.rivm.nl/geo/alo/wfs"
PFAS_LAYER = "rivm_20201201_pfasdef_totaal"

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "pfas"
CACHE_DURATION_SECONDS = 30 * 24 * 60 * 60  # 30 dagen

# PFAS normen (Rijkswaterstaat interventiewaarden grond, µg/kg droge stof)
PFOS_NORM = 3.8  # µg/kg ds (woongebied)
PFOA_NORM = 7.0  # µg/kg ds (woongebied)


@dataclass
class PFASSample:
    """Enkel PFAS bodemmonster."""

    lat: float
    lng: float
    som_pfoa: Optional[float] = None  # µg/kg
    som_pfos: Optional[float] = None  # µg/kg
    diepte_profiel: Optional[str] = None  # "toplaag" of "sublaag"
    diepte_cm: Optional[str] = None  # bijv. "0 - 20"

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class PFASResult:
    """PFAS resultaat voor een locatie."""

    samples_within_radius: int = 0
    max_pfoa: Optional[float] = None  # µg/kg
    max_pfos: Optional[float] = None  # µg/kg
    has_contamination: bool = False  # boven norm
    nearest_sample_distance_km: Optional[float] = None
    search_radius_km: float = 1.0
    samples: List[PFASSample] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "samples_within_radius": self.samples_within_radius,
            "max_pfoa": self.max_pfoa,
            "max_pfos": self.max_pfos,
            "has_contamination": self.has_contamination,
            "nearest_sample_distance_km": self.nearest_sample_distance_km,
            "search_radius_km": self.search_radius_km,
        }
        return {k: v for k, v in d.items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PFASResult":
        fields = {k for k in cls.__dataclass_fields__ if k != "samples"}
        return cls(**{k: v for k, v in data.items() if k in fields})


@dataclass
class RIVMPFASCollector:
    """Collector voor PFAS bodemverontreinigingsdata."""

    search_radius_km: float = 1.0
    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    _samples: List[PFASSample] = field(default_factory=list, init=False, repr=False)
    _loaded: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self) -> Path:
        return self.cache_dir / "pfas_samples.json"

    def _load_from_cache(self) -> bool:
        cache_path = self._cache_path()
        if not cache_path.exists():
            return False
        try:
            with cache_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("timestamp", 0) + CACHE_DURATION_SECONDS < time.time():
                return False
            self._samples = [
                PFASSample(**s) for s in data.get("samples", [])
            ]
            self._loaded = True
            return True
        except (json.JSONDecodeError, IOError, TypeError):
            return False

    def _save_to_cache(self) -> None:
        cache_data = {
            "timestamp": time.time(),
            "samples": [s.to_dict() for s in self._samples],
        }
        try:
            with self._cache_path().open("w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False)
        except IOError:
            pass

    def _fetch_from_wfs(self) -> None:
        """Haal alle PFAS bodemmonsters op via WFS."""
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": PFAS_LAYER,
            "outputFormat": "application/json",
        }

        try:
            print("  PFAS: ophalen van RIVM WFS...")
            response = requests.get(RIVM_WFS_URL, params=params, timeout=120)
            response.raise_for_status()
            features = response.json().get("features", [])
            print(f"  PFAS: {len(features)} monsters ontvangen")
        except requests.RequestException as e:
            print(f"  PFAS WFS fout: {e}")
            features = []

        for feature in features:
            props = feature.get("properties", {})
            geom = feature.get("geometry", {})

            # Coördinaten: MultiPoint in RD (EPSG:28992)
            coords = geom.get("coordinates", [])
            if not coords:
                continue
            # MultiPoint: eerste punt pakken
            point = coords[0] if isinstance(coords[0], list) else coords
            if len(point) < 2:
                continue

            try:
                x, y = float(point[0]), float(point[1])
                lat, lng = rd_to_wgs84(x, y)
            except (ValueError, TypeError):
                continue

            # Alleen toplaag monsters (meest relevant voor bewoners)
            diepte_profiel = str(props.get("diepteprof", "")).strip()
            if diepte_profiel and diepte_profiel != "toplaag":
                continue

            def _safe_float(val: Any) -> Optional[float]:
                if val is None:
                    return None
                try:
                    v = float(val)
                    return v if v >= 0 else None
                except (ValueError, TypeError):
                    return None

            self._samples.append(PFASSample(
                lat=lat,
                lng=lng,
                som_pfoa=_safe_float(props.get("som_pfoa")),
                som_pfos=_safe_float(props.get("som_pfos")),
                diepte_profiel=diepte_profiel or None,
                diepte_cm=str(props.get("diepte_cm", "")).strip() or None,
            ))

        self._loaded = True
        self._save_to_cache()
        print(f"  PFAS: {len(self._samples)} toplaag-monsters verwerkt")

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        if not self._load_from_cache():
            self._fetch_from_wfs()

    def get_for_location(
        self, lat: float, lng: float, radius_km: Optional[float] = None
    ) -> PFASResult:
        """Zoek PFAS monsters in de buurt van een locatie."""
        self._ensure_loaded()
        radius = radius_km if radius_km is not None else self.search_radius_km

        nearby: List[PFASSample] = []
        nearest_dist = float("inf")

        for sample in self._samples:
            dist = haversine_km(lat, lng, sample.lat, sample.lng)
            if dist < nearest_dist:
                nearest_dist = dist
            if dist <= radius:
                nearby.append(sample)

        if not nearby:
            return PFASResult(
                search_radius_km=radius,
                nearest_sample_distance_km=(
                    round(nearest_dist, 2)
                    if nearest_dist < float("inf") and self._samples
                    else None
                ),
            )

        pfoa_values = [s.som_pfoa for s in nearby if s.som_pfoa is not None]
        pfos_values = [s.som_pfos for s in nearby if s.som_pfos is not None]

        max_pfoa = max(pfoa_values) if pfoa_values else None
        max_pfos = max(pfos_values) if pfos_values else None

        has_contamination = (
            (max_pfoa is not None and max_pfoa > PFOA_NORM)
            or (max_pfos is not None and max_pfos > PFOS_NORM)
        )

        return PFASResult(
            samples_within_radius=len(nearby),
            max_pfoa=round(max_pfoa, 2) if max_pfoa is not None else None,
            max_pfos=round(max_pfos, 2) if max_pfos is not None else None,
            has_contamination=has_contamination,
            nearest_sample_distance_km=round(
                min(haversine_km(lat, lng, s.lat, s.lng) for s in nearby), 2
            ),
            search_radius_km=radius,
            samples=nearby,
        )


def create_rivm_pfas_collector(
    cache_dir: Optional[Path] = None,
    search_radius_km: float = 1.0,
) -> RIVMPFASCollector:
    """Factory function met default cache directory."""
    if cache_dir is None:
        cache_dir = Path(__file__).parent.parent.parent / "data" / "cache" / "pfas"
    return RIVMPFASCollector(cache_dir=cache_dir, search_radius_km=search_radius_km)
