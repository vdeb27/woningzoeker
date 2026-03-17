"""
RIVM Atlas Leefomgeving Collector.

Fetches environmental quality data from the RIVM Atlas Leefomgeving WFS service
at buurt level. Data includes:
- Geluidhinder: wegverkeer, trein, vliegtuig, buren, windturbine (% ernstig gehinderd)
- Slaapverstoring: wegverkeer, trein, vliegtuig, buren (% ernstig verstoord)
- Tevredenheid: woning, woonomgeving, groenvoorzieningen
- Verkeer: verkeersdrukte binnen/buiten bebouwde kom

Source: data.rivm.nl/geo/alo/wfs — layer rivm_20251201_geluidhinder_bu_weg_2024
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import requests


# RIVM Atlas WFS endpoint (buurt-level geluidhinder)
RIVM_WFS_URL = "https://data.rivm.nl/geo/alo/wfs"
RIVM_GELUID_LAYER = "rivm_20251201_geluidhinder_bu_weg_2024"

# Mapping from WFS property names to our indicator names
RIVM_COLUMNS = {
    "geluidhinder_weg_pct": "b_gel_weg",
    "geluidhinder_50db_minus_pct": "b_gel_lt50",
    "geluidhinder_50db_plus_pct": "b_gel_gt50",
    "geluidhinder_trein_pct": "b_gel_trei",
    "geluidhinder_vliegverkeer_pct": "b_gel_vv",
    "geluidhinder_windturbine_pct": "b_gel_wtn",
    "geluidhinder_buren_pct": "b_gel_bu",
    "slaapverstoring_weg_pct": "b_sv_weg",
    "slaapverstoring_trein_pct": "b_sv_trein",
    "slaapverstoring_vliegverkeer_pct": "b_sv_vv",
    "slaapverstoring_buren_pct": "b_sv_bu",
    "tevredenheid_woning_pct": "b_tevr_won",
    "tevredenheid_woonomgeving_pct": "b_tevr_wo",
    "tevredenheid_groen_pct": "b_tevr_gr",
    "verkeersdrukte_binnen_pct": "b_verk_bi",
    "verkeersdrukte_buiten_pct": "b_verk_bu",
    "bereikbaarheid_voldoende_pct": "b_vold_br",
}

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "rivm"
CACHE_DURATION_SECONDS = 30 * 24 * 60 * 60  # 30 days

TARGET_MUNICIPALITIES = ["0518", "1916", "0603"]


@dataclass
class RIVMResult:
    """Environmental quality data for a neighborhood (buurt)."""
    buurt_code: str
    # Geluidhinder (% ernstig gehinderd)
    geluidhinder_weg_pct: Optional[float] = None
    geluidhinder_50db_minus_pct: Optional[float] = None
    geluidhinder_50db_plus_pct: Optional[float] = None
    geluidhinder_trein_pct: Optional[float] = None
    geluidhinder_vliegverkeer_pct: Optional[float] = None
    geluidhinder_windturbine_pct: Optional[float] = None
    geluidhinder_buren_pct: Optional[float] = None
    # Slaapverstoring (% ernstig verstoord)
    slaapverstoring_weg_pct: Optional[float] = None
    slaapverstoring_trein_pct: Optional[float] = None
    slaapverstoring_vliegverkeer_pct: Optional[float] = None
    slaapverstoring_buren_pct: Optional[float] = None
    # Tevredenheid (%)
    tevredenheid_woning_pct: Optional[float] = None
    tevredenheid_woonomgeving_pct: Optional[float] = None
    tevredenheid_groen_pct: Optional[float] = None
    # Verkeer
    verkeersdrukte_binnen_pct: Optional[float] = None
    verkeersdrukte_buiten_pct: Optional[float] = None
    bereikbaarheid_voldoende_pct: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RIVMResult":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class RIVMCollector:
    """
    Collector for RIVM Atlas Leefomgeving environmental data.

    Fetches buurt-level geluidhinder data from data.rivm.nl WFS service.
    """

    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    _data: Dict[str, RIVMResult] = field(default_factory=dict, init=False, repr=False)
    _loaded: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self) -> Path:
        return self.cache_dir / "rivm_data.json"

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

    def _fetch_from_wfs(self) -> None:
        """Fetch buurt-level geluidhinder data from RIVM WFS."""
        target_prefixes = tuple(f"BU{m}" for m in TARGET_MUNICIPALITIES)

        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": RIVM_GELUID_LAYER,
            "outputFormat": "application/json",
        }

        try:
            response = requests.get(RIVM_WFS_URL, params=params, timeout=120)
            response.raise_for_status()
            all_features = response.json().get("features", [])
        except requests.RequestException as e:
            print(f"  RIVM WFS fout: {e}")
            all_features = []

        for feature in all_features:
            props = feature.get("properties", {})
            code = str(props.get("buurtcode", "")).strip()
            if not code.startswith(target_prefixes):
                continue

            values = {"buurt_code": code}
            for our_key, wfs_key in RIVM_COLUMNS.items():
                raw = props.get(wfs_key)
                if raw is not None:
                    try:
                        val = float(raw)
                        if val != -9999.0:  # RIVM no-data sentinel
                            values[our_key] = val
                    except (ValueError, TypeError):
                        pass

            self._data[code] = RIVMResult(**values)

        self._loaded = True
        self._save_to_cache()

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        if not self._load_from_cache():
            self._fetch_from_wfs()

    def get_buurt(self, buurt_code: str) -> Optional[RIVMResult]:
        """Get RIVM data for a buurt."""
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
