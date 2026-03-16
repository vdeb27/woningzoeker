"""
Leefbaarometer 3.0 Collector.

Fetches neighborhood livability scores from the Leefbaarometer dataset.
Uses the Leefbaarometer 3.0 API (data.overheid.nl / PDOK WFS).

The Leefbaarometer provides a composite score and 5 dimension scores:
- Fysieke omgeving (physical environment)
- Voorzieningen (facilities)
- Veiligheid (safety)
- Bevolkingssamenstelling (population composition)
- Woningvoorraad (housing stock)
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


# Leefbaarometer WFS endpoint (PDOK)
LBM_WFS_URL = "https://service.pdok.nl/lbm/leefbaarometer/wfs/v1_0"

# Cache settings
CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "leefbaarometer"
CACHE_DURATION_SECONDS = 30 * 24 * 60 * 60  # 30 days (static data)

# Target municipalities
TARGET_MUNICIPALITIES = ["0518", "1916", "0603"]


@dataclass
class LeefbaarometerResult:
    """Leefbaarometer data for a neighborhood."""
    buurt_code: str
    lbm_score: Optional[float] = None  # Overall livability score
    fysieke_omgeving: Optional[float] = None
    voorzieningen: Optional[float] = None
    veiligheid: Optional[float] = None
    bevolkingssamenstelling: Optional[float] = None
    woningvoorraad: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "buurt_code": self.buurt_code,
            "lbm_score": self.lbm_score,
            "fysieke_omgeving": self.fysieke_omgeving,
            "voorzieningen": self.voorzieningen,
            "veiligheid": self.veiligheid,
            "bevolkingssamenstelling": self.bevolkingssamenstelling,
            "woningvoorraad": self.woningvoorraad,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LeefbaarometerResult":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class LeefbaarometerCollector:
    """
    Collector for Leefbaarometer 3.0 neighborhood livability data.

    Fetches data from PDOK WFS service for target municipalities.
    """

    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    _data: Dict[str, LeefbaarometerResult] = field(default_factory=dict, init=False, repr=False)
    _loaded: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self) -> Path:
        return self.cache_dir / "leefbaarometer_data.json"

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
                self._data[code] = LeefbaarometerResult.from_dict(entry)
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
        """Fetch Leefbaarometer data from PDOK WFS."""
        for muni_code in TARGET_MUNICIPALITIES:
            cql_filter = f"gemeentecode='{muni_code}'"

            params = {
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeName": "leefbaarometer:indicatorscorebuurt",
                "outputFormat": "application/json",
                "CQL_FILTER": cql_filter,
            }

            try:
                response = requests.get(LBM_WFS_URL, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()

                for feature in data.get("features", []):
                    props = feature.get("properties", {})
                    buurt_code = props.get("buurtcode", "")
                    if not buurt_code:
                        continue

                    if not buurt_code.startswith("BU"):
                        buurt_code = f"BU{buurt_code}"

                    result = LeefbaarometerResult(
                        buurt_code=buurt_code,
                        lbm_score=_safe_float(props.get("lbm")),
                        fysieke_omgeving=_safe_float(props.get("fys")),
                        voorzieningen=_safe_float(props.get("vrz")),
                        veiligheid=_safe_float(props.get("vei")),
                        bevolkingssamenstelling=_safe_float(props.get("bev")),
                        woningvoorraad=_safe_float(props.get("won")),
                    )
                    self._data[buurt_code] = result

            except requests.RequestException:
                # Graceful degradation - continue without this gemeente
                continue

        self._loaded = True
        self._save_to_cache()

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        if not self._load_from_cache():
            self._fetch_from_wfs()

    def get_buurt(self, buurt_code: str) -> Optional[LeefbaarometerResult]:
        """Get Leefbaarometer data for a buurt."""
        self._ensure_loaded()
        code = buurt_code.upper().strip()
        if not code.startswith("BU"):
            code = f"BU{code}"
        return self._data.get(code)

    def get_all(self) -> Dict[str, LeefbaarometerResult]:
        """Get all Leefbaarometer data."""
        self._ensure_loaded()
        return dict(self._data)


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def create_leefbaarometer_collector(cache_dir: Optional[Path] = None) -> LeefbaarometerCollector:
    """Factory function with default cache directory."""
    if cache_dir is None:
        cache_dir = Path(__file__).parent.parent.parent / "data" / "cache" / "leefbaarometer"
    return LeefbaarometerCollector(cache_dir=cache_dir)
