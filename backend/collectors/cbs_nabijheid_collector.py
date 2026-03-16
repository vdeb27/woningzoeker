"""
CBS Nabijheid Voorzieningen Collector.

Fetches detailed proximity data from CBS dataset 80306ned
"Nabijheid voorzieningen; afstand locatie, wijk- en buurtcijfers".
Provides more detailed distance data than Kerncijfers.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


CBS_API_BASE = "https://opendata.cbs.nl/ODataApi/odata"
DATASET_NABIJHEID = "80306ned"

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "cbs_nabijheid"
CACHE_DURATION_SECONDS = 30 * 24 * 60 * 60  # 30 days

TARGET_MUNICIPALITIES = ["0518", "1916", "0603"]

# Nabijheid columns - distances to various facilities
NABIJHEID_COLUMNS = {
    "afstand_huisarts": "AfstandTotHuisartsenpraktijk_1",
    "afstand_apotheek": "AfstandTotApotheek_2",
    "afstand_ziekenhuis_excl_buitenpoli": "AfstandTotZiekenhuisExclBuitenpoli_3",
    "afstand_ziekenhuis_incl_buitenpoli": "AfstandTotZiekenhuisInclBuitenpoli_4",
    "afstand_kinderdagverblijf": "AfstandTotKinderdagverblijf_5",
    "afstand_buitenschoolse_opvang": "AfstandTotBuitenschoolseOpvang_6",
    "afstand_basisonderwijs": "AfstandTotBasisschool_7",
    "afstand_vmbo": "AfstandTotLocatieVmbo_8",
    "afstand_havo_vwo": "AfstandTotLocatieHavoVwo_9",
    "afstand_supermarkt": "AfstandTotSupermarkt_10",
    "afstand_warenhuis": "AfstandTotWarenhuis_11",
    "afstand_cafe": "AfstandTotCafe_14",
    "afstand_restaurant": "AfstandTotRestaurant_16",
    "afstand_hotel": "AfstandTotHotel_17",
    "afstand_bioscoop": "AfstandTotBioscoop_19",
    "afstand_bibliotheek": "AfstandTotBibliotheek_21",
    "afstand_zwembad": "AfstandTotZwembad_22",
    "afstand_sporthal": "AfstandTotSporthal_24",
    "afstand_museum": "AfstandTotMuseum_26",
    "afstand_attractiepark": "AfstandTotAttractiepark_28",
    "afstand_brandweerkazerne": "AfstandTotBrandweerkazerne_29",
    "afstand_oprit_hoofdverkeersweg": "AfstandTotOpritHoofdverkeersweg_30",
    "afstand_treinstation": "AfstandTotTreinstation_31",
}


@dataclass
class NabijheidResult:
    """Proximity data for a neighborhood."""
    buurt_code: str
    afstanden: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {"buurt_code": self.buurt_code, "afstanden": self.afstanden}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NabijheidResult":
        return cls(buurt_code=data["buurt_code"], afstanden=data.get("afstanden", {}))


@dataclass
class CBSNabijheidCollector:
    """Collector for CBS Nabijheid voorzieningen data."""

    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    _data: Dict[str, NabijheidResult] = field(default_factory=dict, init=False, repr=False)
    _loaded: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self) -> Path:
        return self.cache_dir / "cbs_nabijheid_data.json"

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
                self._data[code] = NabijheidResult.from_dict(entry)
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

    def _fetch_from_cbs(self) -> None:
        """Fetch nabijheid data from CBS OData API."""
        code_filters = []
        for muni_code in TARGET_MUNICIPALITIES:
            code_filters.append(f"startswith(Codering_3,'BU{muni_code}')")

        params = {
            "$filter": " or ".join(code_filters),
        }

        base_url = f"{CBS_API_BASE}/{DATASET_NABIJHEID}/TypedDataSet"
        records = []
        url = base_url
        first_request = True

        while url:
            try:
                if first_request:
                    response = requests.get(url, params=params, timeout=120)
                    first_request = False
                else:
                    response = requests.get(url, timeout=120)
                response.raise_for_status()
                data = response.json()
            except requests.RequestException as exc:
                raise RuntimeError(f"Failed to fetch CBS nabijheid data: {exc}") from exc

            records.extend(data.get("value", []))
            url = data.get("odata.nextLink") or data.get("@odata.nextLink")

        for record in records:
            buurt_code = str(record.get("Codering_3", "")).strip()
            if not buurt_code.startswith("BU"):
                continue

            afstanden = {}
            for key, cbs_col in NABIJHEID_COLUMNS.items():
                raw = record.get(cbs_col)
                if raw is not None:
                    try:
                        afstanden[key] = float(raw)
                    except (ValueError, TypeError):
                        pass

            self._data[buurt_code] = NabijheidResult(
                buurt_code=buurt_code,
                afstanden=afstanden,
            )

        self._loaded = True
        self._save_to_cache()

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        if not self._load_from_cache():
            self._fetch_from_cbs()

    def get_buurt(self, buurt_code: str) -> Optional[NabijheidResult]:
        """Get nabijheid data for a buurt."""
        self._ensure_loaded()
        code = buurt_code.upper().strip()
        if not code.startswith("BU"):
            code = f"BU{code}"
        return self._data.get(code)

    def get_all(self) -> Dict[str, NabijheidResult]:
        """Get all nabijheid data."""
        self._ensure_loaded()
        return dict(self._data)


def create_cbs_nabijheid_collector(cache_dir: Optional[Path] = None) -> CBSNabijheidCollector:
    """Factory function with default cache directory."""
    if cache_dir is None:
        cache_dir = Path(__file__).parent.parent.parent / "data" / "cache" / "cbs_nabijheid"
    return CBSNabijheidCollector(cache_dir=cache_dir)
