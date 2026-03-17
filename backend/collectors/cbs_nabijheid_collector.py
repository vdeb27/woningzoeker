"""
CBS Nabijheid Voorzieningen Collector.

Fetches detailed proximity data from CBS dataset 86270NED
"Nabijheid voorzieningen; afstand locatie, wijk- en buurtcijfers 2025".
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
DATASET_NABIJHEID = "86270NED"  # Nabijheid voorzieningen 2025

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "cbs_nabijheid"
CACHE_DURATION_SECONDS = 30 * 24 * 60 * 60  # 30 days

TARGET_MUNICIPALITIES = ["0518", "1916", "0603"]

# Nabijheid columns - distances to various facilities (dataset 86270NED)
NABIJHEID_COLUMNS = {
    # Zorg
    "afstand_huisarts": "AfstandTotHuisartsenpraktijk_5",
    "afstand_huisartsenpost": "AfstandTotHuisartsenpost_9",
    "afstand_apotheek": "AfstandTotApotheek_10",
    "afstand_ziekenhuis": "AfstandTotZiekenhuis_11",
    "afstand_consultatiebureau": "AfstandTotConsultatiebureau_19",
    "afstand_fysiotherapeut": "AfstandTotFysiotherapeut_20",
    # Winkels & horeca
    "afstand_supermarkt": "AfstandTotGroteSupermarkt_24",
    "afstand_dagelijkse_levensmiddelen": "AfstandTotOvDagelLevensmiddelen_28",
    "afstand_warenhuis": "AfstandTotWarenhuis_32",
    "afstand_cafe": "AfstandTotCafeED_36",
    "afstand_cafetaria": "AfstandTotCafetariaED_40",
    "afstand_restaurant": "AfstandTotRestaurant_44",
    "afstand_hotel": "AfstandTotHotelED_48",
    # Kinderopvang & onderwijs
    "afstand_kinderdagverblijf": "AfstandTotKinderdagverblijf_52",
    "afstand_buitenschoolse_opvang": "AfstandTotBuitenschoolseOpvang_56",
    "afstand_basisonderwijs": "AfstandTotSchool_60",
    "afstand_voortgezet_onderwijs": "AfstandTotSchool_64",
    "afstand_vmbo": "AfstandTotSchool_68",
    "afstand_havo_vwo": "AfstandTotSchool_72",
    # Natuur & groen
    "afstand_openbaar_groen": "AfstandTotOpenbaarGroenTotaal_76",
    "afstand_park": "AfstandTotParkOfPlantsoen_77",
    "afstand_dagrecreatie": "AfstandTotDagrecreatiefTerrein_78",
    "afstand_bos": "AfstandTotBos_79",
    "afstand_open_natuur": "AfstandTotOpenNatTerreinTotaal_80",
    "afstand_semi_openbaar_groen": "AfstandTotSemiOpenbaarGroenTotaal_83",
    "afstand_sportterrein": "AfstandTotSportterrein_84",
    "afstand_volkstuin": "AfstandTotVolkstuin_85",
    "afstand_recreatief_water": "AfstandTotRecreatiefBinnenwater_88",
    # Vervoer
    "afstand_oprit_hoofdverkeersweg": "AfstandTotOpritHoofdverkeersweg_89",
    "afstand_treinstation": "AfstandTotTreinstationsTotaal_90",
    "afstand_overstapstation": "AfstandTotBelangrijkOverstapstation_91",
    # Cultuur & recreatie
    "afstand_bibliotheek": "AfstandTotBibliotheek_92",
    "afstand_zwembad": "AfstandTotBinnenzwembad_93",
    "afstand_kunstijsbaan": "AfstandTotKunstijsbaan_94",
    "afstand_museum": "AfstandTotMuseum_95",
    "afstand_podiumkunsten": "AfstandTotPodiumkunstenTotaal_99",
    "afstand_poppodium": "AfstandTotPoppodium_103",
    "afstand_bioscoop": "AfstandTotBioscoop_104",
    "afstand_sauna": "AfstandTotSauna_108",
    "afstand_attractie": "AfstandTotAttractie_110",
    # Veiligheid
    "afstand_brandweerkazerne": "AfstandTotBrandweerkazerne_114",
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
        import cbsodata

        select_cols = ["Codering_3"]
        select_cols.extend(NABIJHEID_COLUMNS.values())

        try:
            all_records = cbsodata.get_data(
                DATASET_NABIJHEID,
                select=select_cols,
            )
        except Exception as exc:
            raise RuntimeError(f"Failed to fetch CBS nabijheid data: {exc}") from exc

        # Filter to target municipalities client-side
        target_prefixes = tuple(f"BU{m}" for m in TARGET_MUNICIPALITIES)
        records = [
            r for r in all_records
            if str(r.get("Codering_3", "")).strip().startswith(target_prefixes)
        ]

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
