"""
CBS Extra Buurt Datasets Collector.

Fetches additional CBS datasets at buurt/wijk level:
- 84468NED: Geregistreerde misdrijven per buurt (2018)
- 86258NED: Arbeidsdeelname per wijk/buurt (2024)
- 85539NED: SES-WOA scores per wijk/buurt
- 86232NED: Opleidingsniveau per wijk/buurt (2024)
- 86211NED: Bodemgebruik per wijk/buurt (2022)

All data is merged into a single dict keyed by buurt code.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import cbsodata


CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "cbs_extra"
CACHE_DURATION_SECONDS = 30 * 24 * 60 * 60  # 30 days

TARGET_MUNICIPALITIES = ["0518", "1916", "0603"]

# === Dataset definitions ===

# Misdrijven per buurt (2018 - most recent buurt-level)
MISDRIJVEN_DATASET = "84468NED"
MISDRIJVEN_COLUMNS = {
    "misdrijven_totaal": "TotaalVermogenVernielingEnGeweld_6",
    "vermogensmisdrijven": "TotaalVermogensmisdrijven_7",
    "diefstal_totaal": "TotaalDiefstal_8",
    "fietsendiefstal": "Fietsendiefstal_9",
    "diefstal_vervoermiddelen": "DiefstalOverigeVervoermiddelen_10",
    "diefstal_uit_vervoermiddelen": "DiefstalUitVanafVervoermiddelen_11",
    "zakkenrollerij_straatroof": "ZakkenrollerijStraatroofEnBeroving_12",
    "woninginbraak": "TotaalDiefstalUitWoningSchuurED_13",
    "diefstal_niet_residentieel": "DiefstalUitNietResidentieleGebouwen_14",
    "vernieling_totaal": "TotaalVernieling_18",
    "vernieling_auto": "VernielingAanAuto_19",
    "geweld_seksueel_totaal": "TotaalGeweldsEnSeksueleMisdrijven_22",
    "mishandeling": "Mishandeling_23",
    "bedreiging_stalking": "BedreigingEnStalking_24",
    # Per 1000 inwoners
    "misdrijven_per_1000": "TotaalVermogenVernielingEnGeweld_26",
    "woninginbraak_per_1000": "TotaalDiefstalUitWoningSchuurED_28",
    "geweld_per_1000": "GeweldsEnSeksueleMisdrijven_30",
}

# Arbeidsdeelname per wijk/buurt (2024)
ARBEID_DATASET = "86258NED"
ARBEID_COLUMNS = {
    "beroepsbevolking": "BeroepsEnNietBeroepsbevolking_1",
    "werkzame_beroepsbevolking": "WerkzameBeroepsbevolking_2",
    "netto_arbeidsparticipatie": "NettoArbeidsparticipatie_3",
    "werknemers": "Werknemer_4",
    "werknemer_vast": "WerknemerMetVasteArbeidsrelatie_5",
    "werknemer_flex": "WerknemerMetFlexibeleArbeidsrelatie_6",
    "zelfstandig": "Zelfstandige_7",
    "zzp": "ZelfstandigeZonderPersoneelZzp_8",
}

# SES-WOA scores (sociaal-economische status)
SES_DATASET = "85539NED"
SES_COLUMNS = {
    "ses_huishoudens": "ParticuliereHuishoudens_2",
    "ses_laag_pct": "k_1eTotEnMet40ePercentielgroep_3",
    "ses_midden_pct": "k_41eTotEnMet80ePercentielgroep_4",
    "ses_hoog_pct": "k_81eTotEnMet100ePercentielgroep_5",
    "ses_gemiddeld": "GemiddeldePercentielgroep_6",
}

# Opleidingsniveau per wijk/buurt (2024) — has Opleidingsniveau dimension
# We'll handle this specially in _fetch_opleiding()
OPLEIDING_DATASET = "86232NED"

# Bodemgebruik per wijk/buurt (2022)
BODEM_DATASET = "86211NED"
BODEM_COLUMNS = {
    "opp_totaal": "TotaalLandEnWater_4",
    "opp_land": "TotaalLand_5",
    "opp_water": "TotaalWater_6",
    "opp_verkeersterrein": "TotaalVerkeersterrein_9",
    "opp_bebouwd": "TotaalBebouwdTerrein_14",
    "opp_woonterrein": "Woonterrein_15",
    "opp_bedrijfsterrein": "Bedrijfsterrein_19",
    "opp_bouwterrein": "Bouwterrein_24",
}


def _build_name_to_code_map(dataset_id: str) -> Dict[str, str]:
    """Build a name→buurt_code mapping from the WijkenEnBuurten dimension."""
    import requests
    target_prefixes = tuple(f"BU{m}" for m in TARGET_MUNICIPALITIES)
    name_map: Dict[str, str] = {}

    url = f"https://opendata.cbs.nl/ODataApi/odata/{dataset_id}/WijkenEnBuurten"
    while url:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        for rec in data.get("value", []):
            key = str(rec.get("Key", "")).strip()
            title = str(rec.get("Title", "")).strip()
            if key.startswith(target_prefixes) and title:
                name_map[title] = key
        url = data.get("odata.nextLink") or data.get("@odata.nextLink")

    return name_map


def _fetch_dataset(
    dataset_id: str,
    columns: Dict[str, str],
    code_field: str = "Codering_3",
    extra_filters: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, float]]:
    """Fetch a CBS dataset and return data keyed by buurt code.

    For datasets where code_field is 'WijkenEnBuurten' (GeoDetail),
    the field contains names, so we first build a name→code mapping
    from the dimension table.
    """
    is_geodetail = code_field == "WijkenEnBuurten"
    name_to_code = _build_name_to_code_map(dataset_id) if is_geodetail else {}

    select_cols = [code_field]
    if extra_filters:
        select_cols.extend(extra_filters.keys())
    select_cols.extend(columns.values())

    all_records = cbsodata.get_data(dataset_id, select=select_cols)

    target_prefixes = tuple(f"BU{m}" for m in TARGET_MUNICIPALITIES)
    result: Dict[str, Dict[str, float]] = {}

    for record in all_records:
        # Apply extra filters (e.g. Geslacht == 'Totaal mannen en vrouwen')
        if extra_filters:
            skip = False
            for fk, fv in extra_filters.items():
                if str(record.get(fk, "")).strip() != fv:
                    skip = True
                    break
            if skip:
                continue

        raw_code = str(record.get(code_field, "")).strip()

        if is_geodetail:
            code = name_to_code.get(raw_code, "")
        else:
            code = raw_code

        if not code.startswith(target_prefixes):
            continue

        values = {}
        for key, cbs_col in columns.items():
            raw = record.get(cbs_col)
            if raw is not None:
                try:
                    values[key] = float(raw)
                except (ValueError, TypeError):
                    pass

        if values:
            result[code] = values

    return result


@dataclass
class CBSExtraCollector:
    """Collector for additional CBS buurt datasets."""

    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    _data: Dict[str, Dict[str, float]] = field(default_factory=dict, init=False, repr=False)
    _loaded: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self) -> Path:
        return self.cache_dir / "cbs_extra_data.json"

    def _load_from_cache(self) -> bool:
        cache_path = self._cache_path()
        if not cache_path.exists():
            return False
        try:
            with cache_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("timestamp", 0) + CACHE_DURATION_SECONDS < time.time():
                return False
            self._data = data.get("buurten", {})
            self._loaded = True
            return True
        except (json.JSONDecodeError, IOError, TypeError):
            return False

    def _save_to_cache(self) -> None:
        cache_data = {
            "timestamp": time.time(),
            "buurten": self._data,
        }
        try:
            with self._cache_path().open("w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False)
        except IOError:
            pass

    def _fetch_all(self) -> None:
        """Fetch all extra CBS datasets and merge."""
        datasets = [
            ("Misdrijven", MISDRIJVEN_DATASET, MISDRIJVEN_COLUMNS, "Codering_3", None),
            ("Arbeidsdeelname", ARBEID_DATASET, ARBEID_COLUMNS, "WijkenEnBuurten",
             {"Geslacht": "Totaal mannen en vrouwen", "Leeftijd": "15 tot 75 jaar"}),
            ("SES-WOA", SES_DATASET, SES_COLUMNS, "RegiocodeGemeenteWijkBuurt_1",
             {"Perioden": "2021"}),
            ("Bodemgebruik", BODEM_DATASET, BODEM_COLUMNS, "Codering_3", None),
        ]

        for name, dataset_id, columns, code_field, extra_filters in datasets:
            try:
                data = _fetch_dataset(dataset_id, columns, code_field, extra_filters)
                for code, values in data.items():
                    if code not in self._data:
                        self._data[code] = {}
                    self._data[code].update(values)
                print(f"  {name}: {len(data)} buurten")
            except Exception as exc:
                print(f"  {name} mislukt: {exc}")

        # Opleidingsniveau — special handling for multi-dimension
        try:
            data = self._fetch_opleiding()
            for code, values in data.items():
                if code not in self._data:
                    self._data[code] = {}
                self._data[code].update(values)
            print(f"  Opleidingsniveau: {len(data)} buurten")
        except Exception as exc:
            print(f"  Opleidingsniveau mislukt: {exc}")

        self._loaded = True
        self._save_to_cache()

    def _fetch_opleiding(self) -> Dict[str, Dict[str, float]]:
        """Fetch opleidingsniveau with 3 levels as separate columns."""
        name_to_code = _build_name_to_code_map(OPLEIDING_DATASET)
        target_prefixes = tuple(f"BU{m}" for m in TARGET_MUNICIPALITIES)

        level_map = {
            "1 Basisonderwijs, vmbo, mbo1": "opleiding_laag_pct",
            "2 Havo, vwo, mbo2-4": "opleiding_midden_pct",
            "3 Hbo, wo": "opleiding_hoog_pct",
        }

        all_records = cbsodata.get_data(
            OPLEIDING_DATASET,
            select=["Opleidingsniveau", "WijkenEnBuurten", "Bevolking15Tot75Jaar_2"],
        )

        result: Dict[str, Dict[str, float]] = {}
        for record in all_records:
            name = str(record.get("WijkenEnBuurten", "")).strip()
            code = name_to_code.get(name, "")
            if not code.startswith(target_prefixes):
                continue

            level = str(record.get("Opleidingsniveau", "")).strip()
            col_name = level_map.get(level)
            if not col_name:
                continue

            val = record.get("Bevolking15Tot75Jaar_2")
            if val is not None:
                try:
                    if code not in result:
                        result[code] = {}
                    result[code][col_name] = float(val)
                except (ValueError, TypeError):
                    pass

        return result

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        if not self._load_from_cache():
            self._fetch_all()

    def get_buurt(self, buurt_code: str) -> Optional[Dict[str, float]]:
        """Get extra indicators for a buurt."""
        self._ensure_loaded()
        code = buurt_code.upper().strip()
        return self._data.get(code)

    def get_all_buurten(self) -> Dict[str, Dict[str, float]]:
        """Get all buurt data."""
        self._ensure_loaded()
        return dict(self._data)


def create_cbs_extra_collector(cache_dir: Optional[Path] = None) -> CBSExtraCollector:
    """Factory function."""
    if cache_dir is None:
        cache_dir = CACHE_DIR
    return CBSExtraCollector(cache_dir=cache_dir)
