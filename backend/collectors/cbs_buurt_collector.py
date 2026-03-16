"""
CBS Buurt (Neighborhood) Data Collector.

Fetches neighborhood-level statistics from CBS "Kerncijfers wijken en buurten" (85618NED).
Extended to ~50 indicators across categories: bevolking, woningen, inkomen, energie,
voorzieningen, motorvoertuigen.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


# CBS OData API
CBS_API_BASE = "https://opendata.cbs.nl/ODataApi/odata"
DATASET_KERNCIJFERS = "85618NED"  # Kerncijfers wijken en buurten 2024

# Cache settings
CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CACHE_DURATION_SECONDS = 30 * 24 * 60 * 60  # 30 days (yearly dataset)

# Target municipalities
TARGET_MUNICIPALITIES = ["0518", "1916", "0603"]  # Den Haag, Leidschendam-Voorburg, Rijswijk

# CBS column names for housing indicators - extended to ~50
# Organized by category for clarity
HOUSING_COLUMNS = {
    # === Bevolking ===
    "inwoners": "AantalInwoners_5",
    "mannen": "Mannen_6",
    "vrouwen": "Vrouwen_7",
    "leeftijd_0_14": "k_0Tot15Jaar_8",
    "leeftijd_15_24": "k_15Tot25Jaar_9",
    "leeftijd_25_44": "k_25Tot45Jaar_10",
    "leeftijd_45_64": "k_45Tot65Jaar_11",
    "leeftijd_65_plus": "k_65JaarOfOuder_12",
    "ongehuwd": "Ongehuwd_13",
    "gehuwd": "Gehuwd_14",
    "gescheiden": "Gescheiden_15",
    "verweduwd": "Verweduwd_16",
    "bevolkingsdichtheid": "Bevolkingsdichtheid_33",
    "huishoudens_totaal": "HuishoudensTotaal_28",
    "eenpersoons_huishoudens": "Eenpersoonshuishoudens_29",
    "huishoudens_zonder_kinderen": "HuishoudensZonderKinderen_30",
    "huishoudens_met_kinderen": "HuishoudensMetKinderen_31",
    "gem_huishoudensgrootte": "GemiddeldeHuishoudensgrootte_32",

    # === Woningen ===
    "woningvoorraad": "Woningvoorraad_34",
    "woz_waarde": "GemiddeldeWOZWaardeVanWoningen_35",
    "koopwoningen_pct": "Koopwoningen_40",
    "huurwoningen_pct": "HuurwoningenTotaal_41",
    "huur_corporatie_pct": "InBezitWoningcorporatie_42",
    "huur_overig_pct": "InBezitOverigeVerhuurders_43",
    "eigendom_onbekend_pct": "EigendomOnbekend_44",
    "bouwjaar_voor_2000_pct": "BouwjaarVoor2000_53",
    "bouwjaar_vanaf_2000_pct": "BouwjaarVanaf2000_54",

    # === Energie ===
    "gem_gasverbruik": "GemiddeldAardgasverbruikTotaal_27",
    "gem_elektraverbruik": "GemiddeldeElektriciteitsleveringTotaal_23",

    # === Inkomen ===
    "gem_inkomen": "GemiddeldInkomenPerInwoner_66",
    "gem_inkomen_ontvanger": "GemiddeldInkomenPerInkomensontvwordt_67",
    "huishoudens_laag_inkomen_pct": "HuishoudensMetEenLaagInkomen_70",
    "huishoudens_hoog_inkomen_pct": "HuishoudensMetEenHoogInkomen_71",
    "huishoudens_onder_of_rond_sociaal_minimum": "HuishOnderOfRondSociaalMinimum_72",

    # === Uitkeringen ===
    "bijstandsuitkeringen_per_1000": "PersonenPerSoortUitkBijwordt_75",
    "ao_uitkeringen_per_1000": "PersonenPerSoortUitkAO_76",
    "ww_uitkeringen_per_1000": "PersonenPerSoortUitkWW_77",
    "aow_uitkeringen_per_1000": "PersonenPerSoortUitkAOW_78",

    # === Motorvoertuigen ===
    "personenautos_totaal": "PersonenautoSTotaal_109",
    "personenautos_per_huishouden": "PersonenautoSPerHuishouden_110",
    "personenautos_brandstof_benzine": "Personenautos_111",  # benzine
    "personenautos_brandstof_overig": "PersonenautoSOverigeBrandstof_112",

    # === Voorzieningen (afstanden in km) ===
    "afstand_huisartsenpraktijk": "AfstandTotHuisartsenpraktijk_100",
    "afstand_ziekenhuis": "AfstandTotZiekenhuis_102",
    "afstand_basisonderwijs": "AfstandTotSchoolBasisonderwijs_97",
    "afstand_voortgezet_onderwijs": "AfstandTotSchoolVoortgezetOndwordt_98",
    "afstand_kinderdagverblijf": "AfstandTotKinderdagverblijf_96",
    "afstand_grote_supermarkt": "AfstandTotGroteSupermarkt_92",
    "afstand_cafe": "AfstandTotCafe_89",
    "afstand_restaurant": "AfstandTotRestaurant_95",
    "afstand_bioscoop": "AfstandTotBioscoop_87",
    "afstand_zwembad": "AfstandTotZwembad_106",
    "afstand_sportterrein": "AfstandTotSportterrein_105",
    "afstand_bibliotheek": "AfstandTotBibliotheek_86",
    "afstand_oprit_hoofdverkeersweg": "AfstandTotOpritHoofdverkeersweg_93",
    "afstand_treinstation": "AfstandTotBelangrijkOverstapstation_88",
}


@dataclass
class BuurtData:
    """Housing data for a neighborhood (buurt)."""

    buurt_code: str
    buurt_naam: str
    gemeente_code: str
    gemeente_naam: str

    # WOZ waarde (x 1000 euro, so multiply by 1000 for actual value)
    gem_woz_waarde: Optional[int] = None

    # Housing composition (percentages)
    koopwoningen_pct: Optional[float] = None
    huurwoningen_pct: Optional[float] = None

    # Building age (percentages)
    bouwjaar_voor_2000_pct: Optional[float] = None
    bouwjaar_vanaf_2000_pct: Optional[float] = None

    # Income indicators
    gem_inkomen: Optional[int] = None  # x 1000 euro
    huishoudens_laag_inkomen_pct: Optional[float] = None

    # Extended indicators (all CBS data beyond core fields)
    indicatoren: Dict[str, Any] = field(default_factory=dict)

    bron: str = "CBS Kerncijfers wijken en buurten"


@dataclass
class CBSBuurtCollector:
    """
    Collector for CBS neighborhood-level housing data.

    Fetches and caches data from "Kerncijfers wijken en buurten" dataset.
    Provides lookup by buurt code for use in property valuations.
    """

    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    _buurt_data: Dict[str, BuurtData] = field(default_factory=dict, init=False, repr=False)
    _loaded: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        """Ensure cache directory exists."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self) -> Path:
        """Get cache file path."""
        return self.cache_dir / "cbs_buurt_data.json"

    def _load_from_cache(self) -> bool:
        """Load cached buurt data. Returns True if cache is valid."""
        cache_path = self._cache_path()
        if not cache_path.exists():
            return False

        try:
            with cache_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            # Check if cache is still valid
            if data.get("timestamp", 0) + CACHE_DURATION_SECONDS < time.time():
                return False

            # Load buurt data from cache
            for code, buurt_dict in data.get("buurten", {}).items():
                self._buurt_data[code] = BuurtData(**buurt_dict)

            self._loaded = True
            return True

        except (json.JSONDecodeError, IOError, TypeError):
            return False

    def _save_to_cache(self) -> None:
        """Save buurt data to cache."""
        cache_path = self._cache_path()
        cache_data = {
            "timestamp": time.time(),
            "buurten": {
                code: {
                    "buurt_code": b.buurt_code,
                    "buurt_naam": b.buurt_naam,
                    "gemeente_code": b.gemeente_code,
                    "gemeente_naam": b.gemeente_naam,
                    "gem_woz_waarde": b.gem_woz_waarde,
                    "koopwoningen_pct": b.koopwoningen_pct,
                    "huurwoningen_pct": b.huurwoningen_pct,
                    "bouwjaar_voor_2000_pct": b.bouwjaar_voor_2000_pct,
                    "bouwjaar_vanaf_2000_pct": b.bouwjaar_vanaf_2000_pct,
                    "gem_inkomen": b.gem_inkomen,
                    "huishoudens_laag_inkomen_pct": b.huishoudens_laag_inkomen_pct,
                    "indicatoren": b.indicatoren,
                    "bron": b.bron,
                }
                for code, b in self._buurt_data.items()
            },
        }
        try:
            with cache_path.open("w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False)
        except IOError:
            pass

    def _fetch_from_cbs(self) -> None:
        """Fetch buurt data from CBS OData API."""
        # Build filter for target municipalities
        code_filters = []
        for muni_code in TARGET_MUNICIPALITIES:
            code_filters.append(f"startswith(Codering_3,'BU{muni_code}')")

        params = {
            "$filter": " or ".join(code_filters),
        }

        base_url = f"{CBS_API_BASE}/{DATASET_KERNCIJFERS}/TypedDataSet"
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
                raise RuntimeError(f"Failed to fetch CBS buurt data: {exc}") from exc

            records.extend(data.get("value", []))
            url = data.get("odata.nextLink") or data.get("@odata.nextLink")

        # Parse records into BuurtData objects
        for record in records:
            buurt_code = str(record.get("Codering_3", "")).strip()
            if not buurt_code.startswith("BU"):
                continue  # Skip non-buurt records (wijken, gemeentes)

            # Extract gemeente code from buurt code (BU0518xx -> 0518)
            gemeente_code = buurt_code[2:6] if len(buurt_code) >= 6 else ""

            # Parse WOZ value (stored as x1000 euro)
            woz_raw = record.get(HOUSING_COLUMNS["woz_waarde"])
            gem_woz = None
            if woz_raw is not None:
                try:
                    # CBS stores WOZ in thousands, convert to actual value
                    gem_woz = int(float(woz_raw) * 1000)
                except (ValueError, TypeError):
                    pass

            # Parse percentages
            def parse_pct(key: str) -> Optional[float]:
                val = record.get(HOUSING_COLUMNS.get(key, ""))
                if val is not None:
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        pass
                return None

            # Parse income (x1000 euro)
            inkomen_raw = record.get(HOUSING_COLUMNS["gem_inkomen"])
            gem_inkomen = None
            if inkomen_raw is not None:
                try:
                    gem_inkomen = int(float(inkomen_raw) * 1000)
                except (ValueError, TypeError):
                    pass

            # Parse all extended indicators
            indicatoren = {}
            for indicator_key, cbs_column in HOUSING_COLUMNS.items():
                # Skip core fields already handled above
                if indicator_key in ("woz_waarde", "gem_inkomen", "koopwoningen_pct",
                                     "huurwoningen_pct", "bouwjaar_voor_2000_pct",
                                     "bouwjaar_vanaf_2000_pct", "huishoudens_laag_inkomen_pct"):
                    continue

                raw_val = record.get(cbs_column)
                if raw_val is not None:
                    try:
                        indicatoren[indicator_key] = float(raw_val)
                    except (ValueError, TypeError):
                        pass

            buurt = BuurtData(
                buurt_code=buurt_code,
                buurt_naam=str(record.get("WijkenEnBuurten", "")).strip(),
                gemeente_code=gemeente_code,
                gemeente_naam=str(record.get("Gemeentenaam_1", "")).strip(),
                gem_woz_waarde=gem_woz,
                koopwoningen_pct=parse_pct("koopwoningen_pct"),
                huurwoningen_pct=parse_pct("huurwoningen_pct"),
                bouwjaar_voor_2000_pct=parse_pct("bouwjaar_voor_2000_pct"),
                bouwjaar_vanaf_2000_pct=parse_pct("bouwjaar_vanaf_2000_pct"),
                gem_inkomen=gem_inkomen,
                huishoudens_laag_inkomen_pct=parse_pct("huishoudens_laag_inkomen_pct"),
                indicatoren=indicatoren,
            )

            self._buurt_data[buurt_code] = buurt

        self._loaded = True
        self._save_to_cache()

    def _ensure_loaded(self) -> None:
        """Ensure buurt data is loaded (from cache or CBS)."""
        if self._loaded:
            return

        if not self._load_from_cache():
            self._fetch_from_cbs()

    def get_buurt(self, buurt_code: str) -> Optional[BuurtData]:
        """
        Get buurt data by buurt code.

        Parameters
        ----------
        buurt_code : str
            CBS buurt code (e.g., "BU05180001")

        Returns
        -------
        BuurtData or None
            Neighborhood data if found
        """
        self._ensure_loaded()

        # Normalize code
        code = buurt_code.upper().strip()
        if not code.startswith("BU"):
            code = f"BU{code}"

        return self._buurt_data.get(code)

    def get_buurt_by_name(self, name: str, gemeente: Optional[str] = None) -> Optional[BuurtData]:
        """
        Find buurt by name (partial match).

        Parameters
        ----------
        name : str
            Buurt name to search for
        gemeente : str, optional
            Filter by gemeente name

        Returns
        -------
        BuurtData or None
            First matching neighborhood
        """
        self._ensure_loaded()

        name_lower = name.lower()
        gemeente_lower = gemeente.lower() if gemeente else None

        for buurt in self._buurt_data.values():
            if name_lower in buurt.buurt_naam.lower():
                if gemeente_lower is None or gemeente_lower in buurt.gemeente_naam.lower():
                    return buurt

        return None

    def get_all_buurten(self, gemeente_code: Optional[str] = None) -> List[BuurtData]:
        """
        Get all buurten, optionally filtered by gemeente.

        Parameters
        ----------
        gemeente_code : str, optional
            Filter by gemeente code (e.g., "0518" for Den Haag)

        Returns
        -------
        List[BuurtData]
            All matching neighborhoods
        """
        self._ensure_loaded()

        if gemeente_code:
            code = gemeente_code.strip()
            return [b for b in self._buurt_data.values() if b.gemeente_code == code]

        return list(self._buurt_data.values())

    def get_gemeente_average_woz(self, gemeente_code: str) -> Optional[int]:
        """
        Calculate average WOZ for a gemeente from buurt data.

        Parameters
        ----------
        gemeente_code : str
            Gemeente code (e.g., "0518")

        Returns
        -------
        int or None
            Average WOZ value across all buurten
        """
        buurten = self.get_all_buurten(gemeente_code)
        woz_values = [b.gem_woz_waarde for b in buurten if b.gem_woz_waarde]

        if woz_values:
            return int(sum(woz_values) / len(woz_values))
        return None


def lookup_buurt_code_pdok(postcode: str, huisnummer: int) -> Optional[str]:
    """
    Look up buurt code for an address using PDOK Locatieserver.

    Parameters
    ----------
    postcode : str
        Dutch postcode (e.g., "2511AB")
    huisnummer : int
        House number

    Returns
    -------
    str or None
        CBS buurt code (e.g., "BU05180001") if found
    """
    # Normalize postcode
    pc = postcode.replace(" ", "").upper()

    # Query PDOK Locatieserver
    url = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"
    params = {
        "q": f"{pc} {huisnummer}",
        "fq": "type:adres",
        "rows": 1,
        "fl": "buurtcode,buurtnaam,wijkcode,wijknaam,gemeentecode,gemeentenaam",
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        docs = data.get("response", {}).get("docs", [])
        if docs:
            buurt_code = docs[0].get("buurtcode")
            if buurt_code:
                # PDOK returns code without BU prefix, CBS uses BU prefix
                if not buurt_code.startswith("BU"):
                    buurt_code = f"BU{buurt_code}"
                return buurt_code

    except requests.RequestException:
        pass

    return None


def create_cbs_buurt_collector() -> CBSBuurtCollector:
    """Create a CBS buurt collector instance."""
    return CBSBuurtCollector()
