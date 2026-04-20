"""
CBS Buurt (Neighborhood) Data Collector.

Fetches neighborhood-level statistics from CBS "Kerncijfers wijken en buurten" (86165NED).
Extended to ~80 indicators across categories: bevolking, herkomst, woningen, woningtypen,
energie, inkomen, uitkeringen, arbeid, opleiding, onderwijs, zorg, bedrijven, motorvoertuigen,
voorzieningen.
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
DATASET_KERNCIJFERS = "86165NED"  # Kerncijfers wijken en buurten 2025
DATASET_FALLBACK = "85618NED"  # Kerncijfers wijken en buurten 2024 (meest complete recente data)

# Cache settings
# Fallback column mapping for 2024 dataset (85618NED)
# The 2025 dataset is missing socio-economic data; 2024 has it with different suffixes
FALLBACK_COLUMNS = {
    # Energie
    "gem_elektraverbruik": "GemiddeldeElektriciteitsleveringTotaal_48",
    "gem_gasverbruik": "GemiddeldAardgasverbruikTotaal_56",
    "stadsverwarming_pct": "PercentageWoningenMetStadsverwarming_64",
    # Onderwijs
    "leerlingen_po": "LeerlingenPo_65",
    "leerlingen_vo": "LeerlingenVoInclVavo_66",
    "studenten_mbo": "StudentenMboExclExtranei_67",
    "studenten_hbo": "StudentenHbo_68",
    "studenten_wo": "StudentenWo_69",
    # Arbeid
    "netto_arbeidsparticipatie": "Nettoarbeidsparticipatie_74",
    "werknemers_pct": "PercentageWerknemers_75",
    "zelfstandigen_pct": "PercentageZelfstandigen_78",
    # Inkomen
    "aantal_inkomensontvangers": "AantalInkomensontvangers_79",
    "gem_inkomen_ontvanger": "GemiddeldInkomenPerInkomensontvanger_80",
    "gem_inkomen": "GemiddeldInkomenPerInwoner_81",
    "personen_laagste_inkomen_pct": "k_40PersonenMetLaagsteInkomen_82",
    "gem_gestandaardiseerd_inkomen": "GemGestandaardiseerdInkomenVanHuish_84",
    "huishoudens_laag_inkomen_pct": "HuishoudensMetEenLaagInkomen_87",
    "mediaan_vermogen": "MediaanVermogenVanParticuliereHuish_91",
    # Uitkeringen
    "bijstandsuitkeringen_per_1000": "PersonenPerSoortUitkeringBijstand_92",
    "ao_uitkeringen_per_1000": "PersonenPerSoortUitkeringAO_93",
    "ww_uitkeringen_per_1000": "PersonenPerSoortUitkeringWW_94",
    "aow_uitkeringen_per_1000": "PersonenPerSoortUitkeringAOW_95",
    # Jeugdzorg & WMO
    "jongeren_jeugdzorg": "JongerenMetJeugdzorgInNatura_96",
    "jongeren_jeugdzorg_pct": "PercentageJongerenMetJeugdzorg_97",
    "wmo_clienten": "WmoClienten_98",
    "wmo_clienten_relatief": "WmoClientenRelatief_99",
    # Bedrijven
    "bedrijfsvestigingen_totaal": "BedrijfsvestigingenTotaal_100",
    "bedrijven_landbouw": "ALandbouwBosbouwEnVisserij_101",
    "bedrijven_nijverheid": "BFNijverheidEnEnergie_102",
    "bedrijven_handel_horeca": "GIHandelEnHoreca_103",
    "bedrijven_vervoer_ict": "HJVervoerInformatieEnCommunicatie_104",
    "bedrijven_financieel": "KLFinancieleDienstenOnroerendGoed_105",
    "bedrijven_zakelijk": "MNZakelijkeDienstverlening_106",
    "bedrijven_overheid_onderwijs_zorg": "OQOverheidOnderwijsEnZorg_107",
    "bedrijven_cultuur_recreatie": "RUCultuurRecreatieOverigeDiensten_108",
    # Geboorte & Sterfte
    "geboorte_totaal": "GeboorteTotaal_25",
    "geboorte_relatief": "GeboorteRelatief_26",
    "sterfte_totaal": "SterfteTotaal_27",
    "sterfte_relatief": "SterfteRelatief_28",
}

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CACHE_DURATION_SECONDS = 30 * 24 * 60 * 60  # 30 days (yearly dataset)

# Target municipalities
TARGET_MUNICIPALITIES = ["0518", "1916", "0603"]  # Den Haag, Leidschendam-Voorburg, Rijswijk

# CBS column names for housing indicators - ~80 indicators
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
    "bevolkingsdichtheid": "Bevolkingsdichtheid_34",
    "huishoudens_totaal": "HuishoudensTotaal_29",
    "eenpersoons_huishoudens": "Eenpersoonshuishoudens_30",
    "huishoudens_zonder_kinderen": "HuishoudensZonderKinderen_31",
    "huishoudens_met_kinderen": "HuishoudensMetKinderen_32",
    "gem_huishoudensgrootte": "GemiddeldeHuishoudensgrootte_33",
    "geboorte_totaal": "GeboorteTotaal_25",
    "geboorte_relatief": "GeboorteRelatief_26",
    "sterfte_totaal": "SterfteTotaal_27",
    "sterfte_relatief": "SterfteRelatief_28",

    # === Herkomst ===
    "herkomst_nederland": "Nederland_17",
    "herkomst_europa": "EuropaExclusiefNederland_18",
    "herkomst_buiten_europa": "BuitenEuropa_19",

    # === Woningen ===
    "woningvoorraad": "Woningvoorraad_35",
    "nieuwbouw_woningen": "NieuwbouwWoningen_36",
    "woz_waarde": "GemiddeldeWOZWaardeVanWoningen_39",
    "koopwoningen_pct": "Koopwoningen_47",
    "huurwoningen_pct": "HuurwoningenTotaal_48",
    "huur_corporatie_pct": "InBezitWoningcorporatie_49",
    "huur_overig_pct": "InBezitOverigeVerhuurders_50",
    "bouwjaar_oud_pct": "BouwjaarMeerDanTienJaarGeleden_51",
    "bouwjaar_nieuw_pct": "BouwjaarAfgelopenTienJaar_52",
    "onbewoonde_woningen": "OnbewoondeWoningen_46",

    # === Woningtypen ===
    "eengezinswoning_pct": "PercentageEengezinswoning_40",
    "tussenwoning_pct": "PercentageTussenwoningEengezins_41",
    "hoekwoning_pct": "PercentageHoekwoningEengezins_42",
    "twee_onder_een_kap_pct": "PercentageTweeOnderEenKapWoningEe_43",
    "vrijstaand_pct": "PercentageVrijstaandeWoningEengezins_44",
    "meergezinswoning_pct": "PercentageMeergezinswoning_45",

    # === Energie ===
    "gem_elektraverbruik": "GemiddeldeElektriciteitsleveringTotaal_53",
    "gem_elektra_teruglevering": "GemiddeldeElektriciteitsteruglevering_54",
    "gem_gasverbruik": "GemiddeldAardgasverbruikTotaal_55",
    "stadsverwarming_pct": "PercentageWoningenMetStadsverwarming_56",
    "aardgasvrije_woningen": "AardgasvrijeWoningen_57",
    "woningen_met_zonnestroom": "WoningenMetZonnestroom_59",
    "woningen_elektrisch_verwarmd": "WoningenHoofdzElektrischVerwarmd_60",
    "publieke_laadpalen": "AantalPubliekeLaadpalen_61",

    # === Onderwijs (leerlingen/studenten) ===
    "leerlingen_po": "LeerlingenPo_62",
    "leerlingen_vo": "LeerlingenVoInclVavo_63",
    "studenten_mbo": "StudentenMboExclExtranei_64",
    "studenten_hbo": "StudentenHbo_65",
    "studenten_wo": "StudentenWo_66",

    # === Opleidingsniveau ===
    "opleiding_laag_pct": "BasisonderwijsVmboMbo1_67",
    "opleiding_midden_pct": "HavoVwoMbo24_68",
    "opleiding_hoog_pct": "HboWo_69",

    # === Arbeid ===
    "werkzame_beroepsbevolking": "WerkzameBeroepsbevolking_70",
    "netto_arbeidsparticipatie": "Nettoarbeidsparticipatie_71",
    "werknemers_pct": "PercentageWerknemers_72",
    "werknemer_vast_pct": "WerknemersMetVasteArbeidsr_73",
    "werknemer_flex_pct": "WerknemersMetFlexibeleArbe_74",
    "zelfstandigen_pct": "PercentageZelfstandigen_75",

    # === Inkomen ===
    "aantal_inkomensontvangers": "AantalInkomensontvangers_76",
    "gem_inkomen_ontvanger": "GemiddeldInkomenPerInkomensontvanger_77",
    "gem_inkomen": "GemiddeldInkomenPerInwoner_78",
    "personen_laagste_inkomen_pct": "k_40PersonenMetLaagsteInkomen_79",
    "personen_boven_armoedegrens_pct": "PersonenTot25BovenArmoedegrens_82",
    "gem_gestandaardiseerd_inkomen": "GemGestandaardiseerdInkomen_83",
    "huishoudens_laag_inkomen_pct": "k_40HuishoudensMetLaagsteInkomen_84",
    "huishoudens_hoog_inkomen_pct": "k_20HuishoudensMetHoogsteInkomen_85",
    "mediaan_vermogen": "MediaanVermogenVanParticuliereHuish_86",

    # === Uitkeringen ===
    "bijstandsuitkeringen_per_1000": "PersonenPerSoortUitkeringBijstand_87",
    "ao_uitkeringen_per_1000": "PersonenPerSoortUitkeringAO_88",
    "ww_uitkeringen_per_1000": "PersonenPerSoortUitkeringWW_89",
    "aow_uitkeringen_per_1000": "PersonenPerSoortUitkeringAOW_90",

    # === Zorg ===
    "jongeren_jeugdzorg": "JongerenMetJeugdzorgInNatura_91",
    "jongeren_jeugdzorg_pct": "PercentageJongerenMetJeugdzorg_92",
    "wmo_clienten": "WmoClienten_93",
    "wmo_clienten_relatief": "WmoClientenRelatief_94",

    # === Bedrijven ===
    "bedrijfsvestigingen_totaal": "BedrijfsvestigingenTotaal_95",
    "bedrijven_landbouw": "ALandbouwBosbouwEnVisserij_96",
    "bedrijven_nijverheid": "BFNijverheidEnEnergie_97",
    "bedrijven_handel_horeca": "GIHandelEnHoreca_98",
    "bedrijven_vervoer_ict": "HJVervoerInformatieEnCommunicatie_99",
    "bedrijven_financieel": "KLFinancieleDienstenOnroerendGoed_100",
    "bedrijven_zakelijk": "MNZakelijkeDienstverlening_101",
    "bedrijven_overheid_onderwijs_zorg": "OQOverheidOnderwijsEnZorg_102",
    "bedrijven_cultuur_recreatie": "RUCultuurRecreatieOverigeDiensten_103",

    # === Motorvoertuigen ===
    "personenautos_totaal": "PersonenautoSTotaal_104",
    "personenautos_brandstof_benzine": "PersonenautoSBrandstofBenzine_105",
    "personenautos_brandstof_overig": "PersonenautoSOverigeBrandstof_106",
    "personenautos_per_huishouden": "PersonenautoSPerHuishouden_107",
    "motorfietsen": "Motorfietsen_109",

    # === Voorzieningen (afstanden in km) ===
    "afstand_huisartsenpraktijk": "AfstandTotHuisartsenpraktijk_110",
    "afstand_grote_supermarkt": "AfstandTotGroteSupermarkt_111",
    "afstand_kinderdagverblijf": "AfstandTotKinderdagverblijf_112",
    "afstand_school": "AfstandTotSchool_113",
    "scholen_binnen_3km": "ScholenBinnen3Km_114",

    # === Oppervlakte & stedelijkheid ===
    "oppervlakte_totaal": "OppervlakteTotaal_115",
    "oppervlakte_land": "OppervlakteLand_116",
    "oppervlakte_water": "OppervlakteWater_117",
    "stedelijkheid": "MateVanStedelijkheid_120",
    "omgevingsadressendichtheid": "Omgevingsadressendichtheid_121",
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
        """Fetch buurt data from CBS OData API.

        Uses cbsodata which handles pagination. CBS OData ignores $filter
        on Codering_3, so we fetch all records with $select and filter
        client-side.
        """
        import cbsodata

        select_cols = ["Codering_3", "WijkenEnBuurten", "Gemeentenaam_1"]
        select_cols.extend(HOUSING_COLUMNS.values())

        try:
            all_records = cbsodata.get_data(
                DATASET_KERNCIJFERS,
                select=select_cols,
            )
        except Exception as exc:
            raise RuntimeError(f"Failed to fetch CBS buurt data: {exc}") from exc

        # Parse all buurt records (filter to BU* only, skip WK*/GM* records)
        for record in all_records:
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
                                     "huurwoningen_pct", "bouwjaar_oud_pct",
                                     "bouwjaar_nieuw_pct", "huishoudens_laag_inkomen_pct"):
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
                bouwjaar_voor_2000_pct=parse_pct("bouwjaar_oud_pct"),
                bouwjaar_vanaf_2000_pct=parse_pct("bouwjaar_nieuw_pct"),
                gem_inkomen=gem_inkomen,
                huishoudens_laag_inkomen_pct=parse_pct("huishoudens_laag_inkomen_pct"),
                indicatoren=indicatoren,
            )

            self._buurt_data[buurt_code] = buurt

        # Fallback: fill missing income/gas/employment from 2022 dataset
        self._fill_from_fallback()

        self._loaded = True
        self._save_to_cache()

    def _fill_from_fallback(self) -> None:
        """Fill missing income/gas/employment data from older CBS dataset."""
        import cbsodata

        # Check if fallback is needed
        sample = next(iter(self._buurt_data.values()), None)
        if sample and sample.gem_inkomen is not None:
            return  # Primary dataset already has this data

        print(f"  Fallback: inkomen/gas/arbeid ophalen uit {DATASET_FALLBACK}...")
        select_cols = ["Codering_3"] + list(FALLBACK_COLUMNS.values())

        try:
            records = cbsodata.get_data(DATASET_FALLBACK, select=select_cols)
        except Exception as exc:
            print(f"  Fallback dataset laden mislukt: {exc}")
            return

        filled = 0
        for record in records:
            buurt_code = str(record.get("Codering_3", "")).strip()
            buurt = self._buurt_data.get(buurt_code)
            if not buurt:
                continue

            # Fill core fields if missing
            if buurt.gem_inkomen is None:
                raw = record.get(FALLBACK_COLUMNS["gem_inkomen"])
                if raw is not None:
                    try:
                        buurt.gem_inkomen = int(float(raw) * 1000)
                    except (ValueError, TypeError):
                        pass

            if buurt.huishoudens_laag_inkomen_pct is None:
                raw = record.get(FALLBACK_COLUMNS.get("huishoudens_laag_inkomen_pct", ""))
                if raw is not None:
                    try:
                        buurt.huishoudens_laag_inkomen_pct = float(raw)
                    except (ValueError, TypeError):
                        pass

            # Fill indicatoren if missing
            if buurt.indicatoren is None:
                buurt.indicatoren = {}

            for key, cbs_col in FALLBACK_COLUMNS.items():
                if key in ("gem_inkomen", "huishoudens_laag_inkomen_pct"):
                    continue  # Already handled above as core fields
                if key not in buurt.indicatoren or buurt.indicatoren[key] is None:
                    raw = record.get(cbs_col)
                    if raw is not None:
                        try:
                            buurt.indicatoren[key] = float(raw)
                            filled += 1
                        except (ValueError, TypeError):
                            pass

        print(f"  Fallback: {filled} ontbrekende waarden ingevuld")

    def _ensure_loaded(self) -> None:
        """Ensure buurt data is loaded (from cache or CBS)."""
        if self._loaded:
            return

        if not self._load_from_cache():
            self._fetch_from_cbs()

    def get_buurt(self, buurt_code: str) -> Optional[BuurtData]:
        """Get buurt data by buurt code."""
        self._ensure_loaded()
        code = buurt_code.upper().strip()
        if not code.startswith("BU"):
            code = f"BU{code}"
        return self._buurt_data.get(code)

    def get_buurt_by_name(self, name: str, gemeente: Optional[str] = None) -> Optional[BuurtData]:
        """Find buurt by name (partial match)."""
        self._ensure_loaded()
        name_lower = name.lower()
        gemeente_lower = gemeente.lower() if gemeente else None
        for buurt in self._buurt_data.values():
            if name_lower in buurt.buurt_naam.lower():
                if gemeente_lower is None or gemeente_lower in buurt.gemeente_naam.lower():
                    return buurt
        return None

    def get_all_buurten(self, gemeente_code: Optional[str] = None) -> List[BuurtData]:
        """Get all buurten, optionally filtered by gemeente."""
        self._ensure_loaded()
        if gemeente_code:
            code = gemeente_code.strip()
            return [b for b in self._buurt_data.values() if b.gemeente_code == code]
        return list(self._buurt_data.values())

    def get_gemeente_average_woz(self, gemeente_code: str) -> Optional[int]:
        """Calculate average WOZ for a gemeente from buurt data."""
        buurten = self.get_all_buurten(gemeente_code)
        woz_values = [b.gem_woz_waarde for b in buurten if b.gem_woz_waarde]
        if woz_values:
            return int(sum(woz_values) / len(woz_values))
        return None


def lookup_buurt_code_pdok(postcode: str, huisnummer: int) -> Optional[str]:
    """Look up buurt code for an address using PDOK Locatieserver."""
    from utils.pdok import geocode_pdok_full
    result = geocode_pdok_full(postcode, huisnummer)
    return result.buurt_code if result else None


def geocode_address_pdok(postcode: str, huisnummer: int) -> Optional[Dict[str, Any]]:
    """Geocode an address via PDOK Locatieserver, returning lat, lng, buurt_code, buurt_naam."""
    from utils.pdok import geocode_pdok_full
    result = geocode_pdok_full(postcode, huisnummer)
    if result is None:
        return None
    return {
        "lat": result.lat,
        "lng": result.lng,
        "buurt_code": result.buurt_code,
        "buurt_naam": result.buurt_naam,
    }


def create_cbs_buurt_collector() -> CBSBuurtCollector:
    """Create a CBS buurt collector instance."""
    return CBSBuurtCollector()
