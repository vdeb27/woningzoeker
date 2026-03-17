"""
DUO School Collector — haalt schooldata op via de DUO CKAN API.

Datasets:
- PO locaties (adressen_bo) — basisscholen met adres en denominatie
- VO locaties (adressen_vo) — middelbare scholen met onderwijsstructuur
- PO leerlingen (poleerlingen-v1) — leerlingaantallen per vestiging
- PO schooladviezen (wpoadvies-v1) — kwaliteitsproxy: advies per leerling
- PO eindscores (wpo-eindscores) — gemiddelde doorstroomtoets
- VO examencijfers (03_voex-v1) — slagingspercentage + gem. cijfer
- Inspectie oordelen (oordelen-v02) — EindoordeelKwaliteit

Filtert op target gemeenten uit config/areas.yaml.
Geocoding via PDOK Locatieserver.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml

logger = logging.getLogger(__name__)

CKAN_BASE = "https://onderwijsdata.duo.nl/api/3/action"
PDOK_GEOCODE_URL = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"

# DUO CKAN package IDs
PACKAGES = {
    "po_locaties": "adressen_bo",
    "vo_locaties": "adressen_vo",
    "po_leerlingen": "poleerlingen-v1",
    "po_adviezen": "wpoadvies-v1",
    "po_eindscores": "wpo-eindscores",
    "vo_examens": "03_voex-v1",
    "inspectie": "oordelen-v02",
}

# Mapping gemeentenamen (areas.yaml → DUO CKAN)
GEMEENTE_MAPPING = {
    "Den Haag": "'S-GRAVENHAGE",
    "Leidschendam-Voorburg": "LEIDSCHENDAM-VOORBURG",
    "Rijswijk": "RIJSWIJK",
}

# PO adviescodes → categorie
# Codes van DUO: 1=VSO, 2=PRO, 3=VMBO-BB, 4=VMBO-BB/KB, 5=VMBO-KB,
# 6=VMBO-KB/GL/TL, 7=VMBO-GL/TL, 8=VMBO-GL/TL/HAVO, 9=HAVO,
# 10=HAVO/VWO, 11=VWO
HAVO_VWO_CODES = {9, 10, 11}  # HAVO, HAVO/VWO, VWO


@dataclass
class SchoolInfo:
    """Schoolinformatie inclusief kwaliteitsdata."""

    brin: str
    vestigingsnummer: str
    naam: str
    type: str  # "basisonderwijs" | "voortgezet"
    straat: str
    postcode: str
    plaats: str
    gemeente: str
    denominatie: str
    onderwijstype: Optional[str] = None  # VO: "vmbo"/"havo"/"vwo"/combinaties
    leerlingen: Optional[int] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    # Kwaliteit PO
    advies_havo_vwo_pct: Optional[float] = None
    gem_eindtoets: Optional[float] = None
    # Kwaliteit VO
    slagingspercentage: Optional[float] = None
    gem_examencijfer: Optional[float] = None
    # Inspectie
    inspectie_oordeel: Optional[str] = None

    @property
    def brin6(self) -> str:
        """BRIN + vestigingsnummer voor joins."""
        return f"{self.brin}{self.vestigingsnummer}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "brin": self.brin,
            "vestigingsnummer": self.vestigingsnummer,
            "naam": self.naam,
            "type": self.type,
            "straat": self.straat,
            "postcode": self.postcode,
            "plaats": self.plaats,
            "gemeente": self.gemeente,
            "denominatie": self.denominatie,
            "onderwijstype": self.onderwijstype,
            "leerlingen": self.leerlingen,
            "lat": self.lat,
            "lng": self.lng,
            "advies_havo_vwo_pct": self.advies_havo_vwo_pct,
            "gem_eindtoets": self.gem_eindtoets,
            "slagingspercentage": self.slagingspercentage,
            "gem_examencijfer": self.gem_examencijfer,
            "inspectie_oordeel": self.inspectie_oordeel,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchoolInfo":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class DUOSchoolCollector:
    """Collector voor DUO schooldata via CKAN API."""

    cache_dir: Path = field(default_factory=lambda: Path("data/cache/duo"))
    cache_days: int = 90
    geocode_delay: float = 0.5
    api_delay: float = 0.3
    session: Optional[requests.Session] = field(default=None, repr=False)

    # Runtime state
    _resource_ids: Dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _gemeenten: List[str] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self):
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "Woningzoeker/1.0 (schooldata collector)",
            })
        self._load_gemeenten()

    def _load_gemeenten(self):
        """Laad target gemeenten uit config/areas.yaml."""
        config_path = Path(__file__).parent.parent.parent / "config" / "areas.yaml"
        if config_path.exists():
            with open(config_path) as f:
                config = yaml.safe_load(f)
            for m in config.get("municipalities", []):
                name = m["name"]
                duo_name = GEMEENTE_MAPPING.get(name, name.upper())
                self._gemeenten.append(duo_name)
        if not self._gemeenten:
            self._gemeenten = list(GEMEENTE_MAPPING.values())
        logger.info(f"Target gemeenten (DUO): {self._gemeenten}")

    def _cache_path(self, key: str) -> Path:
        safe_key = key.replace("/", "_").replace(":", "_").replace("'", "")
        return self.cache_dir / f"{safe_key}.json"

    def _load_from_cache(self, key: str) -> Optional[Any]:
        path = self._cache_path(key)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
            cached_at = datetime.fromisoformat(data.get("_cached_at", "2000-01-01"))
            age_days = (datetime.now() - cached_at).days
            if age_days > self.cache_days:
                return None
            return data.get("data")
        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    def _save_to_cache(self, key: str, data: Any):
        path = self._cache_path(key)
        payload = {
            "_cached_at": datetime.now().isoformat(),
            "data": data,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, default=str))

    def _rate_limit(self, delay: Optional[float] = None):
        time.sleep(delay or self.api_delay)

    # ── CKAN API helpers ──

    def _get_resource_id(self, package_name: str) -> Optional[str]:
        """Haal resource_id op via package_show."""
        if package_name in self._resource_ids:
            return self._resource_ids[package_name]

        cache_key = f"resource_id_{package_name}"
        cached = self._load_from_cache(cache_key)
        if cached:
            self._resource_ids[package_name] = cached
            return cached

        url = f"{CKAN_BASE}/package_show"
        try:
            self._rate_limit()
            resp = self.session.get(url, params={"id": package_name}, timeout=30)
            resp.raise_for_status()
            result = resp.json()
            if result.get("success") and result.get("result", {}).get("resources"):
                resource_id = result["result"]["resources"][0]["id"]
                self._resource_ids[package_name] = resource_id
                self._save_to_cache(cache_key, resource_id)
                logger.info(f"Resource ID voor {package_name}: {resource_id}")
                return resource_id
        except Exception as e:
            logger.error(f"Fout bij ophalen resource_id voor {package_name}: {e}")
        return None

    def _datastore_search(
        self,
        resource_id: str,
        filters: Optional[Dict[str, str]] = None,
        fields: Optional[List[str]] = None,
        limit: int = 500,
        offset: int = 0,
    ) -> List[Dict]:
        """Voer een CKAN datastore_search uit met paginatie."""
        url = f"{CKAN_BASE}/datastore_search"
        params: Dict[str, Any] = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
        }
        if filters:
            params["filters"] = json.dumps(filters)
        if fields:
            params["fields"] = ",".join(fields)

        try:
            self._rate_limit()
            resp = self.session.get(url, params=params, timeout=60)
            resp.raise_for_status()
            result = resp.json()
            if result.get("success"):
                return result.get("result", {}).get("records", [])
        except Exception as e:
            logger.error(f"datastore_search fout: {e}")
        return []

    def _fetch_all_records(
        self,
        package_name: str,
        filters: Optional[Dict[str, str]] = None,
        fields: Optional[List[str]] = None,
        cache_key: Optional[str] = None,
    ) -> List[Dict]:
        """Haal alle records op met paginatie, optioneel gefilterd."""
        if cache_key:
            cached = self._load_from_cache(cache_key)
            if cached is not None:
                logger.info(f"Cache hit: {cache_key} ({len(cached)} records)")
                return cached

        resource_id = self._get_resource_id(package_name)
        if not resource_id:
            logger.error(f"Geen resource_id voor {package_name}")
            return []

        all_records = []
        offset = 0
        limit = 500

        while True:
            records = self._datastore_search(
                resource_id, filters=filters, fields=fields,
                limit=limit, offset=offset,
            )
            if not records:
                break
            all_records.extend(records)
            logger.info(f"  {package_name}: {len(all_records)} records opgehaald...")
            if len(records) < limit:
                break
            offset += limit

        if cache_key and all_records:
            self._save_to_cache(cache_key, all_records)

        return all_records

    # ── Data ophalen ──

    def _fetch_po_locaties(self) -> List[Dict]:
        """Haal PO locaties op per gemeente."""
        all_records = []
        for gemeente in self._gemeenten:
            cache_key = f"po_locaties_{gemeente}"
            records = self._fetch_all_records(
                PACKAGES["po_locaties"],
                filters={"GEMEENTENAAM": gemeente},
                cache_key=cache_key,
            )
            all_records.extend(records)
            logger.info(f"PO locaties {gemeente}: {len(records)} scholen")
        return all_records

    def _fetch_vo_locaties(self) -> List[Dict]:
        """Haal VO locaties op per gemeente."""
        all_records = []
        for gemeente in self._gemeenten:
            cache_key = f"vo_locaties_{gemeente}"
            records = self._fetch_all_records(
                PACKAGES["vo_locaties"],
                filters={"GEMEENTENAAM": gemeente},
                cache_key=cache_key,
            )
            all_records.extend(records)
            logger.info(f"VO locaties {gemeente}: {len(records)} scholen")
        return all_records

    def _fetch_po_leerlingen(self, brins: set) -> Dict[str, int]:
        """Haal PO leerlingaantallen op. Retourneert dict brin6 → leerlingen."""
        cache_key = "po_leerlingen_all"
        records = self._fetch_all_records(
            PACKAGES["po_leerlingen"],
            cache_key=cache_key,
        )

        # Neem meest recente jaar per vestiging
        year_data: Dict[str, tuple] = {}  # brin6 → (jaar, aantal)

        for r in records:
            brin = str(r.get("BRIN_NUMMER", "")).strip()
            vest = str(r.get("VESTIGINGSNUMMER", "")).strip().zfill(2)
            brin6 = f"{brin}{vest}"
            if brin6 not in brins:
                continue

            jaar = r.get("PEILDATUM_LEERLINGEN", "") or r.get("PEILJAAR", "")
            aantal = r.get("TOTAAL", 0) or r.get("LEERLINGEN", 0)
            try:
                aantal = int(aantal)
            except (ValueError, TypeError):
                continue

            prev = year_data.get(brin6, ("", 0))
            if str(jaar) >= str(prev[0]):
                year_data[brin6] = (str(jaar), aantal)

        leerlingen: Dict[str, int] = {}
        for brin6, (_, aantal) in year_data.items():
            leerlingen[brin6] = aantal

        logger.info(f"PO leerlingen: {len(leerlingen)} vestigingen met data")
        return leerlingen

    def _fetch_po_adviezen(self, brins: set) -> Dict[str, float]:
        """Haal PO schooladviezen op. Retourneert dict brin6 → % HAVO/VWO advies."""
        cache_key = "po_adviezen_all"
        records = self._fetch_all_records(
            PACKAGES["po_adviezen"],
            cache_key=cache_key,
        )

        # Verzamel per vestiging per jaar
        vest_data: Dict[str, Dict[str, list]] = {}

        for r in records:
            brin = str(r.get("BRIN_NUMMER", "")).strip()
            vest = str(r.get("VESTIGINGSNUMMER", "")).strip().zfill(2)
            brin6 = f"{brin}{vest}"
            if brin6 not in brins:
                continue

            jaar = str(r.get("SCHOOLJAAR", "") or r.get("JAAR", ""))
            advies = r.get("ADVIES", r.get("ADVIES_SCORE", None))
            try:
                advies = int(advies)
            except (ValueError, TypeError):
                continue

            vest_data.setdefault(brin6, {}).setdefault(jaar, []).append(advies)

        # Bereken % HAVO/VWO voor meest recente jaar
        result: Dict[str, float] = {}
        for brin6, jaren in vest_data.items():
            latest = max(jaren.keys())
            codes = jaren[latest]
            if codes:
                havo_vwo = sum(1 for c in codes if c in HAVO_VWO_CODES)
                result[brin6] = round(100 * havo_vwo / len(codes), 1)

        logger.info(f"PO adviezen: {len(result)} vestigingen met HAVO/VWO %")
        return result

    def _fetch_po_eindscores(self, brins: set) -> Dict[str, float]:
        """Haal PO eindscores op. Retourneert dict brin6 → gem. eindtoets."""
        cache_key = "po_eindscores_all"
        records = self._fetch_all_records(
            PACKAGES["po_eindscores"],
            cache_key=cache_key,
        )

        vest_data: Dict[str, Dict[str, float]] = {}

        for r in records:
            brin = str(r.get("BRIN_NUMMER", "") or r.get("INSTELLINGSCODE", "")).strip()
            vest = str(r.get("VESTIGINGSNUMMER", "")).strip().zfill(2)
            brin6 = f"{brin}{vest}"
            if brin6 not in brins:
                continue

            jaar = str(r.get("SCHOOLJAAR", "") or r.get("JAAR", ""))
            score = r.get("GEMIDDELDE_UITSLAG", r.get("GEM_SCORE", None))
            try:
                score = float(score)
            except (ValueError, TypeError):
                continue

            vest_data.setdefault(brin6, {})[jaar] = score

        result: Dict[str, float] = {}
        for brin6, jaren in vest_data.items():
            latest = max(jaren.keys())
            result[brin6] = round(jaren[latest], 1)

        logger.info(f"PO eindscores: {len(result)} vestigingen met score")
        return result

    def _fetch_vo_examens(self, brins: set) -> Dict[str, Dict[str, float]]:
        """Haal VO examencijfers op. Retourneert dict brin6 → {slaag%, gem_cijfer}."""
        cache_key = "vo_examens_all"
        records = self._fetch_all_records(
            PACKAGES["vo_examens"],
            cache_key=cache_key,
        )

        vest_data: Dict[str, Dict[str, list]] = {}

        for r in records:
            brin = str(r.get("BRIN_NUMMER", "") or r.get("INSTELLINGSCODE", "")).strip()
            vest = str(r.get("VESTIGINGSNUMMER", "")).strip().zfill(2)
            brin6 = f"{brin}{vest}"
            if brin6 not in brins:
                continue

            jaar = str(r.get("SCHOOLJAAR", "") or r.get("JAAR", ""))

            slaag = r.get("SLAAGPERCENTAGE", r.get("GESLAAGD_PCT", None))
            cijfer = r.get("GEM_CIJFER_CE", r.get("GEMIDDELD_CIJFER_CENTRAAL_EXAMEN", None))

            try:
                slaag = float(slaag) if slaag is not None else None
            except (ValueError, TypeError):
                slaag = None
            try:
                cijfer = float(cijfer) if cijfer is not None else None
            except (ValueError, TypeError):
                cijfer = None

            if slaag is not None or cijfer is not None:
                vest_data.setdefault(brin6, {}).setdefault(jaar, []).append(
                    (slaag, cijfer)
                )

        result: Dict[str, Dict[str, float]] = {}
        for brin6, jaren in vest_data.items():
            latest = max(jaren.keys())
            entries = jaren[latest]
            slaag_vals = [s for s, _ in entries if s is not None]
            cijfer_vals = [c for _, c in entries if c is not None]
            d: Dict[str, float] = {}
            if slaag_vals:
                d["slagingspercentage"] = round(sum(slaag_vals) / len(slaag_vals), 1)
            if cijfer_vals:
                d["gem_examencijfer"] = round(sum(cijfer_vals) / len(cijfer_vals), 2)
            if d:
                result[brin6] = d

        logger.info(f"VO examens: {len(result)} vestigingen met data")
        return result

    def _fetch_inspectie(self, brins: set) -> Dict[str, str]:
        """Haal inspectie oordelen op. Retourneert dict brin6 → oordeel."""
        cache_key = "inspectie_all"
        records = self._fetch_all_records(
            PACKAGES["inspectie"],
            cache_key=cache_key,
        )

        vest_data: Dict[str, Dict[str, str]] = {}

        for r in records:
            brin = str(r.get("BRIN_NUMMER", "") or r.get("INSTELLINGSCODE", "")).strip()
            vest = str(r.get("VESTIGINGSNUMMER", "")).strip().zfill(2) if r.get("VESTIGINGSNUMMER") else "00"
            brin6 = f"{brin}{vest}"

            jaar = str(r.get("JAAR_VAN_OORDEEL", "") or r.get("DATUM", ""))
            oordeel = r.get("EINDBEOORDELING", r.get("EindoordeelKwaliteit", ""))

            if oordeel and isinstance(oordeel, str) and oordeel.strip():
                vest_data.setdefault(brin6, {})[jaar] = oordeel.strip()

        result: Dict[str, str] = {}
        for brin6, jaren in vest_data.items():
            latest = max(jaren.keys())
            result[brin6] = jaren[latest]

        # Also store by brin4 (without vestiging) as fallback
        for brin6, oordeel in list(result.items()):
            brin4 = brin6[:4]
            if brin4 not in result:
                result[brin4] = oordeel

        logger.info(f"Inspectie: {len(result)} oordelen")
        return result

    # ── Geocoding ──

    def _geocode_address(self, straat: str, postcode: str, plaats: str) -> Optional[Dict[str, float]]:
        """Geocode een adres via PDOK Locatieserver."""
        query = f"{straat}, {postcode} {plaats}"
        cache_key = f"geo_{postcode}_{straat.replace(' ', '_')}"
        cached = self._load_from_cache(cache_key)
        if cached:
            return cached

        try:
            time.sleep(self.geocode_delay)
            resp = self.session.get(
                PDOK_GEOCODE_URL,
                params={"q": query, "rows": 1, "fq": "type:adres"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            docs = data.get("response", {}).get("docs", [])
            if docs:
                centroide = docs[0].get("centroide_ll", "")
                if centroide.startswith("POINT("):
                    coords = centroide.replace("POINT(", "").replace(")", "").split()
                    result = {"lng": float(coords[0]), "lat": float(coords[1])}
                    self._save_to_cache(cache_key, result)
                    return result
        except Exception as e:
            logger.debug(f"Geocoding mislukt voor {query}: {e}")

        return None

    # ── Hoofdmethode ──

    def fetch_all(self) -> List[SchoolInfo]:
        """Haal alle schooldata op, verrijk met kwaliteitsdata en geocoding."""
        # Check overall cache
        cached = self._load_from_cache("all_schools")
        if cached:
            logger.info(f"Alle scholen uit cache: {len(cached)}")
            return [SchoolInfo.from_dict(s) for s in cached]

        logger.info("=== DUO Schooldata ophalen ===")

        # 1. Locatiedata ophalen
        po_records = self._fetch_po_locaties()
        vo_records = self._fetch_vo_locaties()

        logger.info(f"Locaties: {len(po_records)} PO, {len(vo_records)} VO")

        # 2. Parse locaties naar SchoolInfo
        schools: Dict[str, SchoolInfo] = {}

        for r in po_records:
            brin = str(r.get("BRIN_NUMMER", "") or r.get("INSTELLINGSCODE", "")).strip()
            vest = str(r.get("VESTIGINGSNUMMER", "")).strip().zfill(2)
            if not brin:
                continue
            brin6 = f"{brin}{vest}"

            schools[brin6] = SchoolInfo(
                brin=brin,
                vestigingsnummer=vest,
                naam=str(r.get("INSTELLINGSNAAM", "") or r.get("NAAM", "")).strip(),
                type="basisonderwijs",
                straat=str(r.get("STRAATNAAM", "") or "").strip(),
                postcode=str(r.get("POSTCODE", "") or "").strip(),
                plaats=str(r.get("PLAATSNAAM", "") or "").strip(),
                gemeente=str(r.get("GEMEENTENAAM", "") or "").strip(),
                denominatie=str(r.get("DENOMINATIE", "") or "").strip(),
            )

        for r in vo_records:
            brin = str(r.get("BRIN_NUMMER", "") or r.get("INSTELLINGSCODE", "")).strip()
            vest = str(r.get("VESTIGINGSNUMMER", "")).strip().zfill(2)
            if not brin:
                continue
            brin6 = f"{brin}{vest}"

            onderwijs = str(r.get("ONDERWIJSSTRUCTUUR", "") or "").strip().lower()

            schools[brin6] = SchoolInfo(
                brin=brin,
                vestigingsnummer=vest,
                naam=str(r.get("INSTELLINGSNAAM", "") or r.get("NAAM", "")).strip(),
                type="voortgezet",
                straat=str(r.get("STRAATNAAM", "") or "").strip(),
                postcode=str(r.get("POSTCODE", "") or "").strip(),
                plaats=str(r.get("PLAATSNAAM", "") or "").strip(),
                gemeente=str(r.get("GEMEENTENAAM", "") or "").strip(),
                denominatie=str(r.get("DENOMINATIE", "") or "").strip(),
                onderwijstype=onderwijs if onderwijs else None,
            )

        logger.info(f"Unieke scholen: {len(schools)}")

        if not schools:
            return []

        brins = set(schools.keys())
        po_brins = {b for b, s in schools.items() if s.type == "basisonderwijs"}
        vo_brins = {b for b, s in schools.items() if s.type == "voortgezet"}

        # 3. Kwaliteitsdata ophalen
        logger.info("Kwaliteitsdata ophalen...")

        po_leerlingen = self._fetch_po_leerlingen(po_brins) if po_brins else {}
        po_adviezen = self._fetch_po_adviezen(po_brins) if po_brins else {}
        po_eindscores = self._fetch_po_eindscores(po_brins) if po_brins else {}
        vo_examens = self._fetch_vo_examens(vo_brins) if vo_brins else {}
        inspectie = self._fetch_inspectie(brins)

        # 4. Verrijken
        for brin6, school in schools.items():
            if school.type == "basisonderwijs":
                school.leerlingen = po_leerlingen.get(brin6)
                school.advies_havo_vwo_pct = po_adviezen.get(brin6)
                school.gem_eindtoets = po_eindscores.get(brin6)
            else:
                examen = vo_examens.get(brin6, {})
                school.slagingspercentage = examen.get("slagingspercentage")
                school.gem_examencijfer = examen.get("gem_examencijfer")

            # Inspectie: probeer brin6, anders brin4 fallback
            school.inspectie_oordeel = inspectie.get(brin6) or inspectie.get(school.brin)

        # 5. Geocoding
        logger.info("Geocoding adressen...")
        geocoded = 0
        for school in schools.values():
            if school.straat and school.postcode:
                coords = self._geocode_address(school.straat, school.postcode, school.plaats)
                if coords:
                    school.lat = coords["lat"]
                    school.lng = coords["lng"]
                    geocoded += 1
        logger.info(f"Geocoded: {geocoded}/{len(schools)} scholen")

        result = list(schools.values())

        # Cache opslaan
        self._save_to_cache("all_schools", [s.to_dict() for s in result])

        return result


def create_duo_school_collector(cache_dir: Optional[Path] = None) -> DUOSchoolCollector:
    """Factory function met default cache directory."""
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "duo"
    return DUOSchoolCollector(cache_dir=cache_dir)
