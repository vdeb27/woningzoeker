"""
Bestemmingsplan Collector.

Haalt bestemmingsplan/omgevingsplan informatie op via de Ruimtelijke Plannen API v4
van het Digitaal Stelsel Omgevingswet (DSO).

Bron: https://ruimte.omgevingswet.overheid.nl/ruimtelijke-plannen/api/opvragen/v4
Vereist: DSO_API_KEY environment variable

Overgangsperiode (2024-2032): zowel oude bestemmingsplannen (IMRO) als nieuwe
omgevingsplannen worden ondersteund via dezelfde API.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import math

import requests

logger = logging.getLogger(__name__)

API_BASE = "https://ruimte.omgevingswet.overheid.nl/ruimtelijke-plannen/api/opvragen/v4"

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "bestemmingsplan"
CACHE_DIR_OMGEVING = CACHE_DIR / "omgeving"
CACHE_DURATION_SECONDS = 30 * 24 * 60 * 60  # 30 dagen

# Bekende plantypen (prioriteit: hoger = relevanter)
PLAN_TYPE_PRIORITY = {
    "bestemmingsplan": 10,
    "omgevingsplan": 10,
    "inpassingsplan": 8,
    "wijzigingsplan": 7,
    "uitwerkingsplan": 7,
    "beheersverordening": 6,
}

# Bekende maatvoering-namen en hun normalisatie
MAATVOERING_PATTERNS = {
    "max_bouwhoogte": re.compile(r"maxi\w*\s+bouwhoogte", re.IGNORECASE),
    "max_goothoogte": re.compile(r"maxi\w*\s+goothoogte", re.IGNORECASE),
    "max_bebouwingspercentage": re.compile(r"maxi\w*\s+bebouwings\s*percentage", re.IGNORECASE),
    "max_inhoud": re.compile(r"maxi\w*\s+inhoud", re.IGNORECASE),
    "max_oppervlakte": re.compile(r"maxi\w*\s+oppervlakte", re.IGNORECASE),
    "max_dakhelling": re.compile(r"maxi\w*\s+dakhelling", re.IGNORECASE),
    "min_dakhelling": re.compile(r"mini\w*\s+dakhelling", re.IGNORECASE),
    "max_bouwdiepte": re.compile(r"maxi\w*\s+bouwdiepte", re.IGNORECASE),
    "max_breedte": re.compile(r"maxi\w*\s+breedte", re.IGNORECASE),
    "min_oppervlakte": re.compile(r"mini\w*\s+oppervlakte", re.IGNORECASE),
}


# Bestemming categorisatie — substring matching (case-insensitive)
BESTEMMING_CATEGORIES: Dict[str, List[str]] = {
    "wonen": ["wonen", "woondoeleinden"],
    "groen": ["groen", "groenvoorzieningen", "natuur", "bos"],
    "verkeer": ["verkeer", "verkeers", "weg", "parkeren"],
    "water": ["water"],
    "bedrijven": ["bedrijv", "bedrijfsdoeleinden"],
    "maatschappelijk": ["maatschappelijk", "onderwijs", "gezondheidszorg"],
    "detailhandel": ["detailhandel", "winkel"],
    "horeca": ["horeca"],
    "recreatie": ["recreat", "sport"],
    "gemengd": ["gemengd", "centrum"],
    "agrarisch": ["agrarisch"],
    "tuin": ["tuin"],
}


def _categorize_bestemming(naam: str) -> str:
    """Categoriseer een bestemmingsnaam naar een standaard categorie."""
    naam_lower = naam.lower()
    for categorie, keywords in BESTEMMING_CATEGORIES.items():
        if any(kw in naam_lower for kw in keywords):
            return categorie
    return "overig"


class _HTMLTextExtractor(HTMLParser):
    """Eenvoudige XHTML → platte tekst converter."""

    def __init__(self):
        super().__init__()
        self._parts: List[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return " ".join(self._parts)


def _xhtml_to_text(xhtml: str) -> str:
    """Strip XHTML tags en retourneer platte tekst."""
    parser = _HTMLTextExtractor()
    parser.feed(xhtml)
    text = parser.get_text()
    # Normaliseer whitespace
    return re.sub(r"\s+", " ", text).strip()


@dataclass
class Maatvoering:
    """Dimensionele bouwregel uit het bestemmingsplan."""

    naam: str
    waarde: str
    eenheid: Optional[str] = None
    waarde_type: Optional[str] = None  # "exact", "maximaal", "minimaal"

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Maatvoering":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class Bouwvlak:
    """Bouwvlak geometrie en regels."""

    geometrie: Optional[Dict] = None  # GeoJSON
    maatvoeringen: List[Maatvoering] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "geometrie": self.geometrie,
            "maatvoeringen": [m.to_dict() for m in self.maatvoeringen],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Bouwvlak":
        return cls(
            geometrie=data.get("geometrie"),
            maatvoeringen=[Maatvoering.from_dict(m) for m in data.get("maatvoeringen", [])],
        )


@dataclass
class BestemmingsplanInfo:
    """Volledige bestemmingsplan informatie voor een locatie."""

    # Plan metadata
    plan_naam: str
    plan_id: str
    plan_type: str
    plan_status: str
    datum_vaststelling: Optional[str] = None

    # Bestemming
    bestemming: str = "Onbekend"
    bestemming_specifiek: Optional[str] = None

    # Bouwregels (uit maatvoeringen)
    max_bouwhoogte: Optional[float] = None
    max_goothoogte: Optional[float] = None
    max_bebouwingspercentage: Optional[int] = None
    max_inhoud: Optional[float] = None

    # Gedetailleerde objecten
    bouwvlak: Optional[Bouwvlak] = None
    functieaanduidingen: List[str] = field(default_factory=list)
    bouwaanduidingen: List[str] = field(default_factory=list)
    maatvoeringen: List[Maatvoering] = field(default_factory=list)

    # Regelteksten
    regels_samenvatting: Optional[str] = None
    regels_url: Optional[str] = None

    # Toekomstige ontwikkelingen
    ontwerp_plannen: List[Dict] = field(default_factory=list)

    # Link
    link_plan: str = ""

    # Meta
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = {}
        for k, v in self.__dict__.items():
            if v is None:
                continue
            if k == "bouwvlak" and v is not None:
                d[k] = v.to_dict()
            elif k == "maatvoeringen":
                d[k] = [m.to_dict() for m in v]
            else:
                d[k] = v
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BestemmingsplanInfo":
        kw = {}
        for k, v in data.items():
            if k not in cls.__dataclass_fields__:
                continue
            if k == "bouwvlak" and isinstance(v, dict):
                kw[k] = Bouwvlak.from_dict(v)
            elif k == "maatvoeringen" and isinstance(v, list):
                kw[k] = [Maatvoering.from_dict(m) for m in v]
            else:
                kw[k] = v
        return cls(**kw)


@dataclass
class OmgevingsBestemming:
    """Een bestemmingsvlak in de omgeving."""

    naam: str
    categorie: str
    geometrie: Optional[Dict] = None
    plan_naam: Optional[str] = None
    plan_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OmgevingsBestemming":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class BurenBouwinfo:
    """Bouwmogelijkheden van een naburig bestemmingsvlak."""

    bestemming: str
    max_bouwhoogte: Optional[float] = None
    max_goothoogte: Optional[float] = None
    max_bebouwingspercentage: Optional[int] = None
    geometrie: Optional[Dict] = None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BurenBouwinfo":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class OmgevingsAnalyse:
    """Analyse van bestemmingen in de omgeving van een locatie."""

    bestemmingen: List[OmgevingsBestemming] = field(default_factory=list)
    statistieken: Dict[str, int] = field(default_factory=dict)
    statistieken_pct: Dict[str, float] = field(default_factory=dict)
    ontwerp_plannen: List[Dict] = field(default_factory=list)
    buren_bouwinfo: List[BurenBouwinfo] = field(default_factory=list)
    center_lat: float = 0.0
    center_lng: float = 0.0
    radius_m: float = 500.0
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "center_lat": self.center_lat,
            "center_lng": self.center_lng,
            "radius_m": self.radius_m,
            "statistieken": self.statistieken,
            "statistieken_pct": self.statistieken_pct,
            "ontwerp_plannen": self.ontwerp_plannen,
            "bestemmingen": [b.to_dict() for b in self.bestemmingen],
            "buren_bouwinfo": [b.to_dict() for b in self.buren_bouwinfo],
        }
        if self.error:
            d["error"] = self.error
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OmgevingsAnalyse":
        return cls(
            bestemmingen=[OmgevingsBestemming.from_dict(b) for b in data.get("bestemmingen", [])],
            statistieken=data.get("statistieken", {}),
            statistieken_pct=data.get("statistieken_pct", {}),
            ontwerp_plannen=data.get("ontwerp_plannen", []),
            buren_bouwinfo=[BurenBouwinfo.from_dict(b) for b in data.get("buren_bouwinfo", [])],
            center_lat=data.get("center_lat", 0),
            center_lng=data.get("center_lng", 0),
            radius_m=data.get("radius_m", 500),
            error=data.get("error"),
        )


@dataclass
class BestemmingsplanCollector:
    """
    Collector voor bestemmingsplannen via de Ruimtelijke Plannen API v4 (DSO).

    Zoekt op basis van coordinaten (WGS84) het geldende bestemmingsplan,
    inclusief bestemming, bouwregels, maatvoeringen en regelteksten.
    """

    min_delay: float = 1.0
    max_delay: float = 2.0
    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    cache_days: int = 30
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({
                "Accept": "application/hal+json",
                "Content-Type": "application/json",
                "Content-Crs": "epsg:4326",
                "Accept-Crs": "epsg:4326",
            })
            api_key = os.environ.get("DSO_API_KEY", "")
            if api_key:
                self.session.headers["X-Api-Key"] = api_key

    def _rate_limit(self) -> None:
        import random
        elapsed = time.time() - self._last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.time()

    def _cache_key(self, lat: float, lng: float) -> str:
        raw = f"{lat:.5f},{lng:.5f}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _load_from_cache(self, key: str) -> Optional[Dict]:
        cache_file = self.cache_dir / f"{key}.json"
        if not cache_file.exists():
            return None
        try:
            with cache_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
            cached_at = data.get("_cached_at", 0)
            if time.time() - cached_at > self.cache_days * 86400:
                return None
            return data
        except (json.JSONDecodeError, OSError):
            return None

    def _save_to_cache(self, key: str, data: Dict) -> None:
        cache_file = self.cache_dir / f"{key}.json"
        data["_cached_at"] = time.time()
        try:
            with cache_file.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except OSError as e:
            logger.warning("Cache write failed: %s", e)

    def _api_call(self, method: str, path: str, **kwargs) -> Optional[Dict]:
        """Voer een API-call uit met rate limiting en error handling."""
        api_key = os.environ.get("DSO_API_KEY", "")
        if not api_key:
            logger.warning("DSO_API_KEY niet geconfigureerd")
            return None

        self._rate_limit()
        url = f"{API_BASE}{path}"
        try:
            if method == "POST":
                resp = self.session.post(url, **kwargs)
            else:
                resp = self.session.get(url, **kwargs)

            if resp.status_code == 429:
                logger.warning("Rate limit bereikt voor DSO API, wacht 10s")
                time.sleep(10)
                return self._api_call(method, path, **kwargs)
            if resp.status_code == 401:
                logger.error("DSO API key ongeldig of verlopen")
                return None
            if resp.status_code >= 400:
                logger.warning("DSO API error %d: %s", resp.status_code, resp.text[:200])
                return None

            return resp.json()
        except requests.RequestException as e:
            logger.error("DSO API request failed: %s", e)
            return None

    def _geo_body(self, lat: float, lng: float) -> Dict:
        """GeoJSON punt voor _zoek endpoints."""
        return {
            "_geo": {
                "intersects": {
                    "type": "Point",
                    "coordinates": [lng, lat],  # GeoJSON: [lng, lat]
                }
            }
        }

    def _find_plan(self, lat: float, lng: float) -> Optional[Dict]:
        """Zoek het meest relevante geldende plan voor een locatie."""
        body = self._geo_body(lat, lng)
        data = self._api_call("POST", "/plannen/_zoek", json=body, params={
            "pageSize": 20,
            "expand": "bbox",
        })
        if not data:
            return None

        plannen = data.get("_embedded", {}).get("plannen", [])
        if not plannen:
            return None

        # Filter en sorteer op relevantie
        scored: List[Tuple[int, Dict]] = []
        for plan in plannen:
            plan_type = plan.get("planType", "").lower()
            status = plan.get("planStatus", "").lower()

            # Skip ontwerp-plannen (die halen we apart op)
            if "ontwerp" in status:
                continue

            priority = PLAN_TYPE_PRIORITY.get(plan_type, 1)
            # Vastgestelde plannen krijgen bonus
            if "vastgesteld" in status or "onherroepelijk" in status:
                priority += 5
            scored.append((priority, plan))

        if not scored:
            return None

        scored.sort(key=lambda x: x[0], reverse=True)
        return scored[0][1]

    def _find_ontwerp_plannen(self, lat: float, lng: float) -> List[Dict]:
        """Zoek ontwerp-plannen voor een locatie (toekomstige ontwikkelingen)."""
        body = self._geo_body(lat, lng)
        data = self._api_call("POST", "/plannen/_zoek", json=body, params={
            "pageSize": 10,
        })
        if not data:
            return []

        plannen = data.get("_embedded", {}).get("plannen", [])
        ontwerp = []
        for plan in plannen:
            status = plan.get("planStatus", "").lower()
            if "ontwerp" in status:
                ontwerp.append({
                    "naam": plan.get("naam", ""),
                    "type": plan.get("planType", ""),
                    "status": plan.get("planStatus", ""),
                    "datum": plan.get("planstatusdatum", ""),
                    "id": plan.get("id", ""),
                })
        return ontwerp

    def _find_bestemmingsvlakken(self, lat: float, lng: float, plan_id: str) -> List[Dict]:
        """Zoek bestemmingsvlakken voor een locatie binnen een plan."""
        body = self._geo_body(lat, lng)
        data = self._api_call("POST", "/bestemmingsvlakken/_zoek", json=body, params={
            "planId": plan_id,
            "expand": "geometrie",
            "pageSize": 10,
        })
        if not data:
            return []
        return data.get("_embedded", {}).get("bestemmingsvlakken", [])

    def _find_bouwvlakken(self, lat: float, lng: float, plan_id: str) -> List[Dict]:
        """Zoek bouwvlakken voor een locatie binnen een plan."""
        body = self._geo_body(lat, lng)
        data = self._api_call("POST", "/bouwvlakken/_zoek", json=body, params={
            "planId": plan_id,
            "expand": "geometrie",
            "pageSize": 10,
        })
        if not data:
            return []
        return data.get("_embedded", {}).get("bouwvlakken", [])

    def _find_maatvoeringen(self, lat: float, lng: float, plan_id: str) -> List[Dict]:
        """Zoek maatvoeringen (bouwhoogte, bebouwingspercentage, etc.)."""
        body = self._geo_body(lat, lng)
        data = self._api_call("POST", "/maatvoeringen/_zoek", json=body, params={
            "planId": plan_id,
            "pageSize": 50,
        })
        if not data:
            return []
        return data.get("_embedded", {}).get("maatvoeringen", [])

    def _find_functieaanduidingen(self, lat: float, lng: float, plan_id: str) -> List[Dict]:
        """Zoek functieaanduidingen voor een locatie."""
        body = self._geo_body(lat, lng)
        data = self._api_call("POST", "/functieaanduidingen/_zoek", json=body, params={
            "planId": plan_id,
            "pageSize": 20,
        })
        if not data:
            return []
        return data.get("_embedded", {}).get("functieaanduidingen", [])

    def _find_bouwaanduidingen(self, lat: float, lng: float, plan_id: str) -> List[Dict]:
        """Zoek bouwaanduidingen voor een locatie."""
        body = self._geo_body(lat, lng)
        data = self._api_call("POST", "/bouwaanduidingen/_zoek", json=body, params={
            "planId": plan_id,
            "pageSize": 20,
        })
        if not data:
            return []
        return data.get("_embedded", {}).get("bouwaanduidingen", [])

    def _get_plan_teksten(self, plan_id: str) -> Optional[str]:
        """Haal regelteksten op en extraheer bouwregel-samenvatting."""
        data = self._api_call("GET", f"/plannen/{plan_id}/teksten", params={
            "pageSize": 50,
        })
        if not data:
            return None

        teksten = data.get("_embedded", {}).get("teksten", [])
        relevant_parts = []
        keywords = [
            "bouwhoogte", "bebouwingspercentage", "bijgebouw",
            "erfafscheiding", "goothoogte", "bouwvlak", "bouwregel",
            "uitbreiding", "aan- en uitbouw", "aanbouw",
        ]

        for tekst in teksten:
            titel = tekst.get("titel", "").lower()
            inhoud = tekst.get("inhoud", "")
            if not inhoud:
                continue

            plain = _xhtml_to_text(inhoud)

            # Check of titel of inhoud relevant is
            is_relevant = any(kw in titel for kw in keywords)
            if not is_relevant:
                is_relevant = any(kw in plain.lower() for kw in keywords)

            if is_relevant and plain:
                header = tekst.get("titel", "")
                snippet = plain[:300]
                if len(plain) > 300:
                    snippet += "..."
                relevant_parts.append(f"{header}: {snippet}" if header else snippet)

        if not relevant_parts:
            return None

        samenvatting = " | ".join(relevant_parts)
        if len(samenvatting) > 500:
            samenvatting = samenvatting[:497] + "..."
        return samenvatting

    def _parse_maatvoeringen(self, raw_maatvoeringen: List[Dict]) -> Tuple[List[Maatvoering], Dict[str, float]]:
        """Parse ruwe maatvoeringen naar gestructureerde objecten."""
        parsed: List[Maatvoering] = []
        extracted: Dict[str, float] = {}

        for mv in raw_maatvoeringen:
            naam = mv.get("naam", "")
            # Maatvoeringen bevatten vaak een waardeType en waarden array
            waarden = mv.get("maatvoeringInfo", [])
            if not waarden:
                # Probeer alternatieve structuur
                waarde = mv.get("waarde", "")
                if waarde:
                    waarden = [{"waarde": waarde}]

            for w in waarden:
                waarde_str = str(w.get("waarde", ""))
                eenheid = w.get("eenheid")
                waarde_type = w.get("waardeType")

                m = Maatvoering(
                    naam=naam,
                    waarde=waarde_str,
                    eenheid=eenheid,
                    waarde_type=waarde_type,
                )
                parsed.append(m)

                # Probeer bekende waarden te extraheren
                try:
                    val = float(waarde_str.replace(",", "."))
                except (ValueError, TypeError):
                    continue

                for key, pattern in MAATVOERING_PATTERNS.items():
                    if pattern.search(naam):
                        # Bewaar de hoogste/meest specifieke waarde
                        if key not in extracted or val > extracted[key]:
                            extracted[key] = val
                        break

        return parsed, extracted

    def _geo_body_bbox(self, lat: float, lng: float, radius_m: float = 500) -> Dict:
        """GeoJSON polygon (bbox) voor _zoek endpoints."""
        lat_rad = math.radians(lat)
        delta_lat = radius_m / 111_000
        delta_lng = radius_m / (111_000 * math.cos(lat_rad))

        min_lat = lat - delta_lat
        max_lat = lat + delta_lat
        min_lng = lng - delta_lng
        max_lng = lng + delta_lng

        return {
            "_geo": {
                "intersects": {
                    "type": "Polygon",
                    "coordinates": [[
                        [min_lng, min_lat],
                        [max_lng, min_lat],
                        [max_lng, max_lat],
                        [min_lng, max_lat],
                        [min_lng, min_lat],  # sluit ring
                    ]]
                }
            }
        }

    def _paginated_api_call(
        self, method: str, path: str, resource_key: str, max_pages: int = 3, **kwargs
    ) -> List[Dict]:
        """Voer een gepagineerde API-call uit en verzamel alle resultaten."""
        all_results: List[Dict] = []
        for page in range(1, max_pages + 1):
            params = kwargs.pop("params", {})
            params["page"] = page
            kwargs["params"] = params

            data = self._api_call(method, path, **kwargs)
            if not data:
                break

            items = data.get("_embedded", {}).get(resource_key, [])
            if not items:
                break
            all_results.extend(items)

            # Stop als er geen volgende pagina is
            links = data.get("_links", {})
            if "next" not in links:
                break

        return all_results

    def find_bestemmingsvlakken_area(
        self, lat: float, lng: float, radius_m: float = 500
    ) -> List[Dict]:
        """Zoek alle bestemmingsvlakken in een gebied (bbox)."""
        body = self._geo_body_bbox(lat, lng, radius_m)
        return self._paginated_api_call(
            "POST", "/bestemmingsvlakken/_zoek", "bestemmingsvlakken",
            max_pages=3,
            json=body,
            params={"expand": "geometrie", "pageSize": 100},
        )

    def _find_ontwerp_plannen_area(
        self, lat: float, lng: float, radius_m: float = 500
    ) -> List[Dict]:
        """Zoek ontwerp-plannen in een gebied."""
        body = self._geo_body_bbox(lat, lng, radius_m)
        data = self._api_call("POST", "/plannen/_zoek", json=body, params={
            "pageSize": 20,
        })
        if not data:
            return []

        plannen = data.get("_embedded", {}).get("plannen", [])
        ontwerp = []
        for plan in plannen:
            status = plan.get("planStatus", "").lower()
            if "ontwerp" in status:
                ontwerp.append({
                    "naam": plan.get("naam", ""),
                    "type": plan.get("planType", ""),
                    "status": plan.get("planStatus", ""),
                    "datum": plan.get("planstatusdatum", ""),
                    "id": plan.get("id", ""),
                })
        return ontwerp

    def find_buren_bouwmogelijkheden(
        self, lat: float, lng: float, eigen_bestemming: str
    ) -> List[BurenBouwinfo]:
        """Zoek bouwmogelijkheden van naburige bestemmingsvlakken (50m radius)."""
        # Zoek vlakken in directe omgeving
        body = self._geo_body_bbox(lat, lng, radius_m=50)
        vlakken = self._paginated_api_call(
            "POST", "/bestemmingsvlakken/_zoek", "bestemmingsvlakken",
            max_pages=1,
            json=body,
            params={"expand": "geometrie", "pageSize": 20},
        )

        # Filter op andere bestemmingen dan de eigen
        eigen_lower = eigen_bestemming.lower()
        buren_vlakken = [
            v for v in vlakken
            if v.get("naam", "").lower() != eigen_lower
        ]

        # Beperk tot max 3 unieke bestemmingen
        seen: set = set()
        buren: List[BurenBouwinfo] = []
        for vlak in buren_vlakken:
            naam = vlak.get("naam", "Onbekend")
            if naam in seen:
                continue
            seen.add(naam)

            # Probeer maatvoeringen op te halen voor dit vlak
            plan_href = vlak.get("_links", {}).get("plan", {}).get("href", "")
            plan_id = plan_href.rstrip("/").split("/")[-1] if plan_href else ""

            extracted: Dict[str, float] = {}
            if plan_id:
                # Zoek maatvoeringen in de buurt binnen hetzelfde plan
                mv_body = self._geo_body_bbox(lat, lng, radius_m=50)
                mv_raw = self._paginated_api_call(
                    "POST", "/maatvoeringen/_zoek", "maatvoeringen",
                    max_pages=1,
                    json=mv_body,
                    params={"planId": plan_id, "pageSize": 20},
                )
                _, extracted = self._parse_maatvoeringen(mv_raw)

            buren.append(BurenBouwinfo(
                bestemming=naam,
                max_bouwhoogte=extracted.get("max_bouwhoogte"),
                max_goothoogte=extracted.get("max_goothoogte"),
                max_bebouwingspercentage=(
                    int(extracted["max_bebouwingspercentage"])
                    if "max_bebouwingspercentage" in extracted
                    else None
                ),
                geometrie=vlak.get("geometrie"),
            ))

            if len(buren) >= 3:
                break

        return buren

    def get_omgevingsanalyse(
        self, lat: float, lng: float, radius_m: float = 500
    ) -> OmgevingsAnalyse:
        """
        Analyseer bestemmingen in de omgeving van een locatie.

        Returns:
            OmgevingsAnalyse met bestemmingsvlakken, statistieken, ontwerp-plannen en buren-info
        """
        # Cache check (afgerond op 4 decimalen)
        cache_dir = self.cache_dir / "omgeving"
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_key = f"omgeving_{lat:.4f}_{lng:.4f}_{int(radius_m)}"
        cache_file = cache_dir / f"{cache_key}.json"

        if cache_file.exists():
            try:
                with cache_file.open("r", encoding="utf-8") as f:
                    cached = json.load(f)
                if time.time() - cached.get("_cached_at", 0) < self.cache_days * 86400:
                    logger.info("Omgevingsanalyse uit cache voor %.4f,%.4f", lat, lng)
                    return OmgevingsAnalyse.from_dict(cached)
            except (json.JSONDecodeError, OSError):
                pass

        # Check API key
        if not os.environ.get("DSO_API_KEY"):
            return OmgevingsAnalyse(
                center_lat=lat, center_lng=lng, radius_m=radius_m,
                error="DSO_API_KEY niet geconfigureerd",
            )

        logger.info("Omgevingsanalyse ophalen voor %.4f,%.4f (radius %dm)", lat, lng, radius_m)

        # 1. Alle bestemmingsvlakken in het gebied
        raw_vlakken = self.find_bestemmingsvlakken_area(lat, lng, radius_m)

        bestemmingen: List[OmgevingsBestemming] = []
        for vlak in raw_vlakken:
            naam = vlak.get("naam", vlak.get("type", "Onbekend"))
            plan_ref = vlak.get("_links", {}).get("plan", {})
            plan_naam = plan_ref.get("title", "")
            plan_id = plan_ref.get("href", "").rstrip("/").split("/")[-1] if plan_ref.get("href") else ""

            bestemmingen.append(OmgevingsBestemming(
                naam=naam,
                categorie=_categorize_bestemming(naam),
                geometrie=vlak.get("geometrie"),
                plan_naam=plan_naam,
                plan_id=plan_id,
            ))

        # 2. Statistieken
        cat_counts: Dict[str, int] = {}
        for b in bestemmingen:
            cat_counts[b.categorie] = cat_counts.get(b.categorie, 0) + 1
        total = max(sum(cat_counts.values()), 1)
        cat_pct = {k: round(v / total * 100, 1) for k, v in cat_counts.items()}

        # Sorteer op percentage (dalend)
        cat_counts = dict(sorted(cat_counts.items(), key=lambda x: x[1], reverse=True))
        cat_pct = dict(sorted(cat_pct.items(), key=lambda x: x[1], reverse=True))

        # 3. Ontwerp-plannen in de omgeving
        ontwerp = self._find_ontwerp_plannen_area(lat, lng, radius_m)

        # 4. Buren bouwmogelijkheden
        # Eerst eigen bestemming bepalen
        eigen_bestemming = ""
        eigen_plan = self._find_plan(lat, lng)
        if eigen_plan:
            eigen_bv = self._find_bestemmingsvlakken(lat, lng, eigen_plan.get("id", ""))
            if eigen_bv:
                eigen_bestemming = eigen_bv[0].get("naam", "")

        buren = []
        if eigen_bestemming:
            buren = self.find_buren_bouwmogelijkheden(lat, lng, eigen_bestemming)

        result = OmgevingsAnalyse(
            bestemmingen=bestemmingen,
            statistieken=cat_counts,
            statistieken_pct=cat_pct,
            ontwerp_plannen=ontwerp,
            buren_bouwinfo=buren,
            center_lat=lat,
            center_lng=lng,
            radius_m=radius_m,
        )

        # Cache opslaan
        try:
            cache_data = result.to_dict()
            cache_data["_cached_at"] = time.time()
            with cache_file.open("w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
        except OSError as e:
            logger.warning("Omgevingsanalyse cache write failed: %s", e)

        return result

    def _build_plan_link(self, lat: float, lng: float) -> str:
        """Genereer link naar Regels op de Kaart viewer."""
        return (
            f"https://omgevingswet.overheid.nl/regels-op-de-kaart"
            f"?locatie-coordinaat={lat},{lng}"
        )

    def get_bestemmingsplan(self, lat: float, lng: float) -> BestemmingsplanInfo:
        """
        Haal bestemmingsplan informatie op voor een locatie.

        Args:
            lat: Breedtegraad (WGS84)
            lng: Lengtegraad (WGS84)

        Returns:
            BestemmingsplanInfo met alle beschikbare data
        """
        # Check cache
        cache_key = self._cache_key(lat, lng)
        cached = self._load_from_cache(cache_key)
        if cached:
            logger.info("Bestemmingsplan uit cache voor %.5f,%.5f", lat, lng)
            return BestemmingsplanInfo.from_dict(cached)

        # Check API key
        if not os.environ.get("DSO_API_KEY"):
            return BestemmingsplanInfo(
                plan_naam="",
                plan_id="",
                plan_type="",
                plan_status="",
                link_plan=self._build_plan_link(lat, lng),
                error="DSO_API_KEY niet geconfigureerd",
            )

        logger.info("Bestemmingsplan ophalen voor %.5f,%.5f", lat, lng)

        # 1. Zoek het geldende plan
        plan = self._find_plan(lat, lng)
        if not plan:
            return BestemmingsplanInfo(
                plan_naam="",
                plan_id="",
                plan_type="",
                plan_status="",
                link_plan=self._build_plan_link(lat, lng),
                error="Geen bestemmingsplan gevonden voor deze locatie",
            )

        plan_id = plan.get("id", "")
        plan_naam = plan.get("naam", "Onbekend plan")
        plan_type = plan.get("planType", "")
        plan_status = plan.get("planStatus", "")
        datum = plan.get("planstatusdatum", "")

        # 2. Bestemming ophalen
        bestemming = "Onbekend"
        bestemming_specifiek = None
        bv_list = self._find_bestemmingsvlakken(lat, lng, plan_id)
        if bv_list:
            bv = bv_list[0]  # Meest specifieke
            bestemming = bv.get("naam", bv.get("type", "Onbekend"))
            bestemming_specifiek = bv.get("specificatie")

        # 3. Bouwvlak
        bouwvlak = None
        bv_raw = self._find_bouwvlakken(lat, lng, plan_id)
        if bv_raw:
            bv_first = bv_raw[0]
            bouwvlak = Bouwvlak(
                geometrie=bv_first.get("geometrie"),
            )

        # 4. Maatvoeringen
        mv_raw = self._find_maatvoeringen(lat, lng, plan_id)
        maatvoeringen, extracted = self._parse_maatvoeringen(mv_raw)

        # Koppel maatvoeringen aan bouwvlak indien aanwezig
        if bouwvlak:
            bouwvlak.maatvoeringen = maatvoeringen

        # 5. Functieaanduidingen
        fa_raw = self._find_functieaanduidingen(lat, lng, plan_id)
        functieaanduidingen = [
            fa.get("naam", fa.get("label", ""))
            for fa in fa_raw
            if fa.get("naam") or fa.get("label")
        ]

        # 6. Bouwaanduidingen
        ba_raw = self._find_bouwaanduidingen(lat, lng, plan_id)
        bouwaanduidingen = [
            ba.get("naam", ba.get("label", ""))
            for ba in ba_raw
            if ba.get("naam") or ba.get("label")
        ]

        # 7. Regelteksten
        regels_samenvatting = self._get_plan_teksten(plan_id)

        # 8. Ontwerp-plannen (toekomstige ontwikkelingen)
        ontwerp_plannen = self._find_ontwerp_plannen(lat, lng)

        # 9. Regels URL
        regels_url = None
        links = plan.get("_links", {})
        if "self" in links:
            regels_url = links["self"].get("href")

        # Bouw resultaat
        info = BestemmingsplanInfo(
            plan_naam=plan_naam,
            plan_id=plan_id,
            plan_type=plan_type,
            plan_status=plan_status,
            datum_vaststelling=datum,
            bestemming=bestemming,
            bestemming_specifiek=bestemming_specifiek,
            max_bouwhoogte=extracted.get("max_bouwhoogte"),
            max_goothoogte=extracted.get("max_goothoogte"),
            max_bebouwingspercentage=(
                int(extracted["max_bebouwingspercentage"])
                if "max_bebouwingspercentage" in extracted
                else None
            ),
            max_inhoud=extracted.get("max_inhoud"),
            bouwvlak=bouwvlak,
            functieaanduidingen=functieaanduidingen,
            bouwaanduidingen=bouwaanduidingen,
            maatvoeringen=maatvoeringen,
            regels_samenvatting=regels_samenvatting,
            regels_url=regels_url,
            ontwerp_plannen=ontwerp_plannen,
            link_plan=self._build_plan_link(lat, lng),
        )

        # Sla op in cache
        self._save_to_cache(cache_key, info.to_dict())
        return info


def create_bestemmingsplan_collector(
    cache_dir: Optional[Path] = None,
) -> BestemmingsplanCollector:
    """Factory function met default cache directory."""
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "bestemmingsplan"
    return BestemmingsplanCollector(cache_dir=cache_dir)
