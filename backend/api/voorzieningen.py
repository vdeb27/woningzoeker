"""API routes for voorzieningen (nearby facilities) and fietsafstand."""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

import requests
import yaml
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from collectors import (
    create_cbs_nabijheid_collector,
    create_osm_overpass_collector,
    create_cycling_collector,
    create_ov_collector,
    geocode_address_pdok,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/voorzieningen", tags=["voorzieningen"])

# Singleton collectors (lazy init)
_nabijheid_collector = None
_osm_collector = None
_cycling_collector = None
_ov_collector = None
_werklocaties = None


def _get_nabijheid_collector():
    global _nabijheid_collector
    if _nabijheid_collector is None:
        _nabijheid_collector = create_cbs_nabijheid_collector()
    return _nabijheid_collector


def _get_osm_collector():
    global _osm_collector
    if _osm_collector is None:
        _osm_collector = create_osm_overpass_collector()
    return _osm_collector


def _get_cycling_collector():
    global _cycling_collector
    if _cycling_collector is None:
        _cycling_collector = create_cycling_collector()
    return _cycling_collector


def _get_ov_collector():
    global _ov_collector
    if _ov_collector is None:
        _ov_collector = create_ov_collector()
    return _ov_collector


def _get_werklocaties() -> List[Dict]:
    """Load werklocaties from config/werklocaties.yaml."""
    global _werklocaties
    if _werklocaties is None:
        config_path = Path(__file__).parent.parent.parent / "config" / "werklocaties.yaml"
        try:
            with open(config_path) as f:
                data = yaml.safe_load(f)
            _werklocaties = data.get("werklocaties", [])
        except Exception as exc:
            logger.warning("Could not load werklocaties.yaml: %s", exc)
            _werklocaties = []
    return _werklocaties


# --- Response models ---

class VoorzieningItem(BaseModel):
    naam: str
    type: str
    categorie: str
    afstand_m: int
    looptijd_min: int
    lat: float
    lng: float


class CBSAfstand(BaseModel):
    indicator: str
    label: str
    afstand_km: float
    looptijd_min: int


class FietsafstandItem(BaseModel):
    dest_naam: str
    afstand_km: float
    reistijd_min: int
    geometry: Optional[List] = None
    error: Optional[str] = None


class OVHalteResponse(BaseModel):
    naam: str
    type: str  # "trein", "tram", "bus", "metro"
    lat: float
    lng: float
    afstand_m: int
    lijnen: List[str]
    frequentie_spits: Optional[int] = None


class OVReistijdResponse(BaseModel):
    dest_naam: str
    reistijd_min: int
    overstappen: int
    route_beschrijving: str
    halte_naam: str = ""
    error: Optional[str] = None


class OVDataResponse(BaseModel):
    ov_score: float
    dichtstbijzijnde_halte: Optional[OVHalteResponse] = None
    haltes_nabij: List[OVHalteResponse] = []
    reistijden_werklocaties: List[OVReistijdResponse] = []
    score_breakdown: Dict[str, float] = {}


class VoorzieningenResponse(BaseModel):
    cbs_afstanden: Dict[str, List[CBSAfstand]]
    voorzieningen: List[VoorzieningItem]
    fietsafstanden: List[FietsafstandItem] = []
    ov_data: Optional[OVDataResponse] = None
    score_voorzieningen: Optional[float] = None
    buurt_code: Optional[str] = None
    buurt_naam: Optional[str] = None
    lat: float
    lng: float


# --- CBS indicator grouping ---

CBS_CATEGORIE_MAP: Dict[str, Dict[str, str]] = {
    "dagelijks": {
        "afstand_supermarkt": "Supermarkt",
        "afstand_dagelijkse_levensmiddelen": "Dagelijkse levensmiddelen",
    },
    "zorg": {
        "afstand_huisarts": "Huisarts",
        "afstand_huisartsenpost": "Huisartsenpost",
        "afstand_apotheek": "Apotheek",
        "afstand_ziekenhuis": "Ziekenhuis",
        "afstand_consultatiebureau": "Consultatiebureau",
        "afstand_fysiotherapeut": "Fysiotherapeut",
    },
    "onderwijs": {
        "afstand_kinderdagverblijf": "Kinderdagverblijf",
        "afstand_buitenschoolse_opvang": "Buitenschoolse opvang",
        "afstand_basisonderwijs": "Basisonderwijs",
        "afstand_voortgezet_onderwijs": "Voortgezet onderwijs",
        "afstand_vmbo": "VMBO",
        "afstand_havo_vwo": "HAVO/VWO",
    },
    "winkels_horeca": {
        "afstand_warenhuis": "Warenhuis",
        "afstand_cafe": "Cafe",
        "afstand_cafetaria": "Cafetaria",
        "afstand_restaurant": "Restaurant",
        "afstand_hotel": "Hotel",
    },
    "sport": {
        "afstand_zwembad": "Zwembad",
        "afstand_sportterrein": "Sportterrein",
        "afstand_kunstijsbaan": "Kunstijsbaan",
    },
    "cultuur": {
        "afstand_bibliotheek": "Bibliotheek",
        "afstand_bioscoop": "Bioscoop",
        "afstand_museum": "Museum",
        "afstand_podiumkunsten": "Podiumkunsten",
        "afstand_poppodium": "Poppodium",
        "afstand_attractie": "Attractie",
        "afstand_sauna": "Sauna",
    },
    "vervoer": {
        "afstand_oprit_hoofdverkeersweg": "Oprit hoofdweg",
        "afstand_treinstation": "Treinstation",
        "afstand_overstapstation": "Overstapstation",
    },
    "natuur": {
        "afstand_openbaar_groen": "Openbaar groen",
        "afstand_park": "Park/plantsoen",
        "afstand_bos": "Bos",
        "afstand_open_natuur": "Open natuur",
        "afstand_dagrecreatie": "Dagrecreatie",
        "afstand_recreatief_water": "Recreatief water",
        "afstand_volkstuin": "Volkstuin",
    },
}


def _build_cbs_afstanden(afstanden: Dict[str, float]) -> Dict[str, List[CBSAfstand]]:
    """Group CBS distance indicators by category."""
    result: Dict[str, List[CBSAfstand]] = {}
    for categorie, indicators in CBS_CATEGORIE_MAP.items():
        items = []
        for key, label in indicators.items():
            if key in afstanden:
                km = afstanden[key]
                items.append(CBSAfstand(
                    indicator=key,
                    label=label,
                    afstand_km=round(km, 1),
                    looptijd_min=round(km * 12),
                ))
        if items:
            result[categorie] = items
    return result


def _get_score_voorzieningen(buurt_code: str) -> Optional[float]:
    """Get the voorzieningen score from the Buurt model if available."""
    try:
        from models.database import SessionLocal
        from models import Buurt
        session = SessionLocal()
        try:
            buurt = session.query(Buurt).filter(Buurt.code == buurt_code).first()
            if buurt:
                return buurt.score_voorzieningen
        finally:
            session.close()
    except Exception:
        pass
    return None


def _reverse_geocode_pdok(lat: float, lng: float) -> Optional[Dict[str, str]]:
    """Reverse geocode coordinates to buurt_code and buurt_naam via PDOK."""
    url = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/reverse"
    params = {
        "lat": lat,
        "lon": lng,
        "rows": 1,
        "type": "adres",
        "fl": "buurtcode,buurtnaam",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        docs = resp.json().get("response", {}).get("docs", [])
        if docs:
            buurt_code = docs[0].get("buurtcode")
            if buurt_code and not buurt_code.startswith("BU"):
                buurt_code = f"BU{buurt_code}"
            return {
                "buurt_code": buurt_code,
                "buurt_naam": docs[0].get("buurtnaam"),
            }
    except requests.RequestException:
        pass
    return None


def _build_response(
    lat: float,
    lng: float,
    buurt_code: Optional[str],
    buurt_naam: Optional[str],
    radius_m: int,
) -> VoorzieningenResponse:
    """Build the combined response from OSM + CBS data."""
    # OSM Overpass data
    osm_items: List[VoorzieningItem] = []
    try:
        osm_collector = _get_osm_collector()
        osm_result = osm_collector.get_voorzieningen(lat, lng, radius_m)
        for v in osm_result.voorzieningen:
            osm_items.append(VoorzieningItem(
                naam=v.naam,
                type=v.type,
                categorie=v.categorie,
                afstand_m=v.afstand_m,
                looptijd_min=max(1, round(v.afstand_m / 83.3)),  # 5 km/h
                lat=v.lat,
                lng=v.lng,
            ))
    except Exception as exc:
        logger.warning("Overpass query failed, returning CBS-only: %s", exc)

    # CBS Nabijheid data
    cbs_afstanden: Dict[str, List[CBSAfstand]] = {}
    score_voorzieningen: Optional[float] = None
    if buurt_code:
        try:
            nabijheid = _get_nabijheid_collector()
            nab_result = nabijheid.get_buurt(buurt_code)
            if nab_result:
                cbs_afstanden = _build_cbs_afstanden(nab_result.afstanden)
        except Exception as exc:
            logger.warning("CBS nabijheid lookup failed: %s", exc)

        score_voorzieningen = _get_score_voorzieningen(buurt_code)

    # Fietsafstanden naar werklocaties
    fietsafstanden: List[FietsafstandItem] = []
    werklocaties = _get_werklocaties()
    if werklocaties:
        try:
            cycling = _get_cycling_collector()
            routes = cycling.get_routes_to_werklocaties(lat, lng, werklocaties)
            for route in routes:
                fietsafstanden.append(FietsafstandItem(
                    dest_naam=route.dest_naam,
                    afstand_km=route.afstand_km,
                    reistijd_min=route.reistijd_min,
                    geometry=route.geometry,
                    error=route.error,
                ))
        except Exception as exc:
            logger.warning("Cycling route calculation failed: %s", exc)

    # OV bereikbaarheid data
    ov_data: Optional[OVDataResponse] = None
    try:
        ov = _get_ov_collector()
        bereikbaarheid = ov.get_bereikbaarheid(lat, lng, werklocaties)

        ov_haltes = [
            OVHalteResponse(
                naam=h.naam,
                type=h.type,
                lat=h.lat,
                lng=h.lng,
                afstand_m=h.afstand_m,
                lijnen=h.lijnen,
                frequentie_spits=h.frequentie_spits,
            )
            for h in bereikbaarheid.haltes_nabij
        ]

        ov_reistijden = [
            OVReistijdResponse(
                dest_naam=r.dest_naam,
                reistijd_min=r.reistijd_min,
                overstappen=r.overstappen,
                route_beschrijving=r.route_beschrijving,
                halte_naam=r.halte_naam,
                error=r.error,
            )
            for r in bereikbaarheid.reistijden
        ]

        ov_data = OVDataResponse(
            ov_score=bereikbaarheid.ov_score,
            dichtstbijzijnde_halte=ov_haltes[0] if ov_haltes else None,
            haltes_nabij=ov_haltes,
            reistijden_werklocaties=ov_reistijden,
            score_breakdown=bereikbaarheid.score_breakdown,
        )
    except Exception as exc:
        logger.warning("OV bereikbaarheid lookup failed: %s", exc)

    return VoorzieningenResponse(
        cbs_afstanden=cbs_afstanden,
        voorzieningen=osm_items,
        fietsafstanden=fietsafstanden,
        ov_data=ov_data,
        score_voorzieningen=score_voorzieningen,
        buurt_code=buurt_code,
        buurt_naam=buurt_naam,
        lat=lat,
        lng=lng,
    )


@router.get("/adres", response_model=VoorzieningenResponse)
def voorzieningen_adres(
    postcode: str = Query(..., description="Postcode (bijv. 2511AB)"),
    huisnummer: int = Query(..., description="Huisnummer"),
    radius_m: int = Query(1500, ge=100, le=5000, description="Zoekradius in meters"),
):
    """Get nearby facilities for a given address."""
    geo = geocode_address_pdok(postcode, huisnummer)
    if geo is None:
        raise HTTPException(status_code=404, detail="Adres niet gevonden via PDOK")

    return _build_response(
        lat=geo["lat"],
        lng=geo["lng"],
        buurt_code=geo.get("buurt_code"),
        buurt_naam=geo.get("buurt_naam"),
        radius_m=radius_m,
    )


@router.get("/locatie/{coords}", response_model=VoorzieningenResponse)
def voorzieningen_locatie(
    coords: str,
    radius_m: int = Query(1500, ge=100, le=5000, description="Zoekradius in meters"),
):
    """Get nearby facilities for given coordinates (lat,lng)."""
    match = re.match(r"^(-?\d+\.?\d*),(-?\d+\.?\d*)$", coords.strip())
    if not match:
        raise HTTPException(status_code=400, detail="Ongeldige coordinaten, verwacht: lat,lng")

    lat = float(match.group(1))
    lng = float(match.group(2))

    # Reverse geocode for buurt info
    buurt_code = None
    buurt_naam = None
    geo = _reverse_geocode_pdok(lat, lng)
    if geo:
        buurt_code = geo.get("buurt_code")
        buurt_naam = geo.get("buurt_naam")

    return _build_response(
        lat=lat,
        lng=lng,
        buurt_code=buurt_code,
        buurt_naam=buurt_naam,
        radius_m=radius_m,
    )
