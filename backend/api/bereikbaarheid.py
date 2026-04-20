"""API routes for OV bereikbaarheid (public transport accessibility)."""

import asyncio
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from collectors import create_ov_collector, create_cycling_collector, geocode_address_pdok
from collectors import create_ors_matrix_collector

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["bereikbaarheid"])

# Singleton collectors (lazy init)
_ov_collector = None
_cycling_collector = None
_ors_matrix_collector = None
_werklocaties = None


def _get_ov_collector():
    global _ov_collector
    if _ov_collector is None:
        _ov_collector = create_ov_collector()
    return _ov_collector


def _get_cycling_collector():
    global _cycling_collector
    if _cycling_collector is None:
        _cycling_collector = create_cycling_collector()
    return _cycling_collector


def _get_ors_matrix_collector():
    global _ors_matrix_collector
    if _ors_matrix_collector is None:
        _ors_matrix_collector = create_ors_matrix_collector()
    return _ors_matrix_collector


def _get_werklocaties() -> List[Dict]:
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

class OVHalteItem(BaseModel):
    naam: str
    type: str
    lat: float
    lng: float
    afstand_m: int
    lijnen: List[str]
    frequentie_spits: Optional[int] = None


class OVReistijdItem(BaseModel):
    dest_naam: str
    reistijd_min: int
    overstappen: int
    route_beschrijving: str
    halte_naam: str = ""
    error: Optional[str] = None


class FietsItem(BaseModel):
    dest_naam: str
    afstand_km: float
    reistijd_min: int
    error: Optional[str] = None


class BereikbaarheidResponse(BaseModel):
    lat: float
    lng: float
    ov_score: float
    score_breakdown: Dict[str, float] = {}
    dichtstbijzijnde_halte: Optional[OVHalteItem] = None
    haltes_nabij: List[OVHalteItem] = []
    ov_reistijden: List[OVReistijdItem] = []
    fiets_reistijden: List[FietsItem] = []


class ReistijdResponse(BaseModel):
    van_lat: float
    van_lng: float
    naar_lat: float
    naar_lng: float
    modus: str
    reistijd_min: int
    overstappen: int = 0
    route_beschrijving: str = ""
    halte_naam: str = ""
    error: Optional[str] = None


# --- Endpoints ---

@router.get("/woningen/{woning_id}/bereikbaarheid", response_model=BereikbaarheidResponse)
async def woning_bereikbaarheid(woning_id: int):
    """Get OV + cycling accessibility for a specific property."""
    from models.database import SessionLocal
    from models import Woning

    session = SessionLocal()
    try:
        woning = session.query(Woning).filter(Woning.id == woning_id).first()
        if not woning:
            raise HTTPException(status_code=404, detail="Woning niet gevonden")

        lat = woning.latitude
        lng = woning.longitude

        # If no coordinates, try geocoding
        if not lat or not lng:
            if woning.postcode and woning.huisnummer:
                geo = geocode_address_pdok(woning.postcode, woning.huisnummer)
                if geo:
                    lat = geo["lat"]
                    lng = geo["lng"]

        if not lat or not lng:
            raise HTTPException(
                status_code=422,
                detail="Geen coordinaten beschikbaar voor deze woning",
            )
    finally:
        session.close()

    return await _build_bereikbaarheid(lat, lng)


@router.get("/locatie/reistijd", response_model=ReistijdResponse)
async def locatie_reistijd(
    van: str = Query(..., description="Startcoordinaten (lat,lng)"),
    naar: str = Query(..., description="Bestemmingscoordinaten (lat,lng)"),
    modus: str = Query("ov", description="Reismodus: ov of fiets"),
):
    """Calculate travel time between two coordinates."""
    van_match = re.match(r"^(-?\d+\.?\d*),(-?\d+\.?\d*)$", van.strip())
    naar_match = re.match(r"^(-?\d+\.?\d*),(-?\d+\.?\d*)$", naar.strip())

    if not van_match or not naar_match:
        raise HTTPException(status_code=400, detail="Ongeldige coordinaten, verwacht: lat,lng")

    van_lat = float(van_match.group(1))
    van_lng = float(van_match.group(2))
    naar_lat = float(naar_match.group(1))
    naar_lng = float(naar_match.group(2))

    if modus == "ov":
        ov = _get_ov_collector()
        reistijd = ov.estimate_travel_time(van_lat, van_lng, naar_lat, naar_lng, "Bestemming")
        return ReistijdResponse(
            van_lat=van_lat,
            van_lng=van_lng,
            naar_lat=naar_lat,
            naar_lng=naar_lng,
            modus="ov",
            reistijd_min=reistijd.reistijd_min,
            overstappen=reistijd.overstappen,
            route_beschrijving=reistijd.route_beschrijving,
            halte_naam=reistijd.halte_naam,
            error=reistijd.error,
        )
    elif modus == "fiets":
        cycling = _get_cycling_collector()
        route = await asyncio.to_thread(cycling.get_route, van_lat, van_lng, naar_lat, naar_lng, "Bestemming")
        return ReistijdResponse(
            van_lat=van_lat,
            van_lng=van_lng,
            naar_lat=naar_lat,
            naar_lng=naar_lng,
            modus="fiets",
            reistijd_min=route.reistijd_min,
            route_beschrijving=f"Fietsroute {route.afstand_km} km",
            error=route.error,
        )
    else:
        raise HTTPException(status_code=400, detail="Ongeldige modus, gebruik 'ov' of 'fiets'")


async def _build_bereikbaarheid(lat: float, lng: float) -> BereikbaarheidResponse:
    """Build bereikbaarheid response for given coordinates."""
    werklocaties = _get_werklocaties()
    ov = _get_ov_collector()
    ors = _get_ors_matrix_collector()

    dest_coords = [(wl["lat"], wl["lng"]) for wl in werklocaties]

    if dest_coords:
        bereikbaarheid, ors_resultaten = await asyncio.gather(
            asyncio.to_thread(ov.get_bereikbaarheid, lat, lng, werklocaties),
            asyncio.to_thread(ors.get_afstanden, lat, lng, dest_coords),
        )
    else:
        bereikbaarheid = await asyncio.to_thread(ov.get_bereikbaarheid, lat, lng, werklocaties)
        ors_resultaten = []

    haltes = [
        OVHalteItem(
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
        OVReistijdItem(
            dest_naam=r.dest_naam,
            reistijd_min=r.reistijd_min,
            overstappen=r.overstappen,
            route_beschrijving=r.route_beschrijving,
            halte_naam=r.halte_naam,
            error=r.error,
        )
        for r in bereikbaarheid.reistijden
    ]

    fiets_reistijden = []
    for r in ors_resultaten:
        naam = werklocaties[r.dest_index]["naam"] if r.dest_index < len(werklocaties) else "Bestemming"
        fiets_reistijden.append(FietsItem(
            dest_naam=naam,
            afstand_km=round(r.afstand_m / 1000, 1),
            reistijd_min=r.reistijd_sec // 60,
        ))

    return BereikbaarheidResponse(
        lat=lat,
        lng=lng,
        ov_score=bereikbaarheid.ov_score,
        score_breakdown=bereikbaarheid.score_breakdown,
        dichtstbijzijnde_halte=haltes[0] if haltes else None,
        haltes_nabij=haltes,
        ov_reistijden=ov_reistijden,
        fiets_reistijden=fiets_reistijden,
    )
