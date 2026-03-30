"""Bestemmingsplan API routes."""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from collectors.bestemmingsplan_collector import create_bestemmingsplan_collector
from models.database import get_db
from models.woning import Woning

router = APIRouter(prefix="/api/bestemmingsplan", tags=["bestemmingsplan"])


# --- Response Models ---


class MaatvoeringResponse(BaseModel):
    naam: str
    waarde: str
    eenheid: Optional[str] = None
    waarde_type: Optional[str] = None


class BouwvlakResponse(BaseModel):
    geometrie: Optional[Dict[str, Any]] = None
    maatvoeringen: List[MaatvoeringResponse] = []


class OntwerpPlanResponse(BaseModel):
    naam: str = ""
    type: str = ""
    status: str = ""
    datum: str = ""
    id: str = ""


class BestemmingsplanResponse(BaseModel):
    # Plan metadata
    plan_naam: str
    plan_type: str
    plan_status: str
    datum_vaststelling: Optional[str] = None

    # Bestemming
    bestemming: str
    bestemming_specifiek: Optional[str] = None

    # Bouwregels
    max_bouwhoogte: Optional[float] = None
    max_goothoogte: Optional[float] = None
    max_bebouwingspercentage: Optional[int] = None
    max_inhoud: Optional[float] = None

    # Gedetailleerde objecten
    bouwvlak: Optional[BouwvlakResponse] = None
    functieaanduidingen: List[str] = []
    bouwaanduidingen: List[str] = []
    maatvoeringen: List[MaatvoeringResponse] = []

    # Regelteksten
    regels_samenvatting: Optional[str] = None
    regels_url: Optional[str] = None

    # Toekomstige ontwikkelingen
    ontwerp_plannen: List[OntwerpPlanResponse] = []

    # Links & indicators
    link_plan: str
    uitbreidings_indicator: Optional[str] = None
    uitbreidings_toelichting: Optional[str] = None

    # Meta
    error: Optional[str] = None


# --- Uitbreidingsmogelijkheden logica ---


def _bereken_uitbreidings_indicator(
    bestemming: str,
    heeft_bouwvlak: bool,
    max_bebouwingspercentage: Optional[int],
    max_bouwhoogte: Optional[float],
    bouwaanduidingen: List[str],
) -> tuple[Optional[str], Optional[str]]:
    """
    Schat uitbreidingsmogelijkheden in op basis van bestemmingsplandata.

    Returns:
        (indicator, toelichting) — indicator is "gunstig", "beperkt", of "ongunstig"
    """
    score = 0
    redenen: List[str] = []

    # Bestemming
    bestemming_lower = bestemming.lower()
    if "wonen" in bestemming_lower:
        score += 2
    elif "gemengd" in bestemming_lower or "centrum" in bestemming_lower:
        score += 1
    else:
        redenen.append(f"bestemming '{bestemming}' beperkt woninguitbreiding")

    # Bouwvlak
    if heeft_bouwvlak:
        score += 1
        redenen.append("bouwvlak aanwezig")
    else:
        redenen.append("geen bouwvlak gevonden")

    # Bebouwingspercentage
    if max_bebouwingspercentage is not None:
        if max_bebouwingspercentage >= 60:
            score += 2
            redenen.append(f"bebouwing tot {max_bebouwingspercentage}% toegestaan")
        elif max_bebouwingspercentage >= 40:
            score += 1
            redenen.append(f"bebouwing tot {max_bebouwingspercentage}%")
        else:
            redenen.append(f"bebouwing beperkt tot {max_bebouwingspercentage}%")

    # Bouwhoogte
    if max_bouwhoogte is not None:
        if max_bouwhoogte >= 9:  # ~3 bouwlagen
            score += 2
            redenen.append(f"bouwhoogte tot {max_bouwhoogte}m")
        elif max_bouwhoogte >= 6:  # ~2 bouwlagen
            score += 1
            redenen.append(f"bouwhoogte tot {max_bouwhoogte}m")
        else:
            redenen.append(f"bouwhoogte beperkt tot {max_bouwhoogte}m")

    # Bouwaanduidingen
    ba_lower = [b.lower() for b in bouwaanduidingen]
    if any("bijgebouw" in b for b in ba_lower):
        score += 1
        redenen.append("bijgebouwen toegestaan")

    # Bepaal indicator
    if score >= 5:
        indicator = "gunstig"
    elif score >= 3:
        indicator = "beperkt"
    else:
        indicator = "ongunstig"

    toelichting = "; ".join(redenen) if redenen else None
    return indicator, toelichting


# --- Collector singleton ---


_collector = None


def _get_collector():
    global _collector
    if _collector is None:
        _collector = create_bestemmingsplan_collector()
    return _collector


# --- Endpoints ---


@router.get("", response_model=BestemmingsplanResponse)
def get_bestemmingsplan(
    lat: float = Query(..., description="Breedtegraad (WGS84)"),
    lng: float = Query(..., description="Lengtegraad (WGS84)"),
):
    """Bestemmingsplan informatie voor een locatie."""
    collector = _get_collector()
    info = collector.get_bestemmingsplan(lat, lng)

    # Bereken uitbreidingsindicator
    indicator, toelichting = _bereken_uitbreidings_indicator(
        bestemming=info.bestemming,
        heeft_bouwvlak=info.bouwvlak is not None,
        max_bebouwingspercentage=info.max_bebouwingspercentage,
        max_bouwhoogte=info.max_bouwhoogte,
        bouwaanduidingen=info.bouwaanduidingen,
    )

    # Bouw response
    bouwvlak_resp = None
    if info.bouwvlak:
        bouwvlak_resp = BouwvlakResponse(
            geometrie=info.bouwvlak.geometrie,
            maatvoeringen=[
                MaatvoeringResponse(
                    naam=m.naam,
                    waarde=m.waarde,
                    eenheid=m.eenheid,
                    waarde_type=m.waarde_type,
                )
                for m in info.bouwvlak.maatvoeringen
            ],
        )

    ontwerp_resp = [
        OntwerpPlanResponse(**op) for op in info.ontwerp_plannen
    ]

    return BestemmingsplanResponse(
        plan_naam=info.plan_naam,
        plan_type=info.plan_type,
        plan_status=info.plan_status,
        datum_vaststelling=info.datum_vaststelling,
        bestemming=info.bestemming,
        bestemming_specifiek=info.bestemming_specifiek,
        max_bouwhoogte=info.max_bouwhoogte,
        max_goothoogte=info.max_goothoogte,
        max_bebouwingspercentage=info.max_bebouwingspercentage,
        max_inhoud=info.max_inhoud,
        bouwvlak=bouwvlak_resp,
        functieaanduidingen=info.functieaanduidingen,
        bouwaanduidingen=info.bouwaanduidingen,
        maatvoeringen=[
            MaatvoeringResponse(
                naam=m.naam,
                waarde=m.waarde,
                eenheid=m.eenheid,
                waarde_type=m.waarde_type,
            )
            for m in info.maatvoeringen
        ],
        regels_samenvatting=info.regels_samenvatting,
        regels_url=info.regels_url,
        ontwerp_plannen=ontwerp_resp,
        link_plan=info.link_plan,
        uitbreidings_indicator=indicator,
        uitbreidings_toelichting=toelichting,
        error=info.error,
    )


@router.get("/woning/{woning_id}", response_model=BestemmingsplanResponse)
def get_woning_bestemmingsplan(
    woning_id: int,
    db: Session = Depends(get_db),
):
    """Bestemmingsplan informatie voor een woning (op basis van coordinaten)."""
    woning = db.query(Woning).filter(Woning.id == woning_id).first()
    if not woning:
        return BestemmingsplanResponse(
            plan_naam="",
            plan_type="",
            plan_status="",
            bestemming="",
            link_plan="",
            error=f"Woning {woning_id} niet gevonden",
        )

    if not woning.latitude or not woning.longitude:
        return BestemmingsplanResponse(
            plan_naam="",
            plan_type="",
            plan_status="",
            bestemming="",
            link_plan="",
            error="Woning heeft geen coordinaten",
        )

    return get_bestemmingsplan(lat=woning.latitude, lng=woning.longitude)


# --- Omgevingsanalyse Response Models ---


class BurenBouwinfoResponse(BaseModel):
    bestemming: str
    max_bouwhoogte: Optional[float] = None
    max_goothoogte: Optional[float] = None
    max_bebouwingspercentage: Optional[int] = None


class OmgevingsAnalyseResponse(BaseModel):
    """GeoJSON FeatureCollection met bestemmingsvlakken + metadata."""

    type: str = "FeatureCollection"
    features: List[Dict[str, Any]] = []
    statistieken: Dict[str, int] = {}
    statistieken_pct: Dict[str, float] = {}
    ontwerp_plannen: List[OntwerpPlanResponse] = []
    buren_bouwinfo: List[BurenBouwinfoResponse] = []
    center: List[float] = []
    radius_m: float = 500
    error: Optional[str] = None


# --- Omgevingsanalyse Endpoints ---


@router.get("/omgeving", response_model=OmgevingsAnalyseResponse)
def get_omgevingsanalyse(
    lat: float = Query(..., description="Breedtegraad (WGS84)"),
    lng: float = Query(..., description="Lengtegraad (WGS84)"),
    radius_m: float = Query(500, ge=100, le=1000, description="Zoekradius in meters"),
):
    """Bestemmingen, ontwerp-plannen en buren-info in de omgeving van een locatie."""
    collector = _get_collector()
    analyse = collector.get_omgevingsanalyse(lat, lng, radius_m)

    if analyse.error:
        return OmgevingsAnalyseResponse(
            center=[lat, lng],
            radius_m=radius_m,
            error=analyse.error,
        )

    # Bouw GeoJSON features
    features: List[Dict[str, Any]] = []
    for b in analyse.bestemmingen:
        if not b.geometrie:
            continue
        features.append({
            "type": "Feature",
            "geometry": b.geometrie,
            "properties": {
                "naam": b.naam,
                "categorie": b.categorie,
                "plan_naam": b.plan_naam or "",
            },
        })

    ontwerp_resp = [
        OntwerpPlanResponse(**op) for op in analyse.ontwerp_plannen
    ]

    buren_resp = [
        BurenBouwinfoResponse(
            bestemming=bi.bestemming,
            max_bouwhoogte=bi.max_bouwhoogte,
            max_goothoogte=bi.max_goothoogte,
            max_bebouwingspercentage=bi.max_bebouwingspercentage,
        )
        for bi in analyse.buren_bouwinfo
    ]

    return OmgevingsAnalyseResponse(
        type="FeatureCollection",
        features=features,
        statistieken=analyse.statistieken,
        statistieken_pct=analyse.statistieken_pct,
        ontwerp_plannen=ontwerp_resp,
        buren_bouwinfo=buren_resp,
        center=[lat, lng],
        radius_m=radius_m,
    )


@router.get("/woning/{woning_id}/omgeving", response_model=OmgevingsAnalyseResponse)
def get_woning_omgevingsanalyse(
    woning_id: int,
    radius_m: float = Query(500, ge=100, le=1000),
    db: Session = Depends(get_db),
):
    """Omgevingsanalyse voor een woning."""
    woning = db.query(Woning).filter(Woning.id == woning_id).first()
    if not woning:
        return OmgevingsAnalyseResponse(error=f"Woning {woning_id} niet gevonden")

    if not woning.latitude or not woning.longitude:
        return OmgevingsAnalyseResponse(error="Woning heeft geen coordinaten")

    return get_omgevingsanalyse(
        lat=woning.latitude, lng=woning.longitude, radius_m=radius_m
    )
