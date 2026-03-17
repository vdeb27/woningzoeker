"""School API routes."""

import math
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from models import get_db
from models.school import School

router = APIRouter(prefix="/api/scholen", tags=["scholen"])


# ── Pydantic schemas ──

class SchoolBase(BaseModel):
    brin: str
    vestigingsnummer: str
    naam: str
    type: str
    gemeente: str

    class Config:
        from_attributes = True


class SchoolSummary(SchoolBase):
    onderwijstype: Optional[str] = None
    postcode: Optional[str] = None
    plaats: Optional[str] = None
    leerlingen: Optional[int] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    advies_havo_vwo_pct: Optional[float] = None
    gem_eindtoets: Optional[float] = None
    slagingspercentage: Optional[float] = None
    gem_examencijfer: Optional[float] = None
    inspectie_oordeel: Optional[str] = None


class SchoolDetail(SchoolSummary):
    straat: Optional[str] = None
    denominatie: Optional[str] = None
    data_jaar: Optional[str] = None


class SchoolNabij(SchoolSummary):
    afstand_m: float  # Afstand in meters


# ── Haversine ──

def _haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Bereken afstand in meters tussen twee coördinaten."""
    R = 6371000  # Aardstraal in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ── Endpoints ──

@router.get("/", response_model=List[SchoolSummary])
def list_scholen(
    gemeente: Optional[str] = Query(None, description="Filter op gemeente"),
    type: Optional[str] = Query(None, description="Filter op type: basisonderwijs of voortgezet"),
    denominatie: Optional[str] = Query(None, description="Filter op denominatie"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """Lijst van scholen, optioneel gefilterd."""
    query = db.query(School)

    if gemeente:
        query = query.filter(School.gemeente.ilike(f"%{gemeente}%"))
    if type:
        query = query.filter(School.type == type)
    if denominatie:
        query = query.filter(School.denominatie.ilike(f"%{denominatie}%"))

    return query.order_by(School.naam).offset(offset).limit(limit).all()


@router.get("/nabij", response_model=List[SchoolNabij])
def scholen_nabij(
    lat: float = Query(..., description="Breedtegraad"),
    lng: float = Query(..., description="Lengtegraad"),
    radius: int = Query(2000, ge=100, le=10000, description="Zoekradius in meters"),
    type: Optional[str] = Query(None, description="Filter op type"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Scholen binnen een radius, gesorteerd op afstand."""
    query = db.query(School).filter(School.lat.isnot(None), School.lng.isnot(None))

    if type:
        query = query.filter(School.type == type)

    # Bounding box pre-filter (grof, ~111km per graad lat)
    dlat = radius / 111000
    dlng = radius / (111000 * math.cos(math.radians(lat)))
    query = query.filter(
        School.lat.between(lat - dlat, lat + dlat),
        School.lng.between(lng - dlng, lng + dlng),
    )

    schools = query.all()

    # Exact afstand berekenen en filteren
    results = []
    for s in schools:
        afstand = _haversine(lat, lng, s.lat, s.lng)
        if afstand <= radius:
            school_dict = SchoolSummary.model_validate(s).model_dump()
            school_dict["afstand_m"] = round(afstand)
            results.append(school_dict)

    results.sort(key=lambda x: x["afstand_m"])
    return results[:limit]


@router.get("/{brin}/{vestiging}", response_model=SchoolDetail)
def get_school(
    brin: str,
    vestiging: str,
    db: Session = Depends(get_db),
):
    """Detail van een specifieke school."""
    school = db.query(School).filter(
        School.brin == brin,
        School.vestigingsnummer == vestiging,
    ).first()

    if not school:
        raise HTTPException(status_code=404, detail="School niet gevonden")

    return school
