"""Property (woning) listing API routes."""

import re as _re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from models import get_db, Woning

router = APIRouter(prefix="/api/woningen", tags=["woningen"])


class WoningBase(BaseModel):
    """Base woning schema."""
    id: int
    adres: str
    postcode: Optional[str] = None
    plaats: Optional[str] = None
    vraagprijs: Optional[int] = None
    woonoppervlakte: Optional[int] = None

    class Config:
        from_attributes = True


class WoningSummary(WoningBase):
    """Summary woning info for listings."""
    kamers: Optional[int] = None
    energielabel: Optional[str] = None
    bouwjaar: Optional[int] = None
    woningtype: Optional[str] = None
    url: Optional[str] = None

    @property
    def m2_prijs(self) -> Optional[float]:
        if self.vraagprijs and self.woonoppervlakte:
            return self.vraagprijs / self.woonoppervlakte
        return None


class WoningDetail(WoningSummary):
    """Detailed woning info."""
    slaapkamers: Optional[int] = None
    badkamers: Optional[int] = None
    perceeloppervlakte: Optional[int] = None
    inhoud: Optional[int] = None
    isolatie: Optional[str] = None
    verwarming: Optional[str] = None
    status: str = "active"

    # BAG enrichment
    bag_oppervlakte: Optional[int] = None
    bag_bouwjaar: Optional[int] = None
    bag_gebruiksdoel: Optional[str] = None

    # Valuation
    geschatte_waarde_laag: Optional[int] = None
    geschatte_waarde_hoog: Optional[int] = None
    biedadvies: Optional[str] = None


@router.get("/geojson")
def get_woningen_geojson(
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Get active properties as GeoJSON with coordinates from PDOK."""
    import requests as _requests

    woningen = (
        db.query(Woning)
        .filter(Woning.status == "active")
        .all()
    )

    features = []
    for woning in woningen:
        if not woning.postcode or not woning.adres:
            continue

        # Extract huisnummer from adres
        match = _re.search(r"(\d+)", woning.adres)
        if not match:
            continue
        huisnummer = match.group(1)

        # Geocode via PDOK Locatieserver
        try:
            resp = _requests.get(
                "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free",
                params={"q": f"{woning.postcode} {huisnummer}", "rows": 1},
                timeout=5,
            )
            resp.raise_for_status()
            docs = resp.json().get("response", {}).get("docs", [])
            if not docs:
                continue

            centroide = docs[0].get("centroide_ll")
            if not centroide:
                continue

            # Parse "POINT(lon lat)"
            coord_match = _re.match(r"POINT\(([\d.]+)\s+([\d.]+)\)", centroide)
            if not coord_match:
                continue

            lon = float(coord_match.group(1))
            lat = float(coord_match.group(2))
        except Exception:
            continue

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "id": woning.id,
                "adres": woning.adres,
                "postcode": woning.postcode,
                "plaats": woning.plaats,
                "vraagprijs": woning.vraagprijs,
                "woonoppervlakte": woning.woonoppervlakte,
                "energielabel": woning.energielabel,
                "woningtype": woning.woningtype,
                "kamers": woning.kamers,
            },
        })

    return {
        "type": "FeatureCollection",
        "features": features,
    }


@router.get("/", response_model=List[WoningSummary])
def list_woningen(
    min_prijs: Optional[int] = Query(None, description="Minimum asking price"),
    max_prijs: Optional[int] = Query(None, description="Maximum asking price"),
    min_oppervlakte: Optional[int] = Query(None, description="Minimum living area"),
    buurt: Optional[str] = Query(None, description="Neighborhood code"),
    energielabel: Optional[str] = Query(None, description="Energy label (A-G)"),
    status: str = Query("active", description="Property status"),
    sort_by: str = Query("vraagprijs", description="Sort field"),
    limit: int = Query(50, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """Search for properties with filters."""
    query = db.query(Woning)

    # Apply filters
    if status:
        query = query.filter(Woning.status == status)
    if min_prijs:
        query = query.filter(Woning.vraagprijs >= min_prijs)
    if max_prijs:
        query = query.filter(Woning.vraagprijs <= max_prijs)
    if min_oppervlakte:
        query = query.filter(Woning.woonoppervlakte >= min_oppervlakte)
    if buurt:
        query = query.filter(Woning.buurt_code == buurt.upper())
    if energielabel:
        query = query.filter(Woning.energielabel.ilike(f"{energielabel}%"))

    # Sort
    if sort_by == "vraagprijs":
        query = query.order_by(Woning.vraagprijs.asc().nullslast())
    elif sort_by == "woonoppervlakte":
        query = query.order_by(Woning.woonoppervlakte.desc().nullslast())
    elif sort_by == "datum":
        query = query.order_by(Woning.datum_aangemeld.desc().nullslast())

    return query.offset(offset).limit(limit).all()


@router.get("/{woning_id}", response_model=WoningDetail)
def get_woning(woning_id: int, db: Session = Depends(get_db)):
    """Get detailed info for a specific property."""
    woning = db.query(Woning).filter(Woning.id == woning_id).first()
    if not woning:
        raise HTTPException(status_code=404, detail="Woning niet gevonden")
    return woning
