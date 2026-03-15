"""Neighborhood (buurt) API routes."""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from models import get_db, Buurt

router = APIRouter(prefix="/api/buurten", tags=["buurten"])


class BuurtBase(BaseModel):
    """Base buurt schema."""
    code: str
    naam: str
    gemeente_naam: Optional[str] = None


class BuurtSummary(BuurtBase):
    """Summary buurt info for listings."""
    score_totaal: Optional[float] = None
    median_vraagprijs: Optional[int] = None
    aantal_te_koop: Optional[int] = None

    class Config:
        from_attributes = True


class BuurtDetail(BuurtBase):
    """Detailed buurt info."""
    wijk_naam: Optional[str] = None
    inwoners: Optional[int] = None
    huishoudens: Optional[int] = None
    gemiddeld_inkomen: Optional[float] = None
    woz_waarde: Optional[float] = None

    score_totaal: Optional[float] = None
    score_inkomen: Optional[float] = None
    score_veiligheid: Optional[float] = None
    score_voorzieningen: Optional[float] = None
    score_woningen: Optional[float] = None
    score_coverage: Optional[float] = None

    leefbaarometer_score: Optional[float] = None
    median_vraagprijs: Optional[int] = None
    median_m2_prijs: Optional[float] = None
    aantal_te_koop: Optional[int] = None

    class Config:
        from_attributes = True


class BuurtVergelijk(BaseModel):
    """Schema for neighborhood comparison."""
    buurten: List[BuurtDetail]


@router.get("/", response_model=List[BuurtSummary])
def list_buurten(
    gemeente: Optional[str] = Query(None, description="Filter by municipality"),
    min_score: Optional[float] = Query(None, ge=0, le=1, description="Minimum score"),
    sort_by: str = Query("score_totaal", description="Sort field"),
    limit: int = Query(100, le=500),
    db: Session = Depends(get_db),
):
    """Get all neighborhoods with summary info."""
    query = db.query(Buurt)

    if gemeente:
        query = query.filter(Buurt.gemeente_naam.ilike(f"%{gemeente}%"))

    if min_score is not None:
        query = query.filter(Buurt.score_totaal >= min_score)

    # Sort
    if sort_by == "score_totaal":
        query = query.order_by(Buurt.score_totaal.desc().nullslast())
    elif sort_by == "naam":
        query = query.order_by(Buurt.naam)
    elif sort_by == "median_vraagprijs":
        query = query.order_by(Buurt.median_vraagprijs.asc().nullslast())

    return query.limit(limit).all()


@router.get("/{code}", response_model=BuurtDetail)
def get_buurt(code: str, db: Session = Depends(get_db)):
    """Get detailed info for a specific neighborhood."""
    buurt = db.query(Buurt).filter(Buurt.code == code.upper()).first()
    if not buurt:
        raise HTTPException(status_code=404, detail="Buurt niet gevonden")
    return buurt


@router.get("/{code}/woningen")
def get_buurt_woningen(
    code: str,
    limit: int = Query(50, le=200),
    db: Session = Depends(get_db),
):
    """Get properties in a specific neighborhood."""
    from models import Woning

    buurt = db.query(Buurt).filter(Buurt.code == code.upper()).first()
    if not buurt:
        raise HTTPException(status_code=404, detail="Buurt niet gevonden")

    woningen = (
        db.query(Woning)
        .filter(Woning.buurt_code == code.upper())
        .filter(Woning.status == "active")
        .order_by(Woning.vraagprijs.asc())
        .limit(limit)
        .all()
    )

    return {
        "buurt": BuurtSummary.model_validate(buurt),
        "woningen": woningen,
        "count": len(woningen),
    }


@router.get("/vergelijk/", response_model=BuurtVergelijk)
def vergelijk_buurten(
    codes: List[str] = Query(..., description="Buurt codes to compare", max_length=5),
    db: Session = Depends(get_db),
):
    """Compare up to 5 neighborhoods side by side."""
    if len(codes) > 5:
        raise HTTPException(
            status_code=400,
            detail="Maximaal 5 buurten kunnen worden vergeleken"
        )

    codes_upper = [c.upper() for c in codes]
    buurten = db.query(Buurt).filter(Buurt.code.in_(codes_upper)).all()

    if len(buurten) != len(codes):
        found = {b.code for b in buurten}
        missing = set(codes_upper) - found
        raise HTTPException(
            status_code=404,
            detail=f"Buurten niet gevonden: {', '.join(missing)}"
        )

    return BuurtVergelijk(buurten=[BuurtDetail.model_validate(b) for b in buurten])
