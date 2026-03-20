"""Neighborhood (buurt) API routes."""

import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import or_
from sqlalchemy.orm import Session

from models import get_db, Buurt
from services.scoring import ScoringService

router = APIRouter(prefix="/api/buurten", tags=["buurten"])

# Standaard gemeenten voor de buurt-selector (CBS-namen, used with ilike)
DEFAULT_GEMEENTEN = ["'s-Gravenhage", "Leidschendam-Voorburg", "Rijswijk"]
# Mapping van frontend-namen naar CBS-namen
GEMEENTE_ALIAS = {"Den Haag": "'s-Gravenhage"}

# Singleton scoring service for metadata
_scoring_service = None


def _get_scoring_service() -> ScoringService:
    global _scoring_service
    if _scoring_service is None:
        _scoring_service = ScoringService()
    return _scoring_service


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
    score_bereikbaarheid: Optional[float] = None
    score_leefbaarheid: Optional[float] = None
    score_coverage: Optional[float] = None

    leefbaarometer_score: Optional[float] = None
    leefbaarometer_fysiek: Optional[float] = None
    leefbaarometer_voorzieningen: Optional[float] = None
    leefbaarometer_veiligheid: Optional[float] = None
    leefbaarometer_bevolking: Optional[float] = None
    leefbaarometer_woningen: Optional[float] = None

    median_vraagprijs: Optional[int] = None
    median_m2_prijs: Optional[float] = None
    aantal_te_koop: Optional[int] = None

    indicatoren: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True


class IndicatorMeta(BaseModel):
    """Metadata for a single indicator."""
    label: str
    category: Optional[str] = None
    unit: str = ""
    higher_is_better: bool = True
    weight: float = 0.0
    description: str = ""


class CategoryMeta(BaseModel):
    """Metadata for a category."""
    label: str
    color: str = "#6b7280"
    weight: float = 0.0
    indicators: List[str] = []


class IndicatorMetaResponse(BaseModel):
    """Response for indicator metadata endpoint."""
    indicators: Dict[str, IndicatorMeta]
    categories: Dict[str, CategoryMeta]


class BuurtVergelijk(BaseModel):
    """Schema for neighborhood comparison."""
    buurten: List[BuurtDetail]
    categories: Dict[str, CategoryMeta] = {}


@router.get("/indicatoren/meta", response_model=IndicatorMetaResponse)
def get_indicator_meta():
    """Get metadata for all available indicators and categories."""
    scorer = _get_scoring_service()
    ind_meta = scorer.get_indicator_meta()
    cat_meta = scorer.get_category_meta()

    return IndicatorMetaResponse(
        indicators={
            k: IndicatorMeta(**v) for k, v in ind_meta.items()
        },
        categories={
            k: CategoryMeta(**v) for k, v in cat_meta.items()
        },
    )


@router.get("/geojson")
def get_buurten_geojson(
    gemeente: Optional[str] = Query(None, description="Filter by municipality"),
    min_score: Optional[float] = Query(None, ge=0, le=1, description="Minimum score"),
    indicator: Optional[str] = Query(None, description="Indicator to include in properties"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Get buurt boundaries as GeoJSON FeatureCollection."""
    query = db.query(Buurt).filter(Buurt.geometrie.isnot(None))

    if gemeente:
        cbs_naam = GEMEENTE_ALIAS.get(gemeente, gemeente)
        query = query.filter(Buurt.gemeente_naam.ilike(f"%{cbs_naam}%"))
    else:
        query = query.filter(or_(
                *[Buurt.gemeente_naam.ilike(f"%{g}%") for g in DEFAULT_GEMEENTEN]
            ))

    if min_score is not None:
        query = query.filter(Buurt.score_totaal >= min_score)

    buurten = query.all()

    features = []
    for buurt in buurten:
        geometry = buurt.geometrie
        if isinstance(geometry, str):
            geometry = json.loads(geometry)

        properties = {
            "code": buurt.code,
            "naam": buurt.naam,
            "gemeente_naam": buurt.gemeente_naam,
            "score_totaal": buurt.score_totaal,
            "median_vraagprijs": buurt.median_vraagprijs,
            "aantal_te_koop": buurt.aantal_te_koop,
            # Category scores for tooltip
            "score_inkomen": buurt.score_inkomen,
            "score_veiligheid": buurt.score_veiligheid,
            "score_voorzieningen": buurt.score_voorzieningen,
            "score_woningen": buurt.score_woningen,
        }

        # Add specific indicator value if requested
        if indicator:
            indicator_value = None
            # Check model columns first
            if hasattr(buurt, indicator):
                indicator_value = getattr(buurt, indicator)
            # Then check indicatoren JSON
            elif buurt.indicatoren and indicator in buurt.indicatoren:
                indicator_value = buurt.indicatoren[indicator]

            properties["indicator_value"] = indicator_value
            properties["indicator_key"] = indicator

        features.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": properties,
        })

    return {
        "type": "FeatureCollection",
        "features": features,
    }


@router.get("/", response_model=List[BuurtDetail])
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
        cbs_naam = GEMEENTE_ALIAS.get(gemeente, gemeente)
        query = query.filter(Buurt.gemeente_naam.ilike(f"%{cbs_naam}%"))
    else:
        query = query.filter(or_(
                *[Buurt.gemeente_naam.ilike(f"%{g}%") for g in DEFAULT_GEMEENTEN]
            ))

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

    scorer = _get_scoring_service()
    cat_meta = scorer.get_category_meta()

    return BuurtVergelijk(
        buurten=[BuurtDetail.model_validate(b) for b in buurten],
        categories={k: CategoryMeta(**v) for k, v in cat_meta.items()},
    )


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
