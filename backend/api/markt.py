"""Market analysis API routes."""

from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from models import get_db, Buurt, Woning, Prijshistorie
from collectors.cbs_market_collector import create_cbs_market_collector

router = APIRouter(prefix="/api/markt", tags=["markt"])


class PrijsTrend(BaseModel):
    """Price trend for a neighborhood."""
    buurt_code: str
    buurt_naam: str
    huidige_median: Optional[int] = None
    vorige_median: Optional[int] = None
    verandering_percentage: Optional[float] = None
    aantal_woningen: int = 0


class OverbiedingStats(BaseModel):
    """Overbidding statistics."""
    gemeente: str
    percentage: float
    sample_size: int
    periode: str


class MarktOverzicht(BaseModel):
    """Market overview."""
    totaal_te_koop: int
    gemiddelde_vraagprijs: Optional[float] = None
    gemiddelde_m2_prijs: Optional[float] = None


class CBSMarktData(BaseModel):
    """CBS market data for a municipality."""
    gemeente_code: str
    gemeente_naam: str
    gemiddelde_prijs: Optional[int] = None
    prijsindex: Optional[float] = None
    aantal_transacties: Optional[int] = None
    gemiddelde_verkooptijd: Optional[int] = None  # dagen
    overbiedingspercentage: Optional[float] = None  # als percentage (bijv. 5.2)
    peildatum: Optional[str] = None
    bron: str = "CBS StatLine"
    datasets: List[str] = []


@router.get("/gemeente/{gemeente_naam}", response_model=CBSMarktData)
def get_gemeente_market_data(
    gemeente_naam: str,
):
    """
    Get CBS market data for a specific municipality.

    Returns transaction prices and market indicators from CBS StatLine:
    - Average transaction price
    - Price index (2015=100)
    - Number of transactions
    - Average time to sell (days)
    - Overbidding percentage

    Data sources:
    - CBS StatLine 83625NED: Bestaande koopwoningen; verkoopprijzen
    - CBS StatLine 83913NED: Verkooptijd en prijsontwikkeling
    """
    collector = create_cbs_market_collector()
    result = collector.get_market_data(gemeente_naam)

    return CBSMarktData(
        gemeente_code=result.gemeente_code,
        gemeente_naam=result.gemeente_naam,
        gemiddelde_prijs=result.gemiddelde_prijs,
        prijsindex=result.prijsindex,
        aantal_transacties=result.aantal_transacties,
        gemiddelde_verkooptijd=result.gemiddelde_verkooptijd,
        overbiedingspercentage=result.overbiedingspercentage,
        peildatum=result.peildatum,
        bron=result.bron,
        datasets=result.datasets,
    )


@router.get("/regionaal", response_model=List[CBSMarktData])
def get_regional_market_data():
    """
    Get CBS market data for all target municipalities in the region.

    Returns market data for Den Haag, Leidschendam-Voorburg, and Rijswijk.
    """
    collector = create_cbs_market_collector()
    results = collector.get_regional_market_data()

    return [
        CBSMarktData(
            gemeente_code=r.gemeente_code,
            gemeente_naam=r.gemeente_naam,
            gemiddelde_prijs=r.gemiddelde_prijs,
            prijsindex=r.prijsindex,
            aantal_transacties=r.aantal_transacties,
            gemiddelde_verkooptijd=r.gemiddelde_verkooptijd,
            overbiedingspercentage=r.overbiedingspercentage,
            peildatum=r.peildatum,
            bron=r.bron,
            datasets=r.datasets,
        )
        for r in results
    ]


@router.get("/trends", response_model=List[PrijsTrend])
def get_prijstrends(
    gemeente: Optional[str] = Query(None, description="Filter by municipality"),
    limit: int = Query(20, le=100),
    db: Session = Depends(get_db),
):
    """Get price trends per neighborhood."""
    query = db.query(Buurt)

    if gemeente:
        query = query.filter(Buurt.gemeente_naam.ilike(f"%{gemeente}%"))

    buurten = query.filter(Buurt.median_vraagprijs.isnot(None)).limit(limit).all()

    trends = []
    for buurt in buurten:
        # Get current active listings median
        woningen = (
            db.query(Woning)
            .filter(Woning.buurt_code == buurt.code)
            .filter(Woning.status == "active")
            .filter(Woning.vraagprijs.isnot(None))
            .all()
        )

        if woningen:
            prijzen = [w.vraagprijs for w in woningen]
            huidige_median = sorted(prijzen)[len(prijzen) // 2]
        else:
            huidige_median = buurt.median_vraagprijs

        # Calculate change percentage (comparing to stored median)
        verandering = None
        if buurt.median_vraagprijs and huidige_median:
            verandering = (
                (huidige_median - buurt.median_vraagprijs)
                / buurt.median_vraagprijs
                * 100
            )

        trends.append(
            PrijsTrend(
                buurt_code=buurt.code,
                buurt_naam=buurt.naam,
                huidige_median=huidige_median,
                vorige_median=buurt.median_vraagprijs,
                verandering_percentage=verandering,
                aantal_woningen=len(woningen),
            )
        )

    return sorted(
        trends,
        key=lambda t: t.verandering_percentage or 0,
        reverse=True,
    )


@router.get("/overbieden", response_model=List[OverbiedingStats])
def get_overbieden_stats(db: Session = Depends(get_db)):
    """
    Get overbidding percentages per municipality.

    Note: This requires sold property data with actual sale prices.
    Currently returns estimates based on market conditions.
    """
    # These are estimated values based on current Dutch market conditions
    # In production, this would be calculated from actual sold data
    stats = [
        OverbiedingStats(
            gemeente="Den Haag",
            percentage=4.5,
            sample_size=0,
            periode="Q1 2026",
        ),
        OverbiedingStats(
            gemeente="Leidschendam-Voorburg",
            percentage=5.2,
            sample_size=0,
            periode="Q1 2026",
        ),
        OverbiedingStats(
            gemeente="Rijswijk",
            percentage=4.8,
            sample_size=0,
            periode="Q1 2026",
        ),
    ]

    return stats


@router.get("/overzicht", response_model=MarktOverzicht)
def get_markt_overzicht(
    gemeente: Optional[str] = Query(None, description="Filter by municipality"),
    db: Session = Depends(get_db),
):
    """Get market overview statistics."""
    query = db.query(Woning).filter(Woning.status == "active")

    if gemeente:
        # Join with buurt to filter by municipality
        query = query.join(Buurt, Woning.buurt_code == Buurt.code)
        query = query.filter(Buurt.gemeente_naam.ilike(f"%{gemeente}%"))

    woningen = query.all()

    if not woningen:
        return MarktOverzicht(
            totaal_te_koop=0,
            gemiddelde_vraagprijs=None,
            gemiddelde_m2_prijs=None,
        )

    prijzen = [w.vraagprijs for w in woningen if w.vraagprijs]
    m2_prijzen = [
        w.vraagprijs / w.woonoppervlakte
        for w in woningen
        if w.vraagprijs and w.woonoppervlakte and w.woonoppervlakte > 0
    ]

    return MarktOverzicht(
        totaal_te_koop=len(woningen),
        gemiddelde_vraagprijs=sum(prijzen) / len(prijzen) if prijzen else None,
        gemiddelde_m2_prijs=sum(m2_prijzen) / len(m2_prijzen) if m2_prijzen else None,
    )


@router.get("/prijshistorie/{buurt_code}")
def get_buurt_prijshistorie(
    buurt_code: str,
    limit: int = Query(50, le=200),
    db: Session = Depends(get_db),
):
    """Get price history for properties in a neighborhood."""
    historie = (
        db.query(Prijshistorie)
        .join(Woning, Prijshistorie.woning_id == Woning.id)
        .filter(Woning.buurt_code == buurt_code.upper())
        .order_by(Prijshistorie.datum.desc())
        .limit(limit)
        .all()
    )

    return [
        {
            "woning_id": h.woning_id,
            "prijs": h.prijs,
            "type": h.type,
            "datum": h.datum.isoformat() if h.datum else None,
        }
        for h in historie
    ]
