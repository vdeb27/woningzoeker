"""Property valuation (waardebepaling) API routes."""

import os
import re
from typing import Any, Dict, List, Optional

import requests

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from models import get_db, Woning, Transactie, GemeentelijkMonument
from services import ValuationService
from collectors.woz_collector import create_woz_collector
from collectors.energielabel_collector import create_energielabel_collector
from collectors.kadaster_collector import create_kadaster_collector, TransactionRecord
from collectors.miljoenhuizen_collector import create_miljoenhuizen_collector, MiljoenhuizenWoning
from collectors.cbs_market_collector import create_cbs_market_collector
from collectors.cbs_buurt_collector import create_cbs_buurt_collector, lookup_buurt_code_pdok, geocode_address_pdok
from collectors.bag_collector import BagClient
from collectors.rce_collector import create_rce_collector
from collectors.pdok_beschermde_gebieden_collector import create_pdok_beschermde_gebieden_collector
from collectors.funda_collector import create_funda_collector, PropertyListing as FundaPropertyListing
from datetime import datetime, timedelta
from sqlalchemy import and_
from utils.address import parse_huisnummer

router = APIRouter(prefix="/api/woningen", tags=["waardebepaling"])


# ============================================================================
# Request/Response Models
# ============================================================================

class WaardebepalingRequest(BaseModel):
    """Request for property valuation."""
    woonoppervlakte: int
    buurt_code: Optional[str] = None
    energielabel: Optional[str] = None
    bouwjaar: Optional[int] = None
    woningtype: Optional[str] = None
    vraagprijs: Optional[int] = None


class WaardebepalingResponse(BaseModel):
    """Response with property valuation."""
    waarde_laag: int
    waarde_hoog: int
    waarde_midden: int
    vraagprijs: Optional[int] = None
    verschil_percentage: Optional[float] = None

    bied_advies: str
    bied_range_laag: int
    bied_range_hoog: int

    basis_waarde: int
    energielabel_correctie: int
    bouwjaar_correctie: int
    woningtype_correctie: int
    perceel_correctie: int = 0
    buurt_kwaliteit_correctie: int = 0
    markt_correctie: int

    confidence: float
    confidence_factors: dict


class WOZResponse(BaseModel):
    """Response with WOZ value for an address."""
    postcode: str
    huisnummer: int
    huisletter: Optional[str] = None
    toevoeging: Optional[str] = None
    woz_waarde: Optional[int] = None
    peildatum: Optional[str] = None
    peiljaar: Optional[int] = None
    adres: Optional[str] = None
    woonplaats: Optional[str] = None
    error: Optional[str] = None


class EnergielabelResponse(BaseModel):
    """Response with energy label for an address."""
    postcode: str
    huisnummer: int
    huisletter: Optional[str] = None
    toevoeging: Optional[str] = None
    energielabel: Optional[str] = None
    energieindex: Optional[float] = None
    registratiedatum: Optional[str] = None
    geldig_tot: Optional[str] = None
    gebouwtype: Optional[str] = None
    bouwjaar: Optional[int] = None
    gebruiksoppervlakte: Optional[float] = None
    error: Optional[str] = None


class TransactionResponse(BaseModel):
    """A single property transaction."""
    postcode: Optional[str] = None
    huisnummer: Optional[int] = None
    straat: Optional[str] = None
    woonplaats: Optional[str] = None
    transactie_datum: Optional[str] = None
    transactie_prijs: Optional[int] = None
    oppervlakte: Optional[int] = None
    prijs_per_m2: Optional[float] = None
    bouwjaar: Optional[int] = None
    woningtype: Optional[str] = None


class ComparablesResponse(BaseModel):
    """Response with comparable sales."""
    target_postcode: str
    target_huisnummer: int
    target_address: Optional[str] = None
    transactions: List[TransactionResponse] = []
    avg_prijs_per_m2: Optional[float] = None
    count: int = 0
    search_radius_pc4: bool = True
    error: Optional[str] = None


class MiljoenhuizenVerkoop(BaseModel):
    """A comparable sale from Miljoenhuizen.nl."""
    url: str
    adres: str
    postcode: str
    plaats: str
    laatste_vraagprijs: Optional[int] = None
    verkoopdatum: Optional[str] = None
    woonoppervlakte: Optional[int] = None
    prijs_per_m2: Optional[float] = None
    bouwjaar: Optional[int] = None
    woningtype: Optional[str] = None
    geschatte_waarde_laag: Optional[int] = None
    geschatte_waarde_hoog: Optional[int] = None


class FundaListing(BaseModel):
    """Funda listing data for a property."""
    url: str
    adres: str
    postcode: Optional[str] = None
    plaats: Optional[str] = None
    vraagprijs: Optional[int] = None
    vraagprijs_suffix: Optional[str] = None
    woonoppervlakte: Optional[int] = None
    perceeloppervlakte: Optional[int] = None
    inhoud: Optional[int] = None
    prijs_per_m2: Optional[float] = None
    kamers: Optional[int] = None
    slaapkamers: Optional[int] = None
    badkamers: Optional[int] = None
    bouwjaar: Optional[int] = None
    woningtype: Optional[str] = None
    bouwtype: Optional[str] = None
    energielabel: Optional[str] = None
    # Eigendomsituatie
    eigendom_type: Optional[str] = None
    vve_bijdrage: Optional[int] = None
    erfpacht_bedrag: Optional[int] = None
    # Tuin & buitenruimte
    tuin_type: Optional[str] = None
    tuin_oppervlakte: Optional[int] = None
    tuin_orientatie: Optional[str] = None
    buitenruimte: Optional[int] = None
    balkon: Optional[bool] = None
    dakterras: Optional[bool] = None
    # Indeling & parkeren
    verdiepingen: Optional[int] = None
    garage_type: Optional[str] = None
    parkeerplaatsen: Optional[int] = None
    parkeer_type: Optional[str] = None
    kelder: Optional[bool] = None
    zolder: Optional[str] = None
    berging: Optional[str] = None
    # Extra
    isolatie: Optional[str] = None
    verwarming: Optional[str] = None
    dak_type: Optional[str] = None
    aangeboden_sinds: Optional[str] = None
    status: str = "beschikbaar"
    # Verkocht-specifiek
    verkoopdatum: Optional[str] = None
    looptijd_dagen: Optional[int] = None


class AddressLookupRequest(BaseModel):
    """Request for address-based data lookup."""
    postcode: str
    huisnummer: int
    huisletter: Optional[str] = None
    toevoeging: Optional[str] = None


class EnhancedWaardebepalingRequest(BaseModel):
    """Request for enhanced property valuation with address lookup."""
    postcode: str
    huisnummer: int
    huisletter: Optional[str] = None
    toevoeging: Optional[str] = None
    woonoppervlakte: Optional[int] = None
    vraagprijs: Optional[int] = None
    woningtype: Optional[str] = None


class EnhancedWaardebepalingResponse(BaseModel):
    """Enhanced valuation response with WOZ and energielabel data."""
    # Address info
    postcode: str
    huisnummer: int
    adres: Optional[str] = None

    # WOZ data
    woz_waarde: Optional[int] = None
    woz_peiljaar: Optional[int] = None
    grondoppervlakte: Optional[int] = None

    # Woninggegevens (auto-fetched)
    woonoppervlakte: Optional[int] = None
    bouwjaar: Optional[int] = None
    woningtype: Optional[str] = None

    # Energielabel (auto-fetched)
    energielabel: Optional[str] = None
    energielabel_bron: str = "auto"

    # Valuation
    waarde_laag: int
    waarde_hoog: int
    waarde_midden: int
    vraagprijs: Optional[int] = None
    verschil_percentage: Optional[float] = None

    # Bidding advice
    bied_advies: str
    bied_range_laag: int
    bied_range_hoog: int

    # Breakdown
    basis_waarde: int
    energielabel_correctie: int
    bouwjaar_correctie: int
    woningtype_correctie: int
    perceel_correctie: int = 0
    buurt_kwaliteit_correctie: int = 0
    markt_correctie: int

    confidence: float
    confidence_factors: dict

    # Comparables summary
    comparables_count: int = 0
    comparables_avg_m2: Optional[float] = None

    # Miljoenhuizen vergelijkbare verkopen
    miljoenhuizen_verkopen: List[MiljoenhuizenVerkoop] = []
    miljoenhuizen_count: int = 0
    miljoenhuizen_avg_vraagprijs: Optional[int] = None

    # Market data (CBS StatLine)
    markt_gem_prijs: Optional[int] = None
    markt_overbiedpct: Optional[float] = None
    markt_verkooptijd: Optional[int] = None
    markt_peildatum: Optional[str] = None

    # Buurt data (CBS Kerncijfers)
    buurt_code: Optional[str] = None
    buurt_naam: Optional[str] = None
    buurt_gem_woz: Optional[int] = None
    buurt_koopwoningen_pct: Optional[float] = None
    buurt_gem_inkomen: Optional[int] = None

    # Data sources used
    data_bronnen: List[str] = []

    # Monument status
    monument: Optional["MonumentResponse"] = None

    # Funda listing
    funda_listing: Optional[FundaListing] = None

    # Saved woning reference
    woning_id: Optional[int] = None


# ============================================================================
# Monument Status Models
# ============================================================================

class RijksmonumentInfo(BaseModel):
    """Rijksmonument details."""
    is_monument: bool = False
    monumentnummer: Optional[int] = None
    omschrijving: Optional[str] = None
    categorie: Optional[str] = None
    url: Optional[str] = None


class GemeentelijkMonumentInfo(BaseModel):
    """Gemeentelijk monument details."""
    is_monument: bool = False
    gemeente: Optional[str] = None
    omschrijving: Optional[str] = None


class BeschermdGezichtInfo(BaseModel):
    """Beschermd stads-/dorpsgezicht details."""
    in_beschermd_gezicht: bool = False
    naam: Optional[str] = None
    type: Optional[str] = None  # "stadsgezicht" or "dorpsgezicht"
    niveau: Optional[str] = None  # "rijks" or "gemeentelijk"


class UnescoInfo(BaseModel):
    """UNESCO Werelderfgoed details."""
    in_unesco: bool = False
    naam: Optional[str] = None


class MonumentResponse(BaseModel):
    """Combined monument status response."""
    rijksmonument: Optional[RijksmonumentInfo] = None
    gemeentelijk_monument: Optional[GemeentelijkMonumentInfo] = None
    beschermd_gezicht: Optional[BeschermdGezichtInfo] = None
    unesco: Optional[UnescoInfo] = None
    heeft_monumentstatus: bool = False


# ============================================================================
# Internal helpers
# ============================================================================

def _get_db_comparables(
    db: Session,
    postcode: str,
    huisnummer: int,
    max_years: int = 2,
) -> List[TransactionRecord]:
    """Query the transacties table for comparable sales in the same PC4 area."""
    pc = postcode.replace(" ", "").upper()
    pc4 = pc[:4]
    date_since = datetime.now() - timedelta(days=max_years * 365)

    rows = db.query(Transactie).filter(
        and_(
            Transactie.pc4 == pc4,
            Transactie.transactie_datum >= date_since.date(),
            ~and_(Transactie.postcode == pc, Transactie.huisnummer == huisnummer),
        )
    ).order_by(Transactie.transactie_datum.desc()).all()

    return [
        TransactionRecord(
            postcode=row.postcode,
            huisnummer=row.huisnummer,
            straat=row.straat,
            woonplaats=row.woonplaats,
            transactie_datum=str(row.transactie_datum) if row.transactie_datum else None,
            transactie_prijs=row.transactie_prijs,
            koopsom=row.transactie_prijs,
            oppervlakte=row.woonoppervlakte,
            prijs_per_m2=row.prijs_per_m2,
            bouwjaar=row.bouwjaar,
            woningtype=row.woningtype,
            koopjaar=row.koopjaar,
        )
        for row in rows
    ]


def _lookup_woz(postcode: str, huisnummer: int, huisletter: Optional[str] = None, toevoeging: Optional[str] = None):
    """Shared WOZ lookup logic."""
    collector = create_woz_collector()
    result = collector.get_woz_value(
        postcode=postcode,
        huisnummer=huisnummer,
        huisletter=huisletter,
        toevoeging=toevoeging,
    )
    return WOZResponse(
        postcode=result.postcode,
        huisnummer=result.huisnummer,
        huisletter=result.huisletter,
        toevoeging=result.toevoeging,
        woz_waarde=result.woz_waarde,
        peildatum=result.peildatum,
        peiljaar=result.peiljaar,
        adres=result.adres,
        woonplaats=result.woonplaats,
        error=result.error,
    )


def _lookup_energielabel(postcode: str, huisnummer: int, huisletter: Optional[str] = None, toevoeging: Optional[str] = None):
    """Shared energielabel lookup logic."""
    collector = create_energielabel_collector()
    result = collector.get_energielabel(
        postcode=postcode,
        huisnummer=huisnummer,
        huisletter=huisletter,
        toevoeging=toevoeging,
    )
    return EnergielabelResponse(
        postcode=result.postcode,
        huisnummer=result.huisnummer,
        huisletter=result.huisletter,
        toevoeging=result.toevoeging,
        energielabel=result.energielabel,
        energieindex=result.energieindex,
        registratiedatum=result.registratiedatum,
        geldig_tot=result.geldig_tot,
        gebouwtype=result.gebouwtype,
        bouwjaar=result.bouwjaar,
        gebruiksoppervlakte=result.gebruiksoppervlakte,
        error=result.error,
    )


# ============================================================================
# Valuation Endpoints
# ============================================================================

@router.get("/{woning_id}/waarde", response_model=WaardebepalingResponse)
def get_woning_waarde(woning_id: int, db: Session = Depends(get_db)):
    """Get valuation for a specific property."""
    woning = db.query(Woning).filter(Woning.id == woning_id).first()
    if not woning:
        raise HTTPException(status_code=404, detail="Woning niet gevonden")

    if not woning.woonoppervlakte:
        raise HTTPException(
            status_code=400,
            detail="Woonoppervlakte is vereist voor waardebepaling"
        )

    service = ValuationService(db)
    result = service.estimate_value(
        woonoppervlakte=woning.woonoppervlakte,
        buurt_code=woning.buurt_code,
        energielabel=woning.energielabel,
        bouwjaar=woning.bouwjaar or woning.bag_bouwjaar,
        woningtype=woning.woningtype,
        vraagprijs=woning.vraagprijs,
    )

    return WaardebepalingResponse(
        waarde_laag=result.waarde_laag,
        waarde_hoog=result.waarde_hoog,
        waarde_midden=result.waarde_midden,
        vraagprijs=result.vraagprijs,
        verschil_percentage=result.verschil_percentage,
        bied_advies=result.bied_advies.value,
        bied_range_laag=result.bied_range_laag,
        bied_range_hoog=result.bied_range_hoog,
        basis_waarde=result.basis_waarde,
        energielabel_correctie=result.energielabel_correctie,
        bouwjaar_correctie=result.bouwjaar_correctie,
        woningtype_correctie=result.woningtype_correctie,
        perceel_correctie=result.perceel_correctie,
        buurt_kwaliteit_correctie=result.buurt_kwaliteit_correctie,
        markt_correctie=result.markt_correctie,
        confidence=result.confidence,
        confidence_factors=result.confidence_factors,
    )


@router.post("/waardebepaling", response_model=WaardebepalingResponse)
def bereken_waarde(request: WaardebepalingRequest, db: Session = Depends(get_db)):
    """Calculate property valuation based on provided details."""
    service = ValuationService(db)
    result = service.estimate_value(
        woonoppervlakte=request.woonoppervlakte,
        buurt_code=request.buurt_code,
        energielabel=request.energielabel,
        bouwjaar=request.bouwjaar,
        woningtype=request.woningtype,
        vraagprijs=request.vraagprijs,
    )

    return WaardebepalingResponse(
        waarde_laag=result.waarde_laag,
        waarde_hoog=result.waarde_hoog,
        waarde_midden=result.waarde_midden,
        vraagprijs=result.vraagprijs,
        verschil_percentage=result.verschil_percentage,
        bied_advies=result.bied_advies.value,
        bied_range_laag=result.bied_range_laag,
        bied_range_hoog=result.bied_range_hoog,
        basis_waarde=result.basis_waarde,
        energielabel_correctie=result.energielabel_correctie,
        bouwjaar_correctie=result.bouwjaar_correctie,
        woningtype_correctie=result.woningtype_correctie,
        perceel_correctie=result.perceel_correctie,
        buurt_kwaliteit_correctie=result.buurt_kwaliteit_correctie,
        markt_correctie=result.markt_correctie,
        confidence=result.confidence,
        confidence_factors=result.confidence_factors,
    )


# ============================================================================
# WOZ Value Endpoints
# ============================================================================

@router.get("/woz", response_model=WOZResponse)
def get_woz_value(
    postcode: str = Query(..., description="Postcode (e.g., '2511AB')"),
    huisnummer: int = Query(..., description="House number"),
    huisletter: Optional[str] = Query(None, description="House letter (e.g., 'A')"),
    toevoeging: Optional[str] = Query(None, description="House number suffix"),
):
    """Get WOZ (property tax) value for an address."""
    return _lookup_woz(postcode, huisnummer, huisletter, toevoeging)


@router.post("/woz", response_model=WOZResponse)
def lookup_woz_value(request: AddressLookupRequest):
    """Get WOZ value for an address (POST version)."""
    return _lookup_woz(request.postcode, request.huisnummer, request.huisletter, request.toevoeging)


@router.get("/{woning_id}/woz", response_model=WOZResponse)
def get_woning_woz(woning_id: int, db: Session = Depends(get_db)):
    """Get WOZ value for a property in the database."""
    woning = db.query(Woning).filter(Woning.id == woning_id).first()
    if not woning:
        raise HTTPException(status_code=404, detail="Woning niet gevonden")

    if not woning.postcode:
        raise HTTPException(status_code=400, detail="Woning heeft geen postcode")

    huisnummer, huisletter = parse_huisnummer(woning.adres)
    if not huisnummer:
        raise HTTPException(
            status_code=400,
            detail="Kan huisnummer niet bepalen uit adres"
        )

    return _lookup_woz(woning.postcode, huisnummer, huisletter)


# ============================================================================
# Energielabel Endpoints
# ============================================================================

@router.get("/energielabel", response_model=EnergielabelResponse)
def get_energielabel(
    postcode: str = Query(..., description="Postcode (e.g., '2511AB')"),
    huisnummer: int = Query(..., description="House number"),
    huisletter: Optional[str] = Query(None, description="House letter"),
    toevoeging: Optional[str] = Query(None, description="House number suffix"),
):
    """Get official energy label for an address from EP-Online (RVO)."""
    return _lookup_energielabel(postcode, huisnummer, huisletter, toevoeging)


@router.post("/energielabel", response_model=EnergielabelResponse)
def lookup_energielabel(request: AddressLookupRequest):
    """Get energy label for an address (POST version)."""
    return _lookup_energielabel(request.postcode, request.huisnummer, request.huisletter, request.toevoeging)


# ============================================================================
# Comparable Sales Endpoints
# ============================================================================

@router.get("/comparables", response_model=ComparablesResponse)
def get_comparables(
    postcode: str = Query(..., description="Postcode"),
    huisnummer: int = Query(..., description="House number"),
    oppervlakte: Optional[int] = Query(None, description="Living area in m2 for filtering"),
    max_years: int = Query(2, description="Maximum age of transactions in years"),
    max_results: int = Query(10, le=25, description="Maximum number of comparables"),
    db: Session = Depends(get_db),
):
    """Get comparable recent sales, combining local DB with live OpenKadaster scraping."""
    pc = postcode.replace(" ", "").upper()

    # 1. Query local database first (bulk-downloaded data)
    db_transactions = _get_db_comparables(db, postcode, huisnummer, max_years)

    # 2. Also try live scraping from OpenKadaster
    collector = create_kadaster_collector()
    result = collector.get_comparables(
        postcode=postcode,
        huisnummer=huisnummer,
        oppervlakte=oppervlakte,
        max_years=max_years,
        max_results=max_results,
    )

    # 3. Merge: deduplicate by (postcode, huisnummer, transactie_datum)
    seen = set()
    merged = []

    for t in db_transactions:
        key = (t.postcode, t.huisnummer, t.transactie_datum)
        if key not in seen:
            seen.add(key)
            merged.append(t)

    for t in result.transactions:
        key = (t.postcode, t.huisnummer, t.transactie_datum)
        if key not in seen:
            seen.add(key)
            merged.append(t)

    # Sort by relevance: same PC6 > same PC4, and more recent first
    def sort_key(t):
        pc6_match = 1 if t.postcode == pc else 0
        opp_match = 0
        if oppervlakte and t.oppervlakte:
            diff = abs(t.oppervlakte - oppervlakte) / oppervlakte
            opp_match = 1 if diff < 0.2 else 0
        date_str = t.transactie_datum or ""
        return (pc6_match, opp_match, date_str)

    merged.sort(key=sort_key, reverse=True)
    merged = merged[:max_results]

    transactions = [
        TransactionResponse(
            postcode=t.postcode,
            huisnummer=t.huisnummer,
            straat=t.straat,
            woonplaats=t.woonplaats,
            transactie_datum=t.transactie_datum,
            transactie_prijs=t.transactie_prijs or t.koopsom,
            oppervlakte=t.oppervlakte,
            prijs_per_m2=t.prijs_per_m2,
            bouwjaar=t.bouwjaar,
            woningtype=t.woningtype,
        )
        for t in merged
    ]

    avg_m2 = None
    prices_m2 = [t.prijs_per_m2 for t in merged if t.prijs_per_m2]
    if prices_m2:
        avg_m2 = sum(prices_m2) / len(prices_m2)

    return ComparablesResponse(
        target_postcode=result.target_postcode,
        target_huisnummer=result.target_huisnummer,
        target_address=result.target_address,
        transactions=transactions,
        avg_prijs_per_m2=avg_m2 or result.avg_prijs_per_m2,
        count=len(transactions),
        search_radius_pc4=True,
        error=result.error,
    )


@router.get("/{woning_id}/comparables", response_model=ComparablesResponse)
def get_woning_comparables(woning_id: int, db: Session = Depends(get_db)):
    """Get comparable sales for a property in the database."""
    woning = db.query(Woning).filter(Woning.id == woning_id).first()
    if not woning:
        raise HTTPException(status_code=404, detail="Woning niet gevonden")

    if not woning.postcode:
        raise HTTPException(status_code=400, detail="Woning heeft geen postcode")

    huisnummer, _ = parse_huisnummer(woning.adres)
    if not huisnummer:
        raise HTTPException(
            status_code=400,
            detail="Kan huisnummer niet bepalen uit adres"
        )

    collector = create_kadaster_collector()
    result = collector.get_comparables(
        postcode=woning.postcode,
        huisnummer=huisnummer,
        oppervlakte=woning.woonoppervlakte,
    )

    transactions = [
        TransactionResponse(
            postcode=t.postcode,
            huisnummer=t.huisnummer,
            straat=t.straat,
            woonplaats=t.woonplaats,
            transactie_datum=t.transactie_datum,
            transactie_prijs=t.transactie_prijs or t.koopsom,
            oppervlakte=t.oppervlakte,
            prijs_per_m2=t.prijs_per_m2,
            bouwjaar=t.bouwjaar,
            woningtype=t.woningtype,
        )
        for t in result.transactions
    ]

    return ComparablesResponse(
        target_postcode=result.target_postcode,
        target_huisnummer=result.target_huisnummer,
        target_address=woning.adres,
        transactions=transactions,
        avg_prijs_per_m2=result.avg_prijs_per_m2,
        count=result.count,
        search_radius_pc4=result.search_radius_pc4,
        error=result.error,
    )


# ============================================================================
# Monument Status Endpoints
# ============================================================================

WKPB_WFS_URL = "https://service.pdok.nl/kadaster/wkpb/wfs/v1_0"
PDOK_LOCATIE_URL = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"


def _lookup_wkpb(postcode: str, huisnummer: int) -> Dict[str, Any]:
    """
    Query Kadaster WKPB (publiekrechtelijke beperkingen) for monument status.

    Returns dict with keys: gemeentelijk_monument (bool), rijksmonument (bool),
    gem_omschrijving, gem_inschrijfdatum, rk_omschrijving, rk_inschrijfdatum.
    """
    import re as _re

    result = {
        "gemeentelijk_monument": False,
        "rijksmonument": False,
        "gem_omschrijving": None,
        "rk_omschrijving": None,
    }

    pc = postcode.replace(" ", "").upper()

    # Step 1: Geocode to RD coordinates via PDOK
    try:
        r = requests.get(
            PDOK_LOCATIE_URL,
            params={
                "q": f"{pc} {huisnummer}",
                "fq": "type:adres",
                "rows": 1,
                "fl": "centroide_rd",
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )
        r.raise_for_status()
        docs = r.json().get("response", {}).get("docs", [])
        if not docs:
            return result

        rd_point = docs[0].get("centroide_rd", "")
        m = _re.search(r"POINT\(([\d.]+)\s+([\d.]+)\)", rd_point)
        if not m:
            return result

        x, y = float(m.group(1)), float(m.group(2))
    except Exception:
        return result

    # Step 2: Query WKPB WFS with small bbox around the point
    buf = 5  # 5 meter buffer for bbox query
    bbox = f"{x - buf},{y - buf},{x + buf},{y + buf},EPSG:28992"

    try:
        from shapely.geometry import Point, shape

        address_point = Point(x, y)

        r = requests.get(
            WKPB_WFS_URL,
            params={
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeName": "wkpb:pb_multipolygon",
                "bbox": bbox,
                "outputFormat": "application/json",
            },
            headers={"Accept": "application/json"},
            timeout=15,
        )
        r.raise_for_status()
        features = r.json().get("features", [])

        for feat in features:
            # Verify the address point actually falls within the WKPB polygon,
            # not just within the bbox used for the query
            geom = feat.get("geometry")
            if geom:
                polygon = shape(geom)
                if not polygon.contains(address_point):
                    continue

            props = feat.get("properties", {})
            code = props.get("grondslagCode", "")
            omschr = props.get("grondslagOmschrijving", "")

            if code == "GWA":  # Gemeentewet: Aanwijzing gemeentelijk monument
                result["gemeentelijk_monument"] = True
                result["gem_omschrijving"] = omschr
            elif code == "EWE":  # Erfgoedwet: Rijksmonument
                result["rijksmonument"] = True
                result["rk_omschrijving"] = omschr

    except Exception:
        pass

    return result


def _lookup_monument(
    postcode: str,
    huisnummer: int,
    latitude: Optional[float],
    longitude: Optional[float],
    db: Session,
) -> MonumentResponse:
    """Look up monument status from all sources."""
    rijksmonument_info = None
    gemeentelijk_info = None
    beschermd_info = None
    unesco_info = None
    heeft_status = False

    # 1. Check WKPB (Kadaster publiekrechtelijke beperkingen) — authoritative
    #    Covers both gemeentelijke monumenten and rijksmonumenten per address
    wkpb = _lookup_wkpb(postcode, huisnummer)

    if wkpb["gemeentelijk_monument"]:
        heeft_status = True
        gemeentelijk_info = GemeentelijkMonumentInfo(
            is_monument=True,
            omschrijving=wkpb.get("gem_omschrijving"),
        )

    # 2. Check rijksmonument via RCE SPARQL API (richer data than WKPB)
    try:
        rce = create_rce_collector()
        rce_result = rce.get_monument_status(postcode, huisnummer)
        if rce_result.is_monument:
            heeft_status = True
            rijksmonument_info = RijksmonumentInfo(
                is_monument=True,
                monumentnummer=rce_result.monumentnummer,
                omschrijving=rce_result.omschrijving,
                categorie=rce_result.categorie,
                url=rce_result.url,
            )
    except Exception:
        pass

    # If WKPB says rijksmonument but RCE didn't find it (address mismatch),
    # still mark it as rijksmonument with basic info from WKPB
    if wkpb["rijksmonument"] and not rijksmonument_info:
        heeft_status = True
        rijksmonument_info = RijksmonumentInfo(
            is_monument=True,
            omschrijving=wkpb.get("rk_omschrijving"),
        )

    # 3. Fallback: check gemeentelijk monument in local DB (for addresses
    #    where WKPB might not have data yet)
    if not gemeentelijk_info:
        pc = postcode.replace(" ", "").upper()
        try:
            gem_mon = (
                db.query(GemeentelijkMonument)
                .filter(
                    GemeentelijkMonument.postcode == pc,
                    GemeentelijkMonument.huisnummer == huisnummer,
                )
                .first()
            )
            if gem_mon:
                heeft_status = True
                gemeentelijk_info = GemeentelijkMonumentInfo(
                    is_monument=True,
                    gemeente=gem_mon.gemeente,
                    omschrijving=gem_mon.omschrijving,
                )
        except Exception:
            pass

    # 4. Check beschermde gezichten + UNESCO via PDOK (requires coordinates)
    if latitude and longitude:
        try:
            pdok = create_pdok_beschermde_gebieden_collector()
            bg_result = pdok.get_beschermd_gebied(latitude, longitude)
            if bg_result.in_beschermd_gezicht:
                heeft_status = True
                beschermd_info = BeschermdGezichtInfo(
                    in_beschermd_gezicht=True,
                    naam=bg_result.gezicht_naam,
                    type=bg_result.gezicht_type,
                    niveau=bg_result.gezicht_niveau,
                )
            if bg_result.in_unesco:
                heeft_status = True
                unesco_info = UnescoInfo(
                    in_unesco=True,
                    naam=bg_result.unesco_naam,
                )
        except Exception:
            pass

    return MonumentResponse(
        rijksmonument=rijksmonument_info,
        gemeentelijk_monument=gemeentelijk_info,
        beschermd_gezicht=beschermd_info,
        unesco=unesco_info,
        heeft_monumentstatus=heeft_status,
    )


@router.get("/adres/{postcode}/{huisnummer}/monument", response_model=MonumentResponse)
def get_monument_status_by_address(
    postcode: str,
    huisnummer: int,
    db: Session = Depends(get_db),
):
    """Get monument status for an address."""
    # Geocode for lat/lon (needed for PDOK beschermde gebieden)
    lat, lon = None, None
    try:
        geo = geocode_address_pdok(postcode, huisnummer)
        if geo:
            lat = geo["lat"]
            lon = geo["lng"]
    except Exception:
        pass

    return _lookup_monument(postcode, huisnummer, lat, lon, db)


@router.get("/{woning_id}/monument", response_model=MonumentResponse)
def get_woning_monument_status(
    woning_id: int,
    db: Session = Depends(get_db),
):
    """Get monument status for a property in the database."""
    woning = db.query(Woning).filter(Woning.id == woning_id).first()
    if not woning:
        raise HTTPException(status_code=404, detail="Woning niet gevonden")

    if not woning.postcode:
        raise HTTPException(status_code=400, detail="Woning heeft geen postcode")

    huisnummer, _ = parse_huisnummer(woning.adres)
    if not huisnummer:
        raise HTTPException(
            status_code=400,
            detail="Kan huisnummer niet bepalen uit adres"
        )

    return _lookup_monument(
        woning.postcode, huisnummer,
        woning.latitude, woning.longitude,
        db,
    )


# ============================================================================
# Enhanced Valuation (with auto-fetch of WOZ and energielabel)
# ============================================================================

@router.post("/waardebepaling/adres", response_model=EnhancedWaardebepalingResponse)
def bereken_waarde_voor_adres(
    request: EnhancedWaardebepalingRequest,
    db: Session = Depends(get_db),
):
    """
    Enhanced property valuation with automatic data lookup.

    This endpoint:
    1. Fetches BAG data (surface area, build year)
    2. Fetches WOZ value from Waardeloket
    3. Fetches energy label from EP-Online
    4. Looks up comparable sales
    5. Calculates valuation with all available data
    """
    # Initialize variables
    woonoppervlakte = request.woonoppervlakte
    bouwjaar: Optional[int] = None
    bag_data = None

    # Helper to safely convert to int
    def safe_int(value) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    # First, try BAG for surface area and build year (most reliable)
    bag_api_key = os.environ.get("BAG_API_KEY")
    if bag_api_key:
        try:
            bag_client = BagClient(api_key=bag_api_key)
            bag_data = bag_client.enrich_address(
                postcode=request.postcode,
                huisnummer=request.huisnummer,
                huisletter=request.huisletter,
                toevoeging=request.toevoeging,
            )
            if bag_data:
                if not woonoppervlakte and bag_data.get("oppervlakte"):
                    woonoppervlakte = safe_int(bag_data["oppervlakte"])
                if bag_data.get("pand_bouwjaar"):
                    bouwjaar = safe_int(bag_data["pand_bouwjaar"])
        except Exception:
            pass

    # Fetch WOZ value (may fail silently)
    woz_result = None
    try:
        woz_collector = create_woz_collector()
        woz_result = woz_collector.get_woz_value(
            postcode=request.postcode,
            huisnummer=request.huisnummer,
            huisletter=request.huisletter,
            toevoeging=request.toevoeging,
        )
    except Exception:
        pass

    # Fetch energy label (may fail silently)
    energielabel_result = None
    try:
        energielabel_collector = create_energielabel_collector()
        energielabel_result = energielabel_collector.get_energielabel(
            postcode=request.postcode,
            huisnummer=request.huisnummer,
            huisletter=request.huisletter,
            toevoeging=request.toevoeging,
        )

        if energielabel_result:
            if not woonoppervlakte and energielabel_result.gebruiksoppervlakte:
                woonoppervlakte = safe_int(energielabel_result.gebruiksoppervlakte)
            if not bouwjaar and energielabel_result.bouwjaar:
                bouwjaar = safe_int(energielabel_result.bouwjaar)
    except Exception:
        pass

    # Check if we have surface area - required for valuation
    if not woonoppervlakte:
        raise HTTPException(
            status_code=400,
            detail="Woonoppervlakte kon niet automatisch worden opgehaald. Vul de woonoppervlakte in."
        )

    # Fetch comparables: combine database + live scraping
    comparables_result = None
    try:
        kadaster_collector = create_kadaster_collector()
        comparables_result = kadaster_collector.get_comparables(
            postcode=request.postcode,
            huisnummer=request.huisnummer,
            oppervlakte=woonoppervlakte,
        )
        db_transactions = _get_db_comparables(db, request.postcode, request.huisnummer)
        if db_transactions:
            seen = {(t.postcode, t.huisnummer, t.transactie_datum) for t in comparables_result.transactions}
            for t in db_transactions:
                key = (t.postcode, t.huisnummer, t.transactie_datum)
                if key not in seen:
                    seen.add(key)
                    comparables_result.transactions.append(t)
    except Exception:
        pass

    # Fetch Miljoenhuizen comparable sales
    miljoenhuizen_verkopen: List[MiljoenhuizenWoning] = []
    try:
        miljoenhuizen_collector = create_miljoenhuizen_collector()
        miljoenhuizen_verkopen = miljoenhuizen_collector.get_vergelijkbare_verkopen(
            postcode=request.postcode,
            huisnummer=request.huisnummer,
            woonoppervlakte=woonoppervlakte,
            max_results=10,
        )
    except Exception:
        pass

    # Fetch Funda listing for this address
    funda_listing_data: Optional[FundaListing] = None
    try:
        funda_collector = create_funda_collector()
        funda_result = funda_collector.search_by_address(
            postcode=request.postcode,
            huisnummer=request.huisnummer,
            huisletter=getattr(request, "huisletter", None),
        )
        if funda_result:
            funda_listing_data = FundaListing(
                url=funda_result.url,
                adres=funda_result.address,
                postcode=funda_result.postcode,
                plaats=funda_result.city,
                vraagprijs=funda_result.price,
                vraagprijs_suffix=funda_result.price_suffix,
                woonoppervlakte=funda_result.living_area,
                perceeloppervlakte=funda_result.plot_area,
                inhoud=funda_result.volume,
                prijs_per_m2=funda_result.price_per_m2,
                kamers=funda_result.rooms,
                slaapkamers=funda_result.bedrooms,
                badkamers=funda_result.bathrooms,
                bouwjaar=funda_result.year_built,
                woningtype=funda_result.building_type,
                bouwtype=funda_result.construction_type,
                energielabel=funda_result.energy_label,
                eigendom_type=funda_result.eigendom_type,
                vve_bijdrage=funda_result.vve_bijdrage,
                erfpacht_bedrag=funda_result.erfpacht_bedrag,
                tuin_type=funda_result.tuin_type,
                tuin_oppervlakte=funda_result.tuin_oppervlakte,
                tuin_orientatie=funda_result.tuin_orientatie,
                buitenruimte=funda_result.buitenruimte,
                balkon=funda_result.balkon,
                dakterras=funda_result.dakterras,
                verdiepingen=funda_result.verdiepingen,
                garage_type=funda_result.garage_type,
                parkeerplaatsen=funda_result.parkeerplaatsen,
                parkeer_type=funda_result.parkeer_type,
                kelder=funda_result.kelder,
                zolder=funda_result.zolder,
                berging=funda_result.berging,
                isolatie=funda_result.isolatie,
                verwarming=funda_result.verwarming,
                dak_type=funda_result.dak_type,
                aangeboden_sinds=funda_result.aangeboden_sinds,
                status=funda_result.status,
                verkoopdatum=funda_result.verkoopdatum,
                looptijd_dagen=funda_result.looptijd_dagen,
            )
    except Exception:
        pass

    energielabel = energielabel_result.energielabel if energielabel_result else None
    grondoppervlakte = woz_result.oppervlakte if woz_result else None

    # Fetch CBS market data for dynamic overbid percentage
    cbs_market_data = None
    market_overbid_pct = None
    try:
        cbs_collector = create_cbs_market_collector()
        gemeente_naam = None
        if bag_data and bag_data.get("woonplaats_naam"):
            gemeente_naam = bag_data.get("woonplaats_naam")
        elif woz_result and woz_result.woonplaats:
            gemeente_naam = woz_result.woonplaats

        if gemeente_naam:
            cbs_market_data = cbs_collector.get_market_data(gemeente_naam)
            if cbs_market_data.overbiedingspercentage is not None:
                market_overbid_pct = cbs_market_data.overbiedingspercentage / 100.0
    except Exception:
        pass

    # Fetch CBS buurt data for neighborhood-level indicators
    buurt_data = None
    buurt_code = None
    try:
        buurt_code = lookup_buurt_code_pdok(request.postcode, request.huisnummer)
        if buurt_code:
            buurt_collector = create_cbs_buurt_collector()
            buurt_data = buurt_collector.get_buurt(buurt_code)
    except Exception:
        pass

    # Fetch monument status
    monument_result = None
    try:
        # Get lat/lon for PDOK check - try geocoding first
        mon_lat, mon_lon = None, None
        try:
            geo_mon = geocode_address_pdok(request.postcode, request.huisnummer)
            if geo_mon:
                mon_lat = geo_mon["lat"]
                mon_lon = geo_mon["lng"]
        except Exception:
            pass

        monument_result = _lookup_monument(
            request.postcode, request.huisnummer,
            mon_lat, mon_lon, db,
        )
    except Exception:
        pass

    # Calculate valuation
    service = ValuationService(db)
    if market_overbid_pct is not None:
        service.set_market_overbid(market_overbid_pct)

    # Use Funda asking price if user didn't provide one
    vraagprijs = request.vraagprijs
    if not vraagprijs and funda_listing_data and funda_listing_data.vraagprijs:
        vraagprijs = funda_listing_data.vraagprijs

    valuation = service.estimate_value(
        woonoppervlakte=woonoppervlakte,
        energielabel=energielabel,
        bouwjaar=bouwjaar,
        woningtype=request.woningtype,
        vraagprijs=vraagprijs,
        grondoppervlakte=grondoppervlakte,
    )

    # Build address string
    adres = None
    woonplaats = None

    if bag_data:
        straat = bag_data.get("openbareruimte_naam")
        woonplaats = bag_data.get("woonplaats_naam")
        if straat:
            adres = f"{straat} {request.huisnummer}"
            if request.huisletter:
                adres += request.huisletter
            if request.toevoeging:
                adres += f"-{request.toevoeging}"
            if woonplaats:
                adres += f", {woonplaats}"

    if not adres:
        if woz_result and woz_result.adres:
            adres = woz_result.adres
            woonplaats = woz_result.woonplaats
        elif energielabel_result and energielabel_result.adres:
            adres = energielabel_result.adres
            woonplaats = energielabel_result.woonplaats

    if not adres:
        adres = f"{request.postcode} {request.huisnummer}"
        if request.huisletter:
            adres += request.huisletter
        if request.toevoeging:
            adres += f" {request.toevoeging}"

    # Build list of data sources used
    data_bronnen = []
    if bag_data and bag_data.get("nummeraanduiding_id"):
        data_bronnen.append("BAG (Kadaster)")
    if woz_result and woz_result.woz_waarde:
        data_bronnen.append("WOZ Waardeloket")
    if energielabel:
        data_bronnen.append("EP-Online (RVO)")
    if comparables_result and comparables_result.count > 0:
        data_bronnen.append("OpenKadaster")
    if cbs_market_data and (cbs_market_data.gemiddelde_prijs or cbs_market_data.overbiedingspercentage):
        data_bronnen.append("CBS StatLine")
    if buurt_data and buurt_data.gem_woz_waarde:
        if "CBS Kerncijfers" not in data_bronnen:
            data_bronnen.append("CBS Kerncijfers")
    if miljoenhuizen_verkopen:
        data_bronnen.append("Miljoenhuizen.nl")
    if funda_listing_data:
        data_bronnen.append("Funda")
    if monument_result and monument_result.heeft_monumentstatus:
        data_bronnen.append("RCE Monumentenregister")

    # --- Save/update woning in database ---
    saved_woning_id = None
    try:
        pc_normalized = request.postcode.replace(" ", "").upper()
        pc6 = pc_normalized[:6]

        # Geocode for lat/lon
        geo = geocode_address_pdok(pc_normalized, request.huisnummer)

        # Build short adres (without city) for dedup
        adres_short = adres.split(",")[0].strip() if adres and "," in adres else adres

        # Dedup: find existing woning by pc6 + adres
        existing = (
            db.query(Woning)
            .filter(Woning.pc6 == pc6)
            .all()
        )
        matched = None
        for w in existing:
            w_adres_short = w.adres.split(",")[0].strip() if w.adres and "," in w.adres else w.adres
            if w_adres_short == adres_short:
                matched = w
                break

        if matched:
            # Update existing woning
            matched.adres = adres or matched.adres
            matched.plaats = woonplaats or matched.plaats
            matched.huisnummer = request.huisnummer
            matched.huisletter = request.huisletter or matched.huisletter
            matched.toevoeging = request.toevoeging or matched.toevoeging
            matched.vraagprijs = request.vraagprijs or matched.vraagprijs
            matched.woonoppervlakte = woonoppervlakte or matched.woonoppervlakte
            matched.woningtype = request.woningtype or matched.woningtype
            matched.energielabel = energielabel or matched.energielabel
            matched.bouwjaar = bouwjaar or matched.bouwjaar
            matched.buurt_code = buurt_code or matched.buurt_code
            matched.geschatte_waarde_laag = valuation.waarde_laag
            matched.geschatte_waarde_hoog = valuation.waarde_hoog
            matched.waarde_confidence = valuation.confidence
            matched.biedadvies = valuation.bied_advies.value
            if geo:
                matched.latitude = geo["lat"]
                matched.longitude = geo["lng"]
            if bag_data:
                matched.bag_oppervlakte = safe_int(bag_data.get("oppervlakte"))
                matched.bag_bouwjaar = safe_int(bag_data.get("pand_bouwjaar"))
                matched.bag_gebruiksdoel = bag_data.get("gebruiksdoel")
            # Funda data: store in raw_data and track price changes
            if funda_listing_data:
                raw = matched.raw_data or {}
                if isinstance(raw, str):
                    import json as _json
                    raw = _json.loads(raw)
                old_funda_prijs = raw.get("funda_vraagprijs")
                new_funda_prijs = funda_listing_data.vraagprijs
                # Track price changes in prijshistorie
                if (
                    old_funda_prijs
                    and new_funda_prijs
                    and old_funda_prijs != new_funda_prijs
                ):
                    from models import Prijshistorie
                    ph_type = (
                        "verlaagd"
                        if new_funda_prijs < old_funda_prijs
                        else "verhoogd"
                    )
                    ph = Prijshistorie(
                        woning_id=matched.id,
                        prijs=old_funda_prijs,
                        type=ph_type,
                        datum=datetime.now(),
                    )
                    db.add(ph)
                raw["funda_url"] = funda_listing_data.url
                raw["funda_vraagprijs"] = new_funda_prijs
                raw["funda_data"] = funda_listing_data.model_dump()
                matched.raw_data = raw
                # Update vraagprijs from Funda if user didn't provide one
                if not request.vraagprijs and new_funda_prijs:
                    matched.vraagprijs = new_funda_prijs
            matched.updated_at = datetime.now()
            matched.enriched_at = datetime.now()
            db.commit()
            saved_woning_id = matched.id
        else:
            # Create new woning
            woning = Woning(
                adres=adres or f"{pc_normalized} {request.huisnummer}",
                postcode=request.postcode,
                pc6=pc6,
                plaats=woonplaats,
                huisnummer=request.huisnummer,
                huisletter=request.huisletter or None,
                toevoeging=request.toevoeging or None,
                buurt_code=buurt_code,
                vraagprijs=request.vraagprijs,
                woonoppervlakte=woonoppervlakte,
                woningtype=request.woningtype,
                bouwjaar=bouwjaar,
                energielabel=energielabel,
                status="active",
                datum_aangemeld=datetime.now(),
                latitude=geo["lat"] if geo else None,
                longitude=geo["lng"] if geo else None,
                bag_oppervlakte=safe_int(bag_data.get("oppervlakte")) if bag_data else None,
                bag_bouwjaar=safe_int(bag_data.get("pand_bouwjaar")) if bag_data else None,
                bag_gebruiksdoel=bag_data.get("gebruiksdoel") if bag_data else None,
                geschatte_waarde_laag=valuation.waarde_laag,
                geschatte_waarde_hoog=valuation.waarde_hoog,
                waarde_confidence=valuation.confidence,
                biedadvies=valuation.bied_advies.value,
                enriched_at=datetime.now(),
                raw_data=(
                    {
                        "funda_url": funda_listing_data.url,
                        "funda_vraagprijs": funda_listing_data.vraagprijs,
                        "funda_data": funda_listing_data.model_dump(),
                    }
                    if funda_listing_data
                    else None
                ),
            )
            db.add(woning)
            db.commit()
            db.refresh(woning)
            saved_woning_id = woning.id
    except Exception:
        db.rollback()

    return EnhancedWaardebepalingResponse(
        postcode=request.postcode,
        huisnummer=request.huisnummer,
        adres=adres,
        woz_waarde=woz_result.woz_waarde if woz_result else None,
        woz_peiljaar=woz_result.peiljaar if woz_result else None,
        grondoppervlakte=grondoppervlakte,
        woonoppervlakte=woonoppervlakte,
        bouwjaar=bouwjaar,
        woningtype=request.woningtype,
        energielabel=energielabel,
        energielabel_bron="auto" if energielabel else "niet_gevonden",
        waarde_laag=valuation.waarde_laag,
        waarde_hoog=valuation.waarde_hoog,
        waarde_midden=valuation.waarde_midden,
        vraagprijs=valuation.vraagprijs,
        verschil_percentage=valuation.verschil_percentage,
        bied_advies=valuation.bied_advies.value,
        bied_range_laag=valuation.bied_range_laag,
        bied_range_hoog=valuation.bied_range_hoog,
        basis_waarde=valuation.basis_waarde,
        energielabel_correctie=valuation.energielabel_correctie,
        bouwjaar_correctie=valuation.bouwjaar_correctie,
        woningtype_correctie=valuation.woningtype_correctie,
        perceel_correctie=valuation.perceel_correctie,
        buurt_kwaliteit_correctie=valuation.buurt_kwaliteit_correctie,
        markt_correctie=valuation.markt_correctie,
        confidence=valuation.confidence,
        confidence_factors=valuation.confidence_factors,
        comparables_count=comparables_result.count if comparables_result else 0,
        comparables_avg_m2=comparables_result.avg_prijs_per_m2 if comparables_result else None,
        miljoenhuizen_verkopen=[
            MiljoenhuizenVerkoop(
                url=v.url,
                adres=v.adres,
                postcode=v.postcode,
                plaats=v.plaats,
                laatste_vraagprijs=v.laatste_vraagprijs,
                verkoopdatum=v.verkoopdatum,
                woonoppervlakte=v.woonoppervlakte,
                prijs_per_m2=v.prijs_per_m2,
                bouwjaar=v.bouwjaar,
                woningtype=v.woningtype,
                geschatte_waarde_laag=v.geschatte_waarde_laag,
                geschatte_waarde_hoog=v.geschatte_waarde_hoog,
            )
            for v in miljoenhuizen_verkopen
        ],
        miljoenhuizen_count=len(miljoenhuizen_verkopen),
        miljoenhuizen_avg_vraagprijs=(
            int(sum(v.laatste_vraagprijs for v in miljoenhuizen_verkopen if v.laatste_vraagprijs) /
                len([v for v in miljoenhuizen_verkopen if v.laatste_vraagprijs]))
            if miljoenhuizen_verkopen and any(v.laatste_vraagprijs for v in miljoenhuizen_verkopen)
            else None
        ),
        markt_gem_prijs=cbs_market_data.gemiddelde_prijs if cbs_market_data else None,
        markt_overbiedpct=cbs_market_data.overbiedingspercentage if cbs_market_data else None,
        markt_verkooptijd=cbs_market_data.gemiddelde_verkooptijd if cbs_market_data else None,
        markt_peildatum=cbs_market_data.peildatum if cbs_market_data else None,
        buurt_code=buurt_code,
        buurt_naam=buurt_data.buurt_naam if buurt_data else None,
        buurt_gem_woz=buurt_data.gem_woz_waarde if buurt_data else None,
        buurt_koopwoningen_pct=buurt_data.koopwoningen_pct if buurt_data else None,
        buurt_gem_inkomen=buurt_data.gem_inkomen if buurt_data else None,
        data_bronnen=data_bronnen,
        monument=monument_result,
        funda_listing=funda_listing_data,
        woning_id=saved_woning_id,
    )
