"""Property valuation (waardebepaling) API routes."""

import asyncio
import os
import re
import time
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
from collectors.cbs_buurt_collector import create_cbs_buurt_collector
from collectors.bag_collector import BagClient
from collectors.rce_collector import create_rce_collector
from collectors.pdok_beschermde_gebieden_collector import create_pdok_beschermde_gebieden_collector
from collectors.funda_collector import create_funda_collector, PropertyListing as FundaPropertyListing
from collectors.driedbag_collector import create_driedbag_collector
from collectors.glasvezel_collector import create_glasvezel_collector
from services.plafondhoogte import bereken_plafondhoogte, PlafondhoogteResult
from datetime import datetime, timedelta
from sqlalchemy import and_
from utils.address import parse_huisnummer
from utils.pdok import geocode_pdok_full, PDOKResult
from utils.timing import TimingTracker

router = APIRouter(prefix="/api/woningen", tags=["waardebepaling"])

# ============================================================================
# Module-level collector singletons — ingeladen éénmalig, persistent over requests
# Voorkomt 36MB JSON-parse (CBS buurt) en CBS API-download per request
# ============================================================================

_cbs_market_collector = None
_cbs_buurt_collector = None


def _get_cbs_market():
    global _cbs_market_collector
    if _cbs_market_collector is None:
        _cbs_market_collector = create_cbs_market_collector()
    return _cbs_market_collector


def _get_cbs_buurt():
    global _cbs_buurt_collector
    if _cbs_buurt_collector is None:
        _cbs_buurt_collector = create_cbs_buurt_collector()
    return _cbs_buurt_collector


# Monument-cache: alle drie WFS/API-calls samen gecached per adres (30 dagen TTL)
# Monument-status verandert zelden; caching bespaart ~240–500ms per request.
_monument_cache: Dict[str, Any] = {}
_MONUMENT_CACHE_TTL = timedelta(days=30)


def _monument_cache_key(postcode: str, huisnummer: int) -> str:
    return f"{postcode.replace(' ', '').upper()}_{huisnummer}"


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


class PlafondhoogteResponse(BaseModel):
    """Geschatte plafondhoogte indicatie."""
    geschatte_verdiepingshoogte: Optional[float] = None
    label: Optional[str] = None
    methode: Optional[str] = None
    betrouwbaarheid: Optional[str] = None
    details: Optional[str] = None


class GlasvezelResponse(BaseModel):
    """Internet beschikbaarheid per adres."""
    glasvezel_beschikbaar: Optional[bool] = None
    glasvezel_snelheid: Optional[int] = None  # Mbit/s
    glasvezel_provider: Optional[str] = None
    kabel_beschikbaar: Optional[bool] = None
    kabel_snelheid: Optional[int] = None  # Mbit/s
    kabel_provider: Optional[str] = None
    dsl_snelheid: Optional[int] = None  # Mbit/s
    max_snelheid: Optional[int] = None  # Mbit/s
    adres_gevonden: bool = False


class OrientatieResponse(BaseModel):
    """Zon en oriëntatie analyse."""
    tuin_orientatie: Optional[str] = None
    tuin_azimut: Optional[float] = None
    tuin_oppervlakte_berekend: Optional[float] = None
    zon_uren_zomer: Optional[float] = None
    zon_uren_lente: Optional[float] = None
    zon_uren_winter: Optional[float] = None
    zon_label: Optional[str] = None
    schaduw_eigen_gebouw: Optional[str] = None
    schaduw_buren: Optional[str] = None
    schaduw_bomen: Optional[str] = None
    effectieve_tuin_diepte: Optional[float] = None
    zonnepanelen_score: Optional[int] = None
    zonnepanelen_label: Optional[str] = None
    dak_orientatie: Optional[str] = None
    dak_hellingshoek: Optional[float] = None
    geschikt_dakoppervlak: Optional[float] = None
    funda_tuin_orientatie: Optional[str] = None
    funda_tuin_oppervlakte: Optional[int] = None
    tuin_oppervlakte_bron: Optional[str] = None
    methode: Optional[str] = None
    betrouwbaarheid: Optional[str] = None
    details: Optional[str] = None


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

    # Coordinaten (voor frontend componenten)
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Plafondhoogte inschatting
    plafondhoogte: Optional[PlafondhoogteResponse] = None

    # Glasvezel beschikbaarheid
    glasvezel: Optional[GlasvezelResponse] = None

    # Zon en oriëntatie
    orientatie: Optional[OrientatieResponse] = None

    # Saved woning reference
    woning_id: Optional[int] = None

    # Performance debug (only populated when ?debug=true)
    timing_breakdown: Optional[dict] = None


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


def _lookup_pand_id_pdok(
    postcode: str, huisnummer: int
) -> tuple[Optional[str], Optional[float], Optional[float]]:
    """Lookup pand identificatie and RD coordinates via free PDOK.

    Returns (pand_id, rd_x, rd_y) where rd_x/rd_y are the address
    coordinates from the PDOK locatieserver.
    """
    rd_x, rd_y = None, None
    try:
        # Step 1: Get adresseerbaarobject_id + centroid from PDOK Locatieserver
        resp = requests.get(
            "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free",
            params={
                "q": f"{postcode} {huisnummer}",
                "fq": "type:adres",
                "fl": "adresseerbaarobject_id,centroide_rd",
                "rows": 1,
            },
            timeout=10,
        )
        resp.raise_for_status()
        docs = resp.json().get("response", {}).get("docs", [])
        if not docs:
            return None, None, None

        # Parse RD coordinates from "POINT(x y)" format
        centroid_str = docs[0].get("centroide_rd", "")
        if centroid_str:
            import re as _re
            m = _re.match(r"POINT\(([\d.]+)\s+([\d.]+)\)", centroid_str)
            if m:
                rd_x = float(m.group(1))
                rd_y = float(m.group(2))

        vbo_id = docs[0].get("adresseerbaarobject_id")
        if not vbo_id:
            return None, rd_x, rd_y

        # Step 2: Get pandidentificatie from PDOK BAG WFS
        resp2 = requests.get(
            "https://service.pdok.nl/lv/bag/wfs/v2_0",
            params={
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeName": "bag:verblijfsobject",
                "outputFormat": "application/json",
                "CQL_FILTER": f"identificatie='{vbo_id}'",
                "count": 1,
            },
            timeout=10,
        )
        resp2.raise_for_status()
        features = resp2.json().get("features", [])
        if features:
            pand_id = features[0].get("properties", {}).get("pandidentificatie")
            if pand_id:
                return str(pand_id), rd_x, rd_y
    except Exception:
        pass
    return None, rd_x, rd_y


def _lookup_wkpb(
    postcode: str,
    huisnummer: int,
    rd_x: Optional[float] = None,
    rd_y: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Query Kadaster WKPB (publiekrechtelijke beperkingen) for monument status.

    Returns dict with keys: gemeentelijk_monument (bool), rijksmonument (bool),
    gem_omschrijving, rk_omschrijving.
    Pass rd_x/rd_y to skip the internal PDOK geocode call.
    """
    import re as _re

    result = {
        "gemeentelijk_monument": False,
        "rijksmonument": False,
        "gem_omschrijving": None,
        "rk_omschrijving": None,
    }

    # Step 1: Get RD coordinates — use provided coords or geocode via PDOK
    if rd_x is not None and rd_y is not None:
        x, y = rd_x, rd_y
    else:
        pc = postcode.replace(" ", "").upper()
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
    db: Optional[Session] = None,
    rd_x: Optional[float] = None,
    rd_y: Optional[float] = None,
    gem_monument_cached=None,
) -> MonumentResponse:
    """Look up monument status from all sources."""
    cache_key = _monument_cache_key(postcode, huisnummer)
    cached = _monument_cache.get(cache_key)
    if cached and datetime.now() - cached["ts"] < _MONUMENT_CACHE_TTL:
        return cached["result"]

    rijksmonument_info = None
    gemeentelijk_info = None
    beschermd_info = None
    unesco_info = None
    heeft_status = False

    # 1. Check WKPB (Kadaster publiekrechtelijke beperkingen) — authoritative
    #    Covers both gemeentelijke monumenten and rijksmonumenten per address
    wkpb = _lookup_wkpb(postcode, huisnummer, rd_x=rd_x, rd_y=rd_y)

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
            # Use pre-fetched result when available (avoids DB access from threads)
            if gem_monument_cached is not None:
                gem_mon = gem_monument_cached
            elif db is not None:
                gem_mon = (
                    db.query(GemeentelijkMonument)
                    .filter(
                        GemeentelijkMonument.postcode == pc,
                        GemeentelijkMonument.huisnummer == huisnummer,
                    )
                    .first()
                )
            else:
                gem_mon = None
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

    monument_result = MonumentResponse(
        rijksmonument=rijksmonument_info,
        gemeentelijk_monument=gemeentelijk_info,
        beschermd_gezicht=beschermd_info,
        unesco=unesco_info,
        heeft_monumentstatus=heeft_status,
    )
    _monument_cache[cache_key] = {"result": monument_result, "ts": datetime.now()}
    return monument_result


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
async def bereken_waarde_voor_adres(
    request: EnhancedWaardebepalingRequest,
    debug: bool = Query(False),
    db: Session = Depends(get_db),
):
    """Enhanced property valuation with automatic parallel data lookup."""
    timer = TimingTracker()

    def safe(result):
        return None if isinstance(result, Exception) else result

    def safe_int(value) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    async def _timed(name: str, func, *args, **kwargs):
        t = time.perf_counter()
        try:
            result = await asyncio.to_thread(func, *args, **kwargs)
            timer.record(name, t)
            return result
        except Exception:
            timer.record(name, t)
            return None

    async def _noop():
        return None

    bag_api_key = os.environ.get("BAG_API_KEY")
    bag_client = BagClient(api_key=bag_api_key) if bag_api_key else None

    # ─── Wave 1: volledig parallel — alleen postcode+huisnummer nodig ───
    t_w1 = time.perf_counter()
    (
        geo_result,
        bag_data,
        woz_result,
        energielabel_result,
        funda_raw,
        glasvezel_raw,
    ) = await asyncio.gather(
        _timed("geocode", geocode_pdok_full, request.postcode, request.huisnummer),
        _timed(
            "bag",
            bag_client.enrich_address,
            postcode=request.postcode,
            huisnummer=request.huisnummer,
            huisletter=request.huisletter,
            toevoeging=request.toevoeging,
        ) if bag_client else _noop(),
        _timed(
            "woz",
            create_woz_collector().get_woz_value,
            postcode=request.postcode,
            huisnummer=request.huisnummer,
            huisletter=request.huisletter,
            toevoeging=request.toevoeging,
        ),
        _timed(
            "energielabel",
            create_energielabel_collector().get_energielabel,
            postcode=request.postcode,
            huisnummer=request.huisnummer,
            huisletter=request.huisletter,
            toevoeging=request.toevoeging,
        ),
        _timed(
            "funda",
            create_funda_collector().search_by_address,
            postcode=request.postcode,
            huisnummer=request.huisnummer,
            huisletter=getattr(request, "huisletter", None),
        ),
        _timed(
            "glasvezel",
            create_glasvezel_collector().get_beschikbaarheid,
            request.postcode,
            request.huisnummer,
        ),
    )
    timer.record("wave1_total", t_w1)

    # ─── Na Wave 1: afleiden van basisgegevens ───
    woonoppervlakte = request.woonoppervlakte
    bouwjaar: Optional[int] = None

    if bag_data:
        if not woonoppervlakte and bag_data.get("oppervlakte"):
            woonoppervlakte = safe_int(bag_data["oppervlakte"])
        if bag_data.get("pand_bouwjaar"):
            bouwjaar = safe_int(bag_data["pand_bouwjaar"])

    if energielabel_result:
        if not woonoppervlakte and energielabel_result.gebruiksoppervlakte:
            woonoppervlakte = safe_int(energielabel_result.gebruiksoppervlakte)
        if not bouwjaar and energielabel_result.bouwjaar:
            bouwjaar = safe_int(energielabel_result.bouwjaar)

    if not woonoppervlakte:
        raise HTTPException(
            status_code=400,
            detail="Woonoppervlakte kon niet automatisch worden opgehaald. Vul de woonoppervlakte in.",
        )

    energielabel = energielabel_result.energielabel if energielabel_result else None
    grondoppervlakte = woz_result.oppervlakte if woz_result else None

    # Funda-listing transformatie
    funda_listing_data: Optional[FundaListing] = None
    if funda_raw:
        try:
            funda_listing_data = FundaListing(
                url=funda_raw.url,
                adres=funda_raw.address,
                postcode=funda_raw.postcode,
                plaats=funda_raw.city,
                vraagprijs=funda_raw.price,
                vraagprijs_suffix=funda_raw.price_suffix,
                woonoppervlakte=funda_raw.living_area,
                perceeloppervlakte=funda_raw.plot_area,
                inhoud=funda_raw.volume,
                prijs_per_m2=funda_raw.price_per_m2,
                kamers=funda_raw.rooms,
                slaapkamers=funda_raw.bedrooms,
                badkamers=funda_raw.bathrooms,
                bouwjaar=funda_raw.year_built,
                woningtype=funda_raw.building_type,
                bouwtype=funda_raw.construction_type,
                energielabel=funda_raw.energy_label,
                eigendom_type=funda_raw.eigendom_type,
                vve_bijdrage=funda_raw.vve_bijdrage,
                erfpacht_bedrag=funda_raw.erfpacht_bedrag,
                tuin_type=funda_raw.tuin_type,
                tuin_oppervlakte=funda_raw.tuin_oppervlakte,
                tuin_orientatie=funda_raw.tuin_orientatie,
                buitenruimte=funda_raw.buitenruimte,
                balkon=funda_raw.balkon,
                dakterras=funda_raw.dakterras,
                verdiepingen=funda_raw.verdiepingen,
                garage_type=funda_raw.garage_type,
                parkeerplaatsen=funda_raw.parkeerplaatsen,
                parkeer_type=funda_raw.parkeer_type,
                kelder=funda_raw.kelder,
                zolder=funda_raw.zolder,
                berging=funda_raw.berging,
                isolatie=funda_raw.isolatie,
                verwarming=funda_raw.verwarming,
                dak_type=funda_raw.dak_type,
                aangeboden_sinds=funda_raw.aangeboden_sinds,
                status=funda_raw.status,
                verkoopdatum=funda_raw.verkoopdatum,
                looptijd_dagen=funda_raw.looptijd_dagen,
            )
        except Exception:
            pass

    # Glasvezel-response transformatie
    glasvezel_response = None
    if glasvezel_raw and not glasvezel_raw.error:
        try:
            glasvezel_response = GlasvezelResponse(
                glasvezel_beschikbaar=glasvezel_raw.glasvezel_beschikbaar,
                glasvezel_snelheid=glasvezel_raw.glasvezel_snelheid,
                glasvezel_provider=glasvezel_raw.glasvezel_provider,
                kabel_beschikbaar=glasvezel_raw.kabel_beschikbaar,
                kabel_snelheid=glasvezel_raw.kabel_snelheid,
                kabel_provider=glasvezel_raw.kabel_provider,
                dsl_snelheid=glasvezel_raw.dsl_snelheid,
                max_snelheid=glasvezel_raw.max_snelheid,
                adres_gevonden=glasvezel_raw.adres_gevonden,
            )
        except Exception:
            pass

    # Coördinaten en pand-ID afleiden uit Wave 1
    adres_rd_x = geo_result.rd_x if geo_result else None
    adres_rd_y = geo_result.rd_y if geo_result else None
    buurt_code = geo_result.buurt_code if geo_result else None

    pand_identificatie: Optional[str] = None
    if bag_data and bag_data.get("pand_identificaties"):
        pand_identificatie = str(bag_data["pand_identificaties"][0])

    # Fallback pand-ID via BAG WFS als BAG API niet beschikbaar was
    if not pand_identificatie and geo_result and geo_result.adresseerbaarobject_id:
        vbo_id = geo_result.adresseerbaarobject_id
        t_wfs = time.perf_counter()
        try:
            def _bag_wfs_lookup():
                r = requests.get(
                    "https://service.pdok.nl/lv/bag/wfs/v2_0",
                    params={
                        "service": "WFS",
                        "version": "2.0.0",
                        "request": "GetFeature",
                        "typeName": "bag:verblijfsobject",
                        "outputFormat": "application/json",
                        "CQL_FILTER": f"identificatie='{vbo_id}'",
                        "count": 1,
                    },
                    timeout=10,
                )
                r.raise_for_status()
                features = r.json().get("features", [])
                if features:
                    pid = features[0].get("properties", {}).get("pandidentificatie")
                    return str(pid) if pid else None
                return None
            pand_identificatie = await asyncio.to_thread(_bag_wfs_lookup)
        except Exception:
            pass
        timer.record("bag_wfs", t_wfs)

    gemeente_naam = None
    if bag_data and bag_data.get("woonplaats_naam"):
        gemeente_naam = bag_data.get("woonplaats_naam")
    elif woz_result and woz_result.woonplaats:
        gemeente_naam = woz_result.woonplaats

    # DB-queries voor Wave 2 (niet in threads — synchrone sessie)
    pc_norm = request.postcode.replace(" ", "").upper()
    db_comparables = _get_db_comparables(db, request.postcode, request.huisnummer)
    gem_monument_db = None
    try:
        gem_monument_db = (
            db.query(GemeentelijkMonument)
            .filter(
                GemeentelijkMonument.postcode == pc_norm,
                GemeentelijkMonument.huisnummer == request.huisnummer,
            )
            .first()
        )
    except Exception:
        pass

    # ─── Wave 2: parallel — gebruikt resultaten van Wave 1 ───
    t_w2 = time.perf_counter()
    driedbag_inst = create_driedbag_collector()

    (
        comparables_result,
        miljoenhuizen_verkopen_raw,
        cbs_market_data,
        buurt_data,
        monument_result,
        driedbag_result,
    ) = await asyncio.gather(
        _timed(
            "kadaster",
            create_kadaster_collector().get_comparables,
            postcode=request.postcode,
            huisnummer=request.huisnummer,
            oppervlakte=woonoppervlakte,
        ),
        _timed(
            "miljoenhuizen",
            create_miljoenhuizen_collector().get_vergelijkbare_verkopen,
            postcode=request.postcode,
            huisnummer=request.huisnummer,
            woonoppervlakte=woonoppervlakte,
            max_results=10,
        ),
        _timed(
            "cbs_market",
            _get_cbs_market().get_market_data,
            gemeente_naam,
        ) if gemeente_naam else _noop(),
        _timed(
            "cbs_buurt",
            _get_cbs_buurt().get_buurt,
            buurt_code,
        ) if buurt_code else _noop(),
        _timed(
            "monument",
            _lookup_monument,
            request.postcode,
            request.huisnummer,
            geo_result.lat if geo_result else None,
            geo_result.lng if geo_result else None,
            None,  # db niet gebruikt want gem_monument_cached is meegegeven
            adres_rd_x,
            adres_rd_y,
            gem_monument_db,
        ),
        _timed(
            "driedbag",
            driedbag_inst.get_building_data,
            pand_identificatie,
        ) if pand_identificatie else _noop(),
    )
    timer.record("wave2_total", t_w2)

    # Comparables samenvoegen met DB-data
    miljoenhuizen_verkopen: List[MiljoenhuizenWoning] = miljoenhuizen_verkopen_raw or []
    if comparables_result and db_comparables:
        seen = {(t.postcode, t.huisnummer, t.transactie_datum) for t in comparables_result.transactions}
        for t in db_comparables:
            key = (t.postcode, t.huisnummer, t.transactie_datum)
            if key not in seen:
                seen.add(key)
                comparables_result.transactions.append(t)

    # 3DBAG footprint validatie en fallback spatial lookup
    if driedbag_result and driedbag_result.footprint_rd and adres_rd_x and adres_rd_y:
        fp = driedbag_result.footprint_rd
        fp_cx = sum(p[0] for p in fp) / len(fp)
        fp_cy = sum(p[1] for p in fp) / len(fp)
        dist = ((fp_cx - adres_rd_x) ** 2 + (fp_cy - adres_rd_y) ** 2) ** 0.5
        if dist > 100:
            driedbag_result = None

    if (not driedbag_result or not driedbag_result.footprint_rd) and adres_rd_x and adres_rd_y:
        t_fallback = time.perf_counter()
        try:
            loc_result = await asyncio.to_thread(
                driedbag_inst.get_building_by_location, adres_rd_x, adres_rd_y
            )
            if loc_result and loc_result.footprint_rd:
                driedbag_result = loc_result
                pand_identificatie = loc_result.pand_identificatie
        except Exception:
            pass
        timer.record("driedbag_fallback", t_fallback)

    # ─── Wave 3: parallel — gebruikt 3DBAG footprint ───
    footprint_rd = driedbag_result.footprint_rd if driedbag_result else None
    orientatie_rd_x, orientatie_rd_y = adres_rd_x, adres_rd_y
    if not orientatie_rd_x and footprint_rd:
        orientatie_rd_x = sum(p[0] for p in footprint_rd) / len(footprint_rd)
        orientatie_rd_y = sum(p[1] for p in footprint_rd) / len(footprint_rd)

    perceel_polygon = None
    buurtgebouwen = []
    bomen = []
    road_polygons = []

    if orientatie_rd_x and orientatie_rd_y:
        from collectors.perceelgrens_collector import create_perceelgrens_collector
        from collectors.bgt_boom_collector import create_bgt_boom_collector
        from collectors.bgt_wegdeel_collector import create_bgt_wegdeel_collector

        t_w3 = time.perf_counter()

        def _get_perceel():
            col = create_perceelgrens_collector()
            result = col.get_perceel(
                orientatie_rd_x, orientatie_rd_y,
                building_footprint_rd=footprint_rd,
            )
            return result.perceel_polygon_rd if result else None

        def _get_roads():
            return create_bgt_wegdeel_collector().get_roads(
                orientatie_rd_x, orientatie_rd_y, radius=50
            )

        def _get_trees():
            return create_bgt_boom_collector().get_trees(
                orientatie_rd_x, orientatie_rd_y, radius=75
            )

        def _get_buurtgebouwen():
            pand_id = driedbag_result.pand_identificatie if driedbag_result else None
            return driedbag_inst.get_surrounding_buildings(
                orientatie_rd_x, orientatie_rd_y,
                radius=75,
                exclude_pand_id=pand_id,
            )

        (
            perceel_polygon,
            road_polygons_raw,
            bomen_raw,
            buurtgebouwen_raw,
        ) = await asyncio.gather(
            _timed("perceel", _get_perceel),
            _timed("bgt_roads", _get_roads),
            _timed("bgt_bomen", _get_trees),
            _timed("buurtgebouwen", _get_buurtgebouwen),
        )
        timer.record("wave3_total", t_w3)

        road_polygons = road_polygons_raw or []
        bomen = bomen_raw or []
        buurtgebouwen = buurtgebouwen_raw or []

    # Oriëntatie berekening
    orientatie_response = None
    if orientatie_rd_x and orientatie_rd_y:
        try:
            from services.orientatie import bereken_orientatie
            funda_tuin_orientatie = funda_listing_data.tuin_orientatie if funda_listing_data else None
            funda_tuin_oppervlakte = funda_listing_data.tuin_oppervlakte if funda_listing_data else None

            orientatie_data = bereken_orientatie(
                rd_x=orientatie_rd_x,
                rd_y=orientatie_rd_y,
                building_footprint_rd=footprint_rd,
                perceel_polygon_rd=perceel_polygon,
                gebouwhoogte=driedbag_result.gebouwhoogte if driedbag_result else None,
                dak_azimut=driedbag_result.dak_azimut if driedbag_result else None,
                dak_hellingshoek=driedbag_result.dak_hellingshoek if driedbag_result else None,
                opp_dak_schuin=driedbag_result.opp_dak_schuin if driedbag_result else None,
                opp_dak_plat=driedbag_result.opp_dak_plat if driedbag_result else None,
                dak_type=driedbag_result.dak_type if driedbag_result else None,
                dak_delen=driedbag_result.dak_delen if driedbag_result else None,
                buurtgebouwen=buurtgebouwen,
                bomen=bomen,
                road_polygons=road_polygons,
                funda_tuin_orientatie=funda_tuin_orientatie,
                funda_tuin_oppervlakte=funda_tuin_oppervlakte,
            )
            if orientatie_data.tuin_orientatie or orientatie_data.zonnepanelen_score:
                orientatie_response = OrientatieResponse(**orientatie_data.to_dict())
        except Exception:
            pass

    # Plafondhoogte berekening
    plafondhoogte_response = None
    plafondhoogte_data = bereken_plafondhoogte(
        h_dak_max=driedbag_result.h_dak_max if driedbag_result else None,
        h_dak_min=driedbag_result.h_dak_min if driedbag_result else None,
        h_dak_50p=driedbag_result.h_dak_50p if driedbag_result else None,
        h_maaiveld=driedbag_result.h_maaiveld if driedbag_result else None,
        dak_type_3dbag=driedbag_result.dak_type if driedbag_result else None,
        bouwlagen_3dbag=driedbag_result.bouwlagen if driedbag_result else None,
        opp_dak_schuin=driedbag_result.opp_dak_schuin if driedbag_result else None,
        opp_dak_plat=driedbag_result.opp_dak_plat if driedbag_result else None,
        aantal_bouwlagen=bag_data.get("aantal_bouwlagen") if bag_data else None,
        inhoud=funda_listing_data.inhoud if funda_listing_data else None,
        woonoppervlakte=woonoppervlakte,
        verdiepingen=funda_listing_data.verdiepingen if funda_listing_data else None,
        dak_type_funda=funda_listing_data.dak_type if funda_listing_data else None,
    )
    if plafondhoogte_data.geschatte_verdiepingshoogte is not None:
        plafondhoogte_response = PlafondhoogteResponse(
            geschatte_verdiepingshoogte=plafondhoogte_data.geschatte_verdiepingshoogte,
            label=plafondhoogte_data.label,
            methode=plafondhoogte_data.methode,
            betrouwbaarheid=plafondhoogte_data.betrouwbaarheid,
            details=plafondhoogte_data.details,
        )

    # CBS market → overbod percentage voor waardebepaling
    market_overbid_pct = None
    if cbs_market_data and cbs_market_data.overbiedingspercentage is not None:
        market_overbid_pct = cbs_market_data.overbiedingspercentage / 100.0

    # Waardebepaling berekening
    service = ValuationService(db)
    if market_overbid_pct is not None:
        service.set_market_overbid(market_overbid_pct)

    vraagprijs = request.vraagprijs
    if not vraagprijs and funda_listing_data and funda_listing_data.vraagprijs:
        vraagprijs = funda_listing_data.vraagprijs

    valuation = service.estimate_value(
        woonoppervlakte=woonoppervlakte,
        buurt_code=buurt_code,
        energielabel=energielabel,
        bouwjaar=bouwjaar,
        woningtype=request.woningtype,
        vraagprijs=vraagprijs,
        grondoppervlakte=grondoppervlakte,
    )

    # Adresstring opbouwen
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

    # Databronnen lijst
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
    if driedbag_result and not driedbag_result.error:
        data_bronnen.append("3DBAG")
    if glasvezel_response:
        data_bronnen.append("Glasvezelcheck.nl")

    # ─── DB opslaan/updaten (synchrone sessie, niet in thread) ───
    saved_woning_id = None
    pc6 = pc_norm[:6]
    geo_lat = geo_result.lat if geo_result else None
    geo_lng = geo_result.lng if geo_result else None

    try:
        adres_short = adres.split(",")[0].strip() if adres and "," in adres else adres
        existing = db.query(Woning).filter(Woning.pc6 == pc6).all()
        matched = None
        for w in existing:
            w_adres_short = w.adres.split(",")[0].strip() if w.adres and "," in w.adres else w.adres
            if w_adres_short == adres_short:
                matched = w
                break

        if matched:
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
            if geo_lat:
                matched.latitude = geo_lat
                matched.longitude = geo_lng
            if bag_data:
                matched.bag_oppervlakte = safe_int(bag_data.get("oppervlakte"))
                matched.bag_bouwjaar = safe_int(bag_data.get("pand_bouwjaar"))
                matched.bag_gebruiksdoel = bag_data.get("gebruiksdoel")
            if funda_listing_data:
                raw = matched.raw_data or {}
                if isinstance(raw, str):
                    import json as _json
                    raw = _json.loads(raw)
                old_funda_prijs = raw.get("funda_vraagprijs")
                new_funda_prijs = funda_listing_data.vraagprijs
                if old_funda_prijs and new_funda_prijs and old_funda_prijs != new_funda_prijs:
                    from models import Prijshistorie
                    ph_type = "verlaagd" if new_funda_prijs < old_funda_prijs else "verhoogd"
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
                if not request.vraagprijs and new_funda_prijs:
                    matched.vraagprijs = new_funda_prijs
            matched.updated_at = datetime.now()
            matched.enriched_at = datetime.now()
            db.commit()
            saved_woning_id = matched.id
        else:
            woning = Woning(
                adres=adres or f"{pc_norm} {request.huisnummer}",
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
                latitude=geo_lat,
                longitude=geo_lng,
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
            int(
                sum(v.laatste_vraagprijs for v in miljoenhuizen_verkopen if v.laatste_vraagprijs)
                / len([v for v in miljoenhuizen_verkopen if v.laatste_vraagprijs])
            )
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
        latitude=geo_lat,
        longitude=geo_lng,
        plafondhoogte=plafondhoogte_response,
        glasvezel=glasvezel_response,
        orientatie=orientatie_response,
        woning_id=saved_woning_id,
        timing_breakdown=timer.to_dict() if debug else None,
    )
