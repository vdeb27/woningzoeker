"""Milieu & gezondheid API routes."""

import asyncio
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from collectors.luchtmeetnet_collector import create_luchtmeetnet_collector
from collectors.rivm_pfas_collector import create_rivm_pfas_collector
from collectors.pfas_bodemkaart_collector import create_pfas_bodemkaart_collector

router = APIRouter(prefix="/api/milieu", tags=["milieu"])


class LuchtmeetnetResponse(BaseModel):
    station_number: str
    station_naam: str
    station_type: str
    distance_km: float
    within_max_distance: bool
    no2_avg: Optional[float] = None
    pm10_avg: Optional[float] = None
    pm25_avg: Optional[float] = None
    o3_avg: Optional[float] = None
    jaar: Optional[int] = None


class BodemkaartResponse(BaseModel):
    in_den_haag: bool = False
    functie: Optional[str] = None
    zone_naam: Optional[str] = None
    kwaliteit_bovengrond: Optional[str] = None
    kwaliteit_ondergrond: Optional[str] = None
    kwaliteit_ranking: int = 0


class PFASResponse(BaseModel):
    # RIVM landelijke monsters
    samples_within_radius: int = 0
    max_pfoa: Optional[float] = None
    max_pfos: Optional[float] = None
    has_contamination: bool = False
    nearest_sample_distance_km: Optional[float] = None
    search_radius_km: float = 1.0
    # Gemeentelijke bodemkaart
    bodemkaart: Optional[BodemkaartResponse] = None


class StationInfo(BaseModel):
    station_number: str
    naam: str
    type: str
    lat: float
    lng: float
    formulas: List[str]
    no2_avg: Optional[float] = None
    pm10_avg: Optional[float] = None
    pm25_avg: Optional[float] = None
    o3_avg: Optional[float] = None
    jaar: Optional[int] = None


@router.get("/luchtmeetnet", response_model=LuchtmeetnetResponse)
def get_luchtmeetnet(
    lat: float = Query(..., description="Breedtegraad (WGS84)"),
    lng: float = Query(..., description="Lengtegraad (WGS84)"),
    max_distance_km: float = Query(4.0, description="Maximale afstand tot meetstation"),
):
    """Luchtkwaliteit van dichtstbijzijnd meetstation."""
    collector = create_luchtmeetnet_collector(max_distance_km=max_distance_km)
    result = collector.get_for_location(lat, lng)
    return LuchtmeetnetResponse(
        station_number=result.station_number,
        station_naam=result.station_naam,
        station_type=result.station_type,
        distance_km=result.distance_km,
        within_max_distance=result.within_max_distance,
        no2_avg=result.no2_avg,
        pm10_avg=result.pm10_avg,
        pm25_avg=result.pm25_avg,
        o3_avg=result.o3_avg,
        jaar=result.jaar,
    )


@router.get("/pfas", response_model=PFASResponse)
async def get_pfas(
    lat: float = Query(..., description="Breedtegraad (WGS84)"),
    lng: float = Query(..., description="Lengtegraad (WGS84)"),
    radius_km: float = Query(1.0, description="Zoekradius in km"),
):
    """PFAS bodemverontreiniging nabij een locatie (RIVM + gemeentelijke bodemkaart)."""
    collector = create_rivm_pfas_collector(search_radius_km=radius_km)
    bodemkaart_collector = create_pfas_bodemkaart_collector()

    result, bk = await asyncio.gather(
        asyncio.to_thread(collector.get_for_location, lat, lng),
        asyncio.to_thread(bodemkaart_collector.get_for_location, lat, lng),
    )

    bodemkaart = None
    if bk.in_den_haag:
        bodemkaart = BodemkaartResponse(
            in_den_haag=True,
            functie=bk.functie,
            zone_naam=bk.zone_naam,
            kwaliteit_bovengrond=bk.kwaliteit_bovengrond,
            kwaliteit_ondergrond=bk.kwaliteit_ondergrond,
            kwaliteit_ranking=bk.kwaliteit_ranking,
        )

    return PFASResponse(
        samples_within_radius=result.samples_within_radius,
        max_pfoa=result.max_pfoa,
        max_pfos=result.max_pfos,
        has_contamination=result.has_contamination,
        nearest_sample_distance_km=result.nearest_sample_distance_km,
        search_radius_km=result.search_radius_km,
        bodemkaart=bodemkaart,
    )


@router.get("/stations", response_model=List[StationInfo])
def get_stations():
    """Alle Luchtmeetnet stations met jaargemiddelden."""
    collector = create_luchtmeetnet_collector()
    stations = collector.get_all_stations()
    return [
        StationInfo(
            station_number=s["station_number"],
            naam=s["naam"],
            type=s["type"],
            lat=s["lat"],
            lng=s["lng"],
            formulas=s["formulas"],
            no2_avg=s.get("no2_avg"),
            pm10_avg=s.get("pm10_avg"),
            pm25_avg=s.get("pm2.5_avg"),
            o3_avg=s.get("o3_avg"),
            jaar=s.get("jaar"),
        )
        for s in stations
    ]
