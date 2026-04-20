"""Unified PDOK Locatieserver geocoder with in-memory TTL cache."""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import requests

PDOK_LOCATIE_URL = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"
_CACHE_TTL = timedelta(hours=24)
_geocode_cache: dict = {}


@dataclass
class PDOKResult:
    lat: float
    lng: float
    rd_x: float
    rd_y: float
    buurt_code: Optional[str] = None
    buurt_naam: Optional[str] = None
    adresseerbaarobject_id: Optional[str] = None


def _parse_point(wkt: str) -> Optional[tuple]:
    """Parse WKT POINT(x y) → (x, y)."""
    m = re.match(r"POINT\(([\d.]+)\s+([\d.]+)\)", wkt or "")
    return (float(m.group(1)), float(m.group(2))) if m else None


def geocode_pdok_full(postcode: str, huisnummer: int) -> Optional[PDOKResult]:
    """Single PDOK call: lat/lng, RD coords, buurtcode, adresseerbaarobject_id.

    Results are cached in-memory for 24 hours to avoid repeated calls within
    the same process lifetime.
    """
    key = f"{postcode.replace(' ', '').upper()}_{huisnummer}"
    if key in _geocode_cache:
        result, ts = _geocode_cache[key]
        if datetime.now() - ts < _CACHE_TTL:
            return result

    pc = postcode.replace(" ", "").upper()
    try:
        resp = requests.get(
            PDOK_LOCATIE_URL,
            params={
                "q": f"{pc} {huisnummer}",
                "fq": "type:adres",
                "rows": 1,
                "fl": "buurtcode,buurtnaam,centroide_ll,centroide_rd,adresseerbaarobject_id",
            },
            timeout=10,
        )
        resp.raise_for_status()
        docs = resp.json().get("response", {}).get("docs", [])
        if not docs:
            return None

        doc = docs[0]
        ll = _parse_point(doc.get("centroide_ll", ""))
        rd = _parse_point(doc.get("centroide_rd", ""))
        if not ll or not rd:
            return None

        buurt_code = doc.get("buurtcode")
        if buurt_code and not buurt_code.startswith("BU"):
            buurt_code = f"BU{buurt_code}"

        result = PDOKResult(
            lat=ll[1],  # centroide_ll: POINT(lng lat)
            lng=ll[0],
            rd_x=rd[0],
            rd_y=rd[1],
            buurt_code=buurt_code,
            buurt_naam=doc.get("buurtnaam"),
            adresseerbaarobject_id=doc.get("adresseerbaarobject_id"),
        )
        _geocode_cache[key] = (result, datetime.now())
        return result
    except Exception:
        return None
