"""Postcode-6 area API routes."""

import json
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from models import get_db
from models.postcode6 import Postcode6

router = APIRouter(prefix="/api/postcode6", tags=["postcode6"])

# CBS-namen voor standaard gemeenten
DEFAULT_GEMEENTEN = ["'s-Gravenhage", "Leidschendam-Voorburg", "Rijswijk"]
GEMEENTE_ALIAS = {"Den Haag": "'s-Gravenhage"}


@router.get("/geojson")
def get_postcode6_geojson(
    gemeente: Optional[str] = Query(None, description="Filter by municipality"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Get PC6 postcode boundaries as GeoJSON FeatureCollection."""
    query = db.query(Postcode6).filter(Postcode6.geometrie.isnot(None))

    if gemeente:
        cbs_naam = GEMEENTE_ALIAS.get(gemeente, gemeente)
        query = query.filter(Postcode6.gemeente_naam.ilike(f"%{cbs_naam}%"))

    postcode6s = query.all()

    features = []
    for pc6 in postcode6s:
        geometry = pc6.geometrie
        if isinstance(geometry, str):
            geometry = json.loads(geometry)

        features.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "postcode": pc6.postcode,
                "aantal_adressen": pc6.aantal_adressen,
            },
        })

    return {
        "type": "FeatureCollection",
        "features": features,
    }
