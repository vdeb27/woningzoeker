"""Geo-utilities: afstandsberekening, coördinaatconversie, centroid."""

import json
import math
from typing import Any, Dict, List, Optional, Tuple


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Bereken afstand in km tussen twee WGS84-coördinaten (haversine)."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlng / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def rd_to_wgs84(x: float, y: float) -> Tuple[float, float]:
    """Converteer Rijksdriehoekscoördinaten (EPSG:28992) naar WGS84 (lat, lng).

    Gebruikt de benaderingsformules van Schreutelkamp & Strang (2009).
    Nauwkeurigheid: ~1 meter, voldoende voor afstandsberekeningen.
    """
    x0, y0 = 155000.0, 463000.0
    phi0, lam0 = 52.15517440, 5.38720621

    dx = 1e-5 * (x - x0)
    dy = 1e-5 * (y - y0)

    phi = phi0 + (
        3235.65389 * dy
        - 32.58297 * dx**2
        - 0.24750 * dy**2
        - 0.84978 * dx**2 * dy
        - 0.06550 * dy**3
        - 0.01709 * dx**2 * dy**2
        - 0.00738 * dx
        + 0.00530 * dx**4
        - 0.00039 * dx**2 * dy**3
        + 0.00033 * dx**4 * dy
        - 0.00012 * dx * dy
    ) / 3600.0

    lam = lam0 + (
        5260.52916 * dx
        + 105.94684 * dx * dy
        + 2.45656 * dx * dy**2
        - 0.81885 * dx**3
        + 0.05594 * dx * dy**3
        - 0.05607 * dx**3 * dy
        + 0.01199 * dy
        - 0.00256 * dx**3 * dy**2
        + 0.00128 * dx * dy**4
        + 0.00022 * dy**2
        - 0.00022 * dx**2
        + 0.00026 * dx**5
    ) / 3600.0

    return phi, lam


def compute_centroid(
    geometry: Any,
) -> Optional[Tuple[float, float]]:
    """Bereken simpele centroid (gemiddelde coördinaten) van een GeoJSON geometry.

    Accepteert een dict (GeoJSON geometry) of een JSON-string.
    Retourneert (lat, lng) of None als het niet kan.
    """
    if geometry is None:
        return None

    if isinstance(geometry, str):
        try:
            geometry = json.loads(geometry)
        except (json.JSONDecodeError, TypeError):
            return None

    if not isinstance(geometry, dict):
        return None

    geom_type = geometry.get("type", "")
    coords = geometry.get("coordinates")
    if not coords:
        return None

    points: List[Tuple[float, float]] = []

    def _extract_points(c: Any, depth: int) -> None:
        """Recursief coördinaten extraheren."""
        if depth == 0:
            if isinstance(c, (list, tuple)) and len(c) >= 2:
                points.append((c[1], c[0]))  # GeoJSON is [lng, lat]
        else:
            if isinstance(c, (list, tuple)):
                for item in c:
                    _extract_points(item, depth - 1)

    if geom_type == "Point":
        _extract_points(coords, 0)
    elif geom_type in ("MultiPoint", "LineString"):
        _extract_points(coords, 1)
    elif geom_type in ("MultiLineString", "Polygon"):
        _extract_points(coords, 2)
    elif geom_type in ("MultiPolygon",):
        _extract_points(coords, 3)
    else:
        return None

    if not points:
        return None

    avg_lat = sum(p[0] for p in points) / len(points)
    avg_lng = sum(p[1] for p in points) / len(points)
    return avg_lat, avg_lng
