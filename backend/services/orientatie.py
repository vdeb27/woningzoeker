"""
Oriëntatie en schaduwanalyse service.

Berekent tuinoriëntatie, zonuren (incl. schaduw van eigen gebouw,
buurtgebouwen en bomen), en zonnepanelen geschiktheid.

Databronnen: 3DBAG, PDOK Kadaster, BGT, AHN.
"""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class OrientatieResult:
    """Complete oriëntatie- en zonanalyse resultaat."""

    # Tuinoriëntatie
    tuin_orientatie: Optional[str] = None
    tuin_azimut: Optional[float] = None
    tuin_oppervlakte_berekend: Optional[float] = None

    # Zonuren (incl. schaduwcorrectie)
    zon_uren_zomer: Optional[float] = None
    zon_uren_lente: Optional[float] = None
    zon_uren_winter: Optional[float] = None
    zon_label: Optional[str] = None

    # Schaduwanalyse
    schaduw_eigen_gebouw: Optional[str] = None
    schaduw_buren: Optional[str] = None
    schaduw_bomen: Optional[str] = None
    effectieve_tuin_diepte: Optional[float] = None

    # Zonnepanelen
    zonnepanelen_score: Optional[int] = None
    zonnepanelen_label: Optional[str] = None
    dak_orientatie: Optional[str] = None
    dak_hellingshoek: Optional[float] = None
    geschikt_dakoppervlak: Optional[float] = None

    # Meta
    methode: Optional[str] = None
    betrouwbaarheid: Optional[str] = None
    details: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}


# --- Zonnestand berekening ---

def bereken_zonnestand(uur: float, dag_van_jaar: int, latitude: float = 52.0) -> tuple[float, float]:
    """Bereken zonazimut en zonhoogte voor een gegeven tijdstip.

    Parameters
    ----------
    uur : float
        Uur van de dag in UTC+2 (zomertijd), bijv. 14.5 = 14:30
    dag_van_jaar : int
        Dag van het jaar (1-365), bijv. 172 = 21 juni
    latitude : float
        Breedtegraad in graden (default: 52.0 voor Nederland)

    Returns
    -------
    tuple of (azimut, hoogte) in graden
        azimut: 0=Noord, 90=Oost, 180=Zuid, 270=West
        hoogte: graden boven horizon (negatief = onder horizon)
    """
    lat_rad = math.radians(latitude)

    # Zonsdeclinatie (Cooper formule)
    declinatie = 23.45 * math.sin(math.radians(360 * (284 + dag_van_jaar) / 365))
    dec_rad = math.radians(declinatie)

    # Uurhoek (solar hour angle): 12:00 UTC+2 ≈ 12:00 solar time voor NL
    # Correctie voor tijdzone: NL is ~5° oost, UTC+2 zomertijd
    solar_hour = uur - 2.0 + (5.0 / 15.0)  # correctie naar solar time
    hour_angle = math.radians((solar_hour - 12.0) * 15.0)

    # Zonhoogte (altitude)
    sin_alt = (
        math.sin(lat_rad) * math.sin(dec_rad)
        + math.cos(lat_rad) * math.cos(dec_rad) * math.cos(hour_angle)
    )
    altitude = math.degrees(math.asin(max(-1, min(1, sin_alt))))

    # Zonazimut
    cos_az = (
        math.sin(dec_rad) - math.sin(lat_rad) * sin_alt
    ) / max(math.cos(lat_rad) * math.cos(math.radians(altitude)), 1e-10)
    cos_az = max(-1, min(1, cos_az))
    azimut = math.degrees(math.acos(cos_az))

    # Correctie: azimut is van zuid, omrekenen naar noord
    if hour_angle > 0:
        azimut = 360 - azimut

    return round(azimut, 1), round(altitude, 1)


# --- Kompasrichting ---

KOMPAS_RICHTINGEN = [
    (0, "Noord"), (45, "Noordoost"), (90, "Oost"), (135, "Zuidoost"),
    (180, "Zuid"), (225, "Zuidwest"), (270, "West"), (315, "Noordwest"),
]


def azimut_naar_kompas(azimut: float) -> str:
    """Converteer azimut (graden) naar 8-punt kompasrichting."""
    azimut = azimut % 360
    best_label = "Noord"
    best_diff = 360
    for angle, label in KOMPAS_RICHTINGEN:
        diff = abs(azimut - angle)
        if diff > 180:
            diff = 360 - diff
        if diff < best_diff:
            best_diff = diff
            best_label = label
    return best_label


# --- Tuinoriëntatie ---

def bepaal_tuin_orientatie(
    building_footprint_rd: Optional[List[List[float]]],
    perceel_polygon_rd: Optional[List[List[float]]],
    funda_tuin_orientatie: Optional[str] = None,
) -> tuple[Optional[str], Optional[float], Optional[float]]:
    """Bepaal tuinoriëntatie op basis van perceelgrens en gebouw-footprint.

    Returns (orientatie_label, azimut, tuin_oppervlakte)
    """
    if building_footprint_rd and perceel_polygon_rd:
        try:
            from shapely.geometry import MultiPolygon, Polygon

            building = Polygon(building_footprint_rd)
            perceel = Polygon(perceel_polygon_rd)

            if not building.is_valid:
                building = building.buffer(0)
            if not perceel.is_valid:
                perceel = perceel.buffer(0)

            tuin = perceel.difference(building)
            tuin_opp = round(tuin.area, 1)

            if tuin_opp < 1.0:
                return None, None, 0.0

            # Bepaal tuinoriëntatie: richting van gebouw-centroid naar tuin-centroid
            bld_cx, bld_cy = building.centroid.x, building.centroid.y

            # Voor MultiPolygon (L-vormige tuinen bij hoekwoningen):
            # gebruik het grootste deel
            if isinstance(tuin, MultiPolygon):
                tuin_parts = list(tuin.geoms)
                tuin_main = max(tuin_parts, key=lambda p: p.area)
            else:
                tuin_main = tuin

            tuin_cx, tuin_cy = tuin_main.centroid.x, tuin_main.centroid.y

            # Vector van gebouw naar tuin
            dx = tuin_cx - bld_cx
            dy = tuin_cy - bld_cy

            # Azimut: 0=Noord, 90=Oost, 180=Zuid, 270=West
            azimut = (math.degrees(math.atan2(dx, dy)) + 360) % 360
            label = azimut_naar_kompas(azimut)

            return label, round(azimut, 1), tuin_opp

        except Exception as e:
            logger.warning(f"Fout bij tuinoriëntatie berekening: {e}")

    # Fallback: parse Funda tuin_orientatie
    if funda_tuin_orientatie:
        return _parse_funda_orientatie(funda_tuin_orientatie)

    return None, None, None


def _parse_funda_orientatie(
    text: str,
) -> tuple[Optional[str], Optional[float], Optional[float]]:
    """Parse Funda tuin_orientatie string naar richting en azimut."""
    text_lower = text.lower()

    richting_map = {
        "noord": 0, "noordoost": 45, "oost": 90, "zuidoost": 135,
        "zuid": 180, "zuidwest": 225, "west": 270, "noordwest": 315,
        "noorden": 0, "noordoosten": 45, "oosten": 90, "zuidoosten": 135,
        "zuiden": 180, "zuidwesten": 225, "westen": 270, "noordwesten": 315,
    }

    for richting, azimut in sorted(richting_map.items(), key=lambda x: -len(x[0])):
        if richting in text_lower:
            label = azimut_naar_kompas(azimut)
            return label, float(azimut), None

    return None, None, None


# --- Schaduwberekening ---

def _schaduw_lengte(hoogte: float, zon_altitude: float) -> float:
    """Bereken schaduwlengte gegeven objecthoogte en zonhoogte."""
    if zon_altitude <= 0:
        return 999.0  # zon onder horizon
    return hoogte / math.tan(math.radians(zon_altitude))


def _schaduw_richting(zon_azimut: float) -> float:
    """Schaduw valt in tegengestelde richting van de zon."""
    return (zon_azimut + 180) % 360


def bereken_schaduw_op_punt(
    punt_x: float,
    punt_y: float,
    schaduwwerpers: List[Dict[str, Any]],
    zon_azimut: float,
    zon_altitude: float,
) -> bool:
    """Check of een punt in de schaduw ligt van enig object.

    Parameters
    ----------
    punt_x, punt_y : float
        Punt om te checken (RD)
    schaduwwerpers : list of dict
        Elk met: x, y, hoogte, (optioneel: footprint_rd)
    zon_azimut, zon_altitude : float
        Huidige zonstand

    Returns
    -------
    bool
        True als het punt in de schaduw ligt
    """
    if zon_altitude <= 2:
        return True  # Zon te laag, effectief geen zon

    schaduw_len_per_m = 1.0 / math.tan(math.radians(zon_altitude))
    # Schaduwrichting als dx, dy per meter hoogte
    schaduw_az = math.radians(_schaduw_richting(zon_azimut))
    dx_per_m = math.sin(schaduw_az) * schaduw_len_per_m
    dy_per_m = math.cos(schaduw_az) * schaduw_len_per_m

    for obj in schaduwwerpers:
        hoogte = obj.get("hoogte", 0)
        if hoogte < 1.5:
            continue

        footprint = obj.get("footprint_rd")
        if footprint and len(footprint) >= 3:
            # Voor gebouwen: check of het punt in de schaduwprojectie ligt
            if _punt_in_gebouwschaduw(
                punt_x, punt_y, footprint, hoogte, zon_azimut, zon_altitude
            ):
                return True
        else:
            # Voor bomen (puntobjecten): simplere check
            obj_x = obj.get("rd_x", obj.get("x", 0))
            obj_y = obj.get("rd_y", obj.get("y", 0))
            schaduw_tip_x = obj_x + dx_per_m * hoogte
            schaduw_tip_y = obj_y + dy_per_m * hoogte

            # Check of punt binnen ~3m van de schaduwlijn ligt (boomkruin)
            kruin_radius = min(hoogte * 0.4, 5.0)  # Kruin ~40% van hoogte
            if _punt_nabij_lijn(
                punt_x, punt_y, obj_x, obj_y,
                schaduw_tip_x, schaduw_tip_y, kruin_radius
            ):
                return True

    return False


def _punt_in_gebouwschaduw(
    px: float,
    py: float,
    footprint: List[List[float]],
    hoogte: float,
    zon_azimut: float,
    zon_altitude: float,
) -> bool:
    """Check of een punt in de schaduw van een gebouw ligt."""
    try:
        from shapely.geometry import Point, Polygon
        from shapely.affinity import translate

        gebouw = Polygon(footprint)
        if not gebouw.is_valid:
            gebouw = gebouw.buffer(0)

        schaduw_len = _schaduw_lengte(hoogte, zon_altitude)
        schaduw_az = math.radians(_schaduw_richting(zon_azimut))
        dx = math.sin(schaduw_az) * schaduw_len
        dy = math.cos(schaduw_az) * schaduw_len

        # Schaduwpolygon = unie van origineel gebouw en verschoven gebouw
        verschoven = translate(gebouw, xoff=dx, yoff=dy)
        schaduw_poly = gebouw.union(verschoven).convex_hull

        return schaduw_poly.contains(Point(px, py))
    except Exception:
        return False


def _punt_nabij_lijn(
    px: float, py: float,
    x1: float, y1: float,
    x2: float, y2: float,
    max_afstand: float,
) -> bool:
    """Check of een punt binnen max_afstand van een lijnstuk ligt."""
    dx = x2 - x1
    dy = y2 - y1
    len_sq = dx * dx + dy * dy
    if len_sq < 0.01:
        return math.hypot(px - x1, py - y1) <= max_afstand

    t = max(0, min(1, ((px - x1) * dx + (py - y1) * dy) / len_sq))
    proj_x = x1 + t * dx
    proj_y = y1 + t * dy
    dist = math.hypot(px - proj_x, py - proj_y)
    return dist <= max_afstand


# --- Zonuren berekening ---

def bereken_zon_uren(
    tuin_centroid_x: float,
    tuin_centroid_y: float,
    building_footprint_rd: Optional[List[List[float]]],
    gebouwhoogte: float,
    buurtgebouwen: List[Dict[str, Any]],
    bomen: List[Dict[str, Any]],
    latitude: float = 52.0,
) -> Dict[str, Any]:
    """Bereken zonuren voor zomer, lente en winter incl. schaduwanalyse.

    Returns dict met zon_uren_zomer, zon_uren_lente, zon_uren_winter, label,
    schaduw_eigen_gebouw, schaduw_buren, schaduw_bomen.
    """
    # Bouw lijst van alle schaduwwerpers
    schaduwwerpers = []

    # Eigen gebouw
    if building_footprint_rd and gebouwhoogte:
        schaduwwerpers.append({
            "footprint_rd": building_footprint_rd,
            "hoogte": gebouwhoogte,
            "type": "eigen",
        })

    # Buurtgebouwen
    for buur in buurtgebouwen:
        if buur.get("hoogte") and buur.get("hoogte") > 2.0:
            schaduwwerpers.append({
                "footprint_rd": buur.get("footprint_rd"),
                "hoogte": buur["hoogte"],
                "rd_x": _centroid_x(buur.get("footprint_rd")),
                "rd_y": _centroid_y(buur.get("footprint_rd")),
                "type": "buur",
            })

    # Bomen
    for boom in bomen:
        schaduwwerpers.append({
            "rd_x": boom["rd_x"],
            "rd_y": boom["rd_y"],
            "hoogte": boom["hoogte"],
            "type": "boom",
        })

    # Referentiedagen: zomer (172), lente (80), winter (355)
    dagen = {"zomer": 172, "lente": 80, "winter": 355}
    resultaat = {}

    schaduw_eigen_uren = {"zomer": 0, "lente": 0, "winter": 0}
    schaduw_buren_uren = {"zomer": 0, "lente": 0, "winter": 0}
    schaduw_bomen_uren = {"zomer": 0, "lente": 0, "winter": 0}

    for seizoen, dag in dagen.items():
        zon_uren = 0.0
        tijdstappen = 0

        for half_uur in range(10, 44):  # 5:00 - 22:00 (UTC+2)
            uur = half_uur / 2.0
            az, alt = bereken_zonnestand(uur, dag, latitude)

            if alt <= 2:
                continue

            tijdstappen += 1

            # Check schaduw per type
            in_schaduw = False
            in_eigen = bereken_schaduw_op_punt(
                tuin_centroid_x, tuin_centroid_y,
                [s for s in schaduwwerpers if s["type"] == "eigen"],
                az, alt,
            )
            in_buren = bereken_schaduw_op_punt(
                tuin_centroid_x, tuin_centroid_y,
                [s for s in schaduwwerpers if s["type"] == "buur"],
                az, alt,
            )
            in_bomen = bereken_schaduw_op_punt(
                tuin_centroid_x, tuin_centroid_y,
                [s for s in schaduwwerpers if s["type"] == "boom"],
                az, alt,
            )

            if in_eigen:
                schaduw_eigen_uren[seizoen] += 0.5
            if in_buren:
                schaduw_buren_uren[seizoen] += 0.5
            if in_bomen:
                schaduw_bomen_uren[seizoen] += 0.5

            if in_eigen or in_buren or in_bomen:
                in_schaduw = True

            if not in_schaduw:
                zon_uren += 0.5

        resultaat[f"zon_uren_{seizoen}"] = round(zon_uren, 1)

    # Bepaal label op basis van zomerzonuren
    zomer_uren = resultaat.get("zon_uren_zomer", 0)
    if zomer_uren >= 6:
        resultaat["zon_label"] = "Veel zon"
    elif zomer_uren >= 4:
        resultaat["zon_label"] = "Gemiddeld"
    else:
        resultaat["zon_label"] = "Weinig zon"

    # Schaduw-beschrijvingen
    resultaat["schaduw_eigen_gebouw"] = _beschrijf_eigen_schaduw(
        gebouwhoogte, schaduw_eigen_uren
    )
    resultaat["schaduw_buren"] = _beschrijf_buren_schaduw(
        buurtgebouwen, schaduw_buren_uren, tuin_centroid_x, tuin_centroid_y
    )
    resultaat["schaduw_bomen"] = _beschrijf_bomen_schaduw(
        bomen, schaduw_bomen_uren, tuin_centroid_x, tuin_centroid_y
    )

    # Effectieve tuindiepte: schaduwlengte eigen gebouw bij zomermiddag
    if gebouwhoogte:
        _, alt_zomer = bereken_zonnestand(13.0, 172, latitude)
        if alt_zomer > 0:
            resultaat["effectieve_tuin_diepte"] = round(
                _schaduw_lengte(gebouwhoogte, alt_zomer), 1
            )

    return resultaat


def _centroid_x(footprint: Optional[List[List[float]]]) -> float:
    if not footprint:
        return 0.0
    return sum(p[0] for p in footprint) / len(footprint)


def _centroid_y(footprint: Optional[List[List[float]]]) -> float:
    if not footprint:
        return 0.0
    return sum(p[1] for p in footprint) / len(footprint)


def _beschrijf_eigen_schaduw(
    hoogte: Optional[float], uren: Dict[str, float]
) -> Optional[str]:
    if not hoogte or uren["zomer"] < 0.5:
        return None
    return (
        f"Eigen gebouw ({hoogte:.0f}m) veroorzaakt "
        f"{uren['zomer']:.0f}u schaduw in zomer, "
        f"{uren['winter']:.0f}u in winter"
    )


def _beschrijf_buren_schaduw(
    buurtgebouwen: List[Dict[str, Any]],
    uren: Dict[str, float],
    tuin_x: float,
    tuin_y: float,
) -> Optional[str]:
    if uren["zomer"] < 0.5:
        return None

    # Vind dichtstbijzijnde hoge buur
    closest = None
    closest_dist = float("inf")
    for buur in buurtgebouwen:
        fp = buur.get("footprint_rd")
        if not fp or buur.get("hoogte", 0) < 5:
            continue
        bx = _centroid_x(fp)
        by = _centroid_y(fp)
        dist = math.hypot(bx - tuin_x, by - tuin_y)
        if dist < closest_dist:
            closest_dist = dist
            closest = buur

    if closest:
        return (
            f"Buurpand ({closest['hoogte']:.0f}m, {closest_dist:.0f}m afstand) "
            f"veroorzaakt {uren['zomer']:.0f}u schaduw in zomer"
        )
    return f"Buurtgebouwen veroorzaken {uren['zomer']:.0f}u schaduw in zomer"


def _beschrijf_bomen_schaduw(
    bomen: List[Dict[str, Any]],
    uren: Dict[str, float],
    tuin_x: float,
    tuin_y: float,
) -> Optional[str]:
    if uren["zomer"] < 0.5 or not bomen:
        return None

    # Tel bomen die dichtbij de tuin staan
    nabije_bomen = [
        b for b in bomen
        if math.hypot(b["rd_x"] - tuin_x, b["rd_y"] - tuin_y) < 30
    ]
    if not nabije_bomen:
        return None

    max_h = max(b["hoogte"] for b in nabije_bomen)
    return (
        f"{len(nabije_bomen)} {'boom' if len(nabije_bomen) == 1 else 'bomen'} "
        f"(tot {max_h:.0f}m) {'veroorzaakt' if len(nabije_bomen) == 1 else 'veroorzaken'} "
        f"{uren['zomer']:.0f}u schaduw in zomer"
    )


# --- Zonnepanelen score ---

def bereken_zonnepanelen_score(
    dak_azimut: Optional[float],
    dak_hellingshoek: Optional[float],
    opp_dak_schuin: Optional[float],
    opp_dak_plat: Optional[float],
    dak_type: Optional[str],
) -> Dict[str, Any]:
    """Bereken zonnepanelen geschiktheid score (1-10).

    Ideaal: zuid-georiënteerd (180°), 30-40° helling.
    """
    if dak_azimut is None and opp_dak_plat is None and opp_dak_schuin is None:
        return {}

    score = 5.0  # Basiscore

    # Oriëntatiefactor (max 3 punten)
    if dak_azimut is not None:
        # cos(azimut - 180) = 1 bij zuid, -1 bij noord
        orientatie_factor = (1 + math.cos(math.radians(dak_azimut - 180))) / 2
        score += orientatie_factor * 3.0 - 1.5
    else:
        # Plat dak zonder oriëntatie: panelen kunnen optimaal gericht worden
        score += 1.0

    # Hellingsfactor (max 2 punten)
    if dak_hellingshoek is not None and dak_hellingshoek > 2:
        # Optimaal: 30-40°
        if 25 <= dak_hellingshoek <= 45:
            score += 2.0
        elif 15 <= dak_hellingshoek <= 50:
            score += 1.0
        else:
            score += 0.0
    elif dak_type and "horizontal" in dak_type.lower():
        # Plat dak: goed, panelen op frame
        score += 1.5

    # Beschikbaar oppervlak (max 1 punt)
    totaal_opp = (opp_dak_schuin or 0) + (opp_dak_plat or 0)
    if totaal_opp > 40:
        score += 1.0
    elif totaal_opp > 20:
        score += 0.5

    score = max(1, min(10, round(score)))

    # Label
    if score >= 8:
        label = "Zeer geschikt"
    elif score >= 6:
        label = "Geschikt"
    elif score >= 4:
        label = "Matig geschikt"
    else:
        label = "Beperkt geschikt"

    # Bepaal geschikt oppervlak
    geschikt_opp = None
    if dak_type and "horizontal" in dak_type.lower():
        # Plat dak: ~60% bruikbaar (randen, doorvoeren)
        geschikt_opp = round((opp_dak_plat or 0) * 0.6, 1)
    elif opp_dak_schuin and opp_dak_schuin > 0:
        # Schuin dak: ~ 80% bruikbaar van de zuidkant
        geschikt_opp = round(opp_dak_schuin * 0.8, 1)

    dak_orient = azimut_naar_kompas(dak_azimut) if dak_azimut is not None else None

    return {
        "zonnepanelen_score": score,
        "zonnepanelen_label": label,
        "dak_orientatie": dak_orient,
        "dak_hellingshoek": dak_hellingshoek,
        "geschikt_dakoppervlak": geschikt_opp,
    }


# --- Hoofd-entrypoint ---

def bereken_orientatie(
    building_footprint_rd: Optional[List[List[float]]] = None,
    perceel_polygon_rd: Optional[List[List[float]]] = None,
    gebouwhoogte: Optional[float] = None,
    dak_azimut: Optional[float] = None,
    dak_hellingshoek: Optional[float] = None,
    opp_dak_schuin: Optional[float] = None,
    opp_dak_plat: Optional[float] = None,
    dak_type: Optional[str] = None,
    buurtgebouwen: Optional[List[Dict[str, Any]]] = None,
    bomen: Optional[List[Dict[str, Any]]] = None,
    funda_tuin_orientatie: Optional[str] = None,
    latitude: float = 52.0,
) -> OrientatieResult:
    """Hoofd-entrypoint voor oriëntatie- en zonanalyse."""
    result = OrientatieResult()
    methode_parts = []

    # 1. Tuinoriëntatie
    tuin_label, tuin_az, tuin_opp = bepaal_tuin_orientatie(
        building_footprint_rd, perceel_polygon_rd, funda_tuin_orientatie
    )
    result.tuin_orientatie = tuin_label
    result.tuin_azimut = tuin_az
    result.tuin_oppervlakte_berekend = tuin_opp

    if building_footprint_rd and perceel_polygon_rd:
        methode_parts.append("3DBAG+Kadaster")
    elif funda_tuin_orientatie:
        methode_parts.append("Funda")

    # 2. Zonuren met schaduwanalyse
    if tuin_az is not None and building_footprint_rd:
        try:
            from shapely.geometry import Polygon

            # Bereken tuin-centroid
            building = Polygon(building_footprint_rd)
            if perceel_polygon_rd:
                perceel = Polygon(perceel_polygon_rd)
                tuin = perceel.difference(building.buffer(0))
                if hasattr(tuin, 'geoms'):
                    tuin_main = max(tuin.geoms, key=lambda p: p.area)
                else:
                    tuin_main = tuin
                tuin_cx, tuin_cy = tuin_main.centroid.x, tuin_main.centroid.y
            else:
                # Schat tuin-centroid op basis van azimut
                bld_cx, bld_cy = building.centroid.x, building.centroid.y
                offset = 10.0  # 10m achter het huis
                tuin_cx = bld_cx + offset * math.sin(math.radians(tuin_az))
                tuin_cy = bld_cy + offset * math.cos(math.radians(tuin_az))

            zon_data = bereken_zon_uren(
                tuin_cx, tuin_cy,
                building_footprint_rd,
                gebouwhoogte or 0,
                buurtgebouwen or [],
                bomen or [],
                latitude,
            )

            result.zon_uren_zomer = zon_data.get("zon_uren_zomer")
            result.zon_uren_lente = zon_data.get("zon_uren_lente")
            result.zon_uren_winter = zon_data.get("zon_uren_winter")
            result.zon_label = zon_data.get("zon_label")
            result.schaduw_eigen_gebouw = zon_data.get("schaduw_eigen_gebouw")
            result.schaduw_buren = zon_data.get("schaduw_buren")
            result.schaduw_bomen = zon_data.get("schaduw_bomen")
            result.effectieve_tuin_diepte = zon_data.get("effectieve_tuin_diepte")

            if buurtgebouwen or bomen:
                methode_parts.append("AHN+BGT")

        except Exception as e:
            logger.warning(f"Fout bij zonurenberekening: {e}")

    # 3. Zonnepanelen score
    paneel_data = bereken_zonnepanelen_score(
        dak_azimut, dak_hellingshoek, opp_dak_schuin, opp_dak_plat, dak_type
    )
    if paneel_data:
        result.zonnepanelen_score = paneel_data.get("zonnepanelen_score")
        result.zonnepanelen_label = paneel_data.get("zonnepanelen_label")
        result.dak_orientatie = paneel_data.get("dak_orientatie")
        result.dak_hellingshoek = paneel_data.get("dak_hellingshoek")
        result.geschikt_dakoppervlak = paneel_data.get("geschikt_dakoppervlak")

    # Meta
    result.methode = "+".join(methode_parts) if methode_parts else None
    if building_footprint_rd and perceel_polygon_rd and (buurtgebouwen or bomen):
        result.betrouwbaarheid = "hoog"
    elif building_footprint_rd or funda_tuin_orientatie:
        result.betrouwbaarheid = "gemiddeld"
    else:
        result.betrouwbaarheid = "laag"

    details = []
    if result.tuin_orientatie:
        details.append(f"Tuin op het {result.tuin_orientatie.lower()}")
    if result.zon_uren_zomer is not None:
        details.append(f"{result.zon_uren_zomer:.0f}u zon in zomer")
    if result.effectieve_tuin_diepte:
        details.append(f"Eigen schaduw tot {result.effectieve_tuin_diepte:.0f}m")
    result.details = ", ".join(details) if details else None

    return result
