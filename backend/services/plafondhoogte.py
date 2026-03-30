"""
Plafondhoogte inschatting op basis van 3DBAG en/of Funda data.

Twee methodes:
- Methode A (3DBAG+BAG): gebouwhoogte / aantal bouwlagen, met dakcorrectie
- Methode B (Funda): inhoud / woonoppervlakte / verdiepingen
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# Dikte vloer/plafondconstructie in meters (gemiddeld)
CONSTRUCTIE_DIKTE = 0.3

# Correctiefactor voor schuine daken: nokhoogte is hoger dan werkelijke
# bruikbare hoogte op de bovenste verdieping
SCHUIN_DAK_CORRECTIE = 0.85


@dataclass
class PlafondhoogteResult:
    """Resultaat van de plafondhoogte-inschatting."""

    geschatte_verdiepingshoogte: Optional[float] = None
    label: Optional[str] = None
    methode: Optional[str] = None
    betrouwbaarheid: Optional[str] = None
    details: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "geschatte_verdiepingshoogte": self.geschatte_verdiepingshoogte,
            "label": self.label,
            "methode": self.methode,
            "betrouwbaarheid": self.betrouwbaarheid,
            "details": self.details,
        }


def _bepaal_label(hoogte: float) -> str:
    """Bepaal label op basis van geschatte verdiepingshoogte."""
    if hoogte < 2.6:
        return "Standaard"
    elif hoogte < 3.0:
        return "Ruim"
    elif hoogte < 3.5:
        return "Hoog"
    else:
        return "Zeer hoog"


def _is_schuin_dak(
    dak_type_3dbag: Optional[str] = None,
    dak_type_funda: Optional[str] = None,
    opp_dak_schuin: Optional[float] = None,
    opp_dak_plat: Optional[float] = None,
) -> bool:
    """Bepaal of het gebouw een schuin dak heeft."""
    if dak_type_3dbag:
        if "slanted" in dak_type_3dbag.lower():
            return True
        if "horizontal" in dak_type_3dbag.lower() or "flat" in dak_type_3dbag.lower():
            return False

    # Gebruik oppervlakte-verhouding als dak_type niet duidelijk is
    if opp_dak_schuin is not None and opp_dak_plat is not None:
        totaal = opp_dak_schuin + opp_dak_plat
        if totaal > 0 and opp_dak_schuin / totaal > 0.5:
            return True

    if dak_type_funda:
        funda_lower = dak_type_funda.lower()
        if any(w in funda_lower for w in ["schuin", "zadeldak", "mansarde", "puntdak"]):
            return True

    return False


def bereken_plafondhoogte(
    # 3DBAG data
    h_dak_max: Optional[float] = None,
    h_dak_min: Optional[float] = None,
    h_dak_50p: Optional[float] = None,
    h_maaiveld: Optional[float] = None,
    dak_type_3dbag: Optional[str] = None,
    bouwlagen_3dbag: Optional[int] = None,
    opp_dak_schuin: Optional[float] = None,
    opp_dak_plat: Optional[float] = None,
    # BAG data
    aantal_bouwlagen: Optional[int] = None,
    # Funda data
    inhoud: Optional[int] = None,
    woonoppervlakte: Optional[int] = None,
    verdiepingen: Optional[int] = None,
    dak_type_funda: Optional[str] = None,
) -> PlafondhoogteResult:
    """
    Bereken geschatte plafondhoogte.

    Probeert eerst Methode A (3DBAG + BAG), valt terug op Methode B (Funda).

    Returns
    -------
    PlafondhoogteResult
        Geschatte verdiepingshoogte met label en betrouwbaarheid.
        Kan lege waarden bevatten als berekening niet mogelijk is.
    """
    result = PlafondhoogteResult()

    # Methode A: 3DBAG + BAG
    bouwlagen = aantal_bouwlagen or bouwlagen_3dbag
    if h_dak_max is not None and h_maaiveld is not None and bouwlagen and bouwlagen > 0:
        schuin = _is_schuin_dak(dak_type_3dbag, dak_type_funda, opp_dak_schuin, opp_dak_plat)

        if schuin and h_dak_min is not None:
            # Bij schuin dak: gebruik mediaan dakhoogte als betere schatting
            dak_hoogte = h_dak_50p if h_dak_50p is not None else (h_dak_max + h_dak_min) / 2
        else:
            dak_hoogte = h_dak_max

        gebouwhoogte = dak_hoogte - h_maaiveld
        ruwe_hoogte = gebouwhoogte / bouwlagen

        # Trek constructiedikte af
        geschat = ruwe_hoogte - CONSTRUCTIE_DIKTE

        # Sanity check
        if 2.0 <= geschat <= 6.0:
            result.geschatte_verdiepingshoogte = round(geschat, 1)
            result.label = _bepaal_label(result.geschatte_verdiepingshoogte)
            result.methode = "3DBAG+BAG"
            result.betrouwbaarheid = "hoog"

            details_parts = [
                f"Gebouwhoogte {gebouwhoogte:.1f}m",
                f"{bouwlagen} bouwlagen",
            ]
            if schuin:
                details_parts.append("correctie schuin dak")
            result.details = ", ".join(details_parts)

            logger.info(
                f"Plafondhoogte (3DBAG): {result.geschatte_verdiepingshoogte}m "
                f"({result.label})"
            )
            return result
        else:
            logger.warning(
                f"Plafondhoogte sanity check mislukt: {geschat:.1f}m "
                f"(gebouwhoogte={gebouwhoogte:.1f}m, bouwlagen={bouwlagen})"
            )

    # Methode B: Funda (inhoud / oppervlakte / verdiepingen)
    if inhoud and woonoppervlakte and woonoppervlakte > 0:
        etages = verdiepingen if verdiepingen and verdiepingen > 0 else 1
        geschat = inhoud / woonoppervlakte / etages

        # Sanity check
        if 2.0 <= geschat <= 6.0:
            result.geschatte_verdiepingshoogte = round(geschat, 1)
            result.label = _bepaal_label(result.geschatte_verdiepingshoogte)
            result.methode = "Funda"
            result.betrouwbaarheid = "gemiddeld" if verdiepingen else "laag"
            result.details = f"Inhoud {inhoud}m³ / {woonoppervlakte}m² / {etages} woonlagen"

            logger.info(
                f"Plafondhoogte (Funda): {result.geschatte_verdiepingshoogte}m "
                f"({result.label})"
            )
            return result
        else:
            logger.warning(
                f"Plafondhoogte Funda sanity check mislukt: {geschat:.1f}m "
                f"(inhoud={inhoud}, opp={woonoppervlakte}, etages={etages})"
            )

    return result
