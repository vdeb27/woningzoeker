"""Property valuation service - the core feature of Woningzoeker."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from models import Buurt

# Module-level caches: gerichte per-buurt lookups, 1 uur geldig.
# Vervangt de bulk-load van alle 14k+ buurten per request (~7 seconden).
_buurt_cache: Dict[str, Any] = {}       # buurt_code → (m2_price, score, gemeente_code, cached_at)
_gemeente_cache: Dict[str, Any] = {}    # gemeente_code → (avg_m2_price, cached_at)
_PRICE_CACHE_TTL = timedelta(hours=1)


class BiedAdvies(str, Enum):
    """Bidding advice based on valuation."""
    ONDER_VRAAGPRIJS = "onder_vraagprijs"
    VRAAGPRIJS = "vraagprijs"
    LICHT_BOVEN = "licht_boven"
    BOVEN_VRAAGPRIJS = "boven_vraagprijs"


# Buurt quality correction based on score_totaal
BUURT_QUALITY_CORRECTIONS = [
    (0.80, 1.00, +0.08),   # Uitstekende buurt
    (0.65, 0.80, +0.04),   # Goede buurt
    (0.50, 0.65,  0.00),   # Gemiddeld (referentie)
    (0.35, 0.50, -0.03),   # Onder gemiddeld
    (0.00, 0.35, -0.06),   # Zwakke buurt
]


@dataclass
class ValuationResult:
    """Result of a property valuation."""

    # Estimated value range
    waarde_laag: int
    waarde_hoog: int
    waarde_midden: int

    # Comparison with asking price
    vraagprijs: Optional[int]
    verschil_percentage: Optional[float]  # Positive = asking price above estimate

    # Bidding advice
    bied_advies: BiedAdvies
    bied_range_laag: int
    bied_range_hoog: int

    # Components breakdown
    basis_waarde: int  # m2 price * area
    energielabel_correctie: int
    bouwjaar_correctie: int
    woningtype_correctie: int
    perceel_correctie: int  # Plot size correction
    buurt_kwaliteit_correctie: int  # Neighborhood quality correction
    markt_correctie: int

    # Confidence
    confidence: float  # 0-1, based on data completeness
    confidence_factors: Dict[str, bool]

    # Comparable properties
    vergelijkbare_woningen: List[Dict[str, Any]]


class ValuationService:
    """
    Estimate property values based on neighborhood data and property characteristics.

    Formula:
        Estimated value = Base (neighborhood m2 price * area)
                        + Energy label correction (-15% to +5%)
                        + Build year correction (new +10%, pre-1950 -5%)
                        + Property type correction (apartment -5%, detached +10%)
                        + Plot size correction
                        + Neighborhood quality correction (-6% to +8%)
                        ± Market conditions (current overbidding percentage)
    """

    # Energy label corrections (relative to C)
    ENERGY_CORRECTIONS = {
        "A++++": 0.05,
        "A+++": 0.05,
        "A++": 0.04,
        "A+": 0.03,
        "A": 0.02,
        "B": 0.01,
        "C": 0.00,
        "D": -0.03,
        "E": -0.06,
        "F": -0.10,
        "G": -0.15,
    }

    # Build year corrections
    BUILD_YEAR_CORRECTIONS = {
        (2020, 9999): 0.10,   # Nieuwbouw
        (2000, 2019): 0.05,   # Recent
        (1980, 1999): 0.00,   # Referentie
        (1960, 1979): -0.02,  # Naoorlogs
        (1945, 1959): -0.03,  # Wederopbouw
        (1900, 1944): -0.04,  # Vooroorlogs
        (0, 1899): -0.05,     # Historisch
    }

    # Property type corrections
    TYPE_CORRECTIONS = {
        "appartement": -0.05,
        "tussenwoning": 0.00,
        "hoekwoning": 0.02,
        "twee-onder-een-kap": 0.05,
        "vrijstaand": 0.10,
        "villa": 0.15,
    }

    # Reference plot sizes per property type (in m²)
    REFERENCE_PLOT_SIZES = {
        "appartement": 0,
        "tussenwoning": 150,
        "hoekwoning": 200,
        "twee-onder-een-kap": 300,
        "vrijstaand": 500,
        "villa": 800,
    }
    DEFAULT_REFERENCE_PLOT_SIZE = 150

    # Default market overbidding percentage
    DEFAULT_OVERBID_PERCENTAGE = 0.05

    # Regional fallback prices (per m²)
    REGIONAL_M2_PRICES = {
        "0518": 5200.0,  # Den Haag
        "1916": 4800.0,  # Leidschendam-Voorburg
        "0603": 4600.0,  # Rijswijk
    }
    DEFAULT_M2_PRICE = 4500.0

    def __init__(self, db: Optional[Session] = None, load_prices: bool = True):
        self.db = db
        self._market_overbid: float = self.DEFAULT_OVERBID_PERCENTAGE

    def _fetch_buurt(self, buurt_code: str) -> None:
        """Fetch a single buurt from DB into the module-level cache."""
        if not self.db:
            return
        try:
            buurt = self.db.query(Buurt).filter(Buurt.code == buurt_code).first()
            _buurt_cache[buurt_code] = (
                buurt.median_m2_prijs if buurt else None,
                buurt.score_totaal if buurt else None,
                buurt.gemeente_code if buurt else None,
                datetime.now(),
            )
        except Exception:
            _buurt_cache[buurt_code] = (None, None, None, datetime.now())

    def _fetch_gemeente(self, gemeente_code: str) -> None:
        """Compute gemeente average m2 price from DB into the module-level cache."""
        if not self.db:
            return
        try:
            buurten = (
                self.db.query(Buurt)
                .filter(Buurt.gemeente_code == gemeente_code, Buurt.median_m2_prijs.isnot(None))
                .all()
            )
            avg = sum(b.median_m2_prijs for b in buurten) / len(buurten) if buurten else None
            _gemeente_cache[gemeente_code] = (avg, datetime.now())
        except Exception:
            _gemeente_cache[gemeente_code] = (None, datetime.now())

    def _buurt_cached(self, buurt_code: str) -> bool:
        entry = _buurt_cache.get(buurt_code)
        return entry is not None and datetime.now() - entry[3] < _PRICE_CACHE_TTL

    def _gemeente_cached(self, gemeente_code: str) -> bool:
        entry = _gemeente_cache.get(gemeente_code)
        return entry is not None and datetime.now() - entry[1] < _PRICE_CACHE_TTL

    def get_buurt_score(self, buurt_code: Optional[str]) -> Optional[float]:
        """Return leefbaarheids-score for a buurt (0-1), or None."""
        if not buurt_code:
            return None
        if not self._buurt_cached(buurt_code):
            self._fetch_buurt(buurt_code)
        entry = _buurt_cache.get(buurt_code)
        return entry[1] if entry else None

    def get_buurt_m2_price(self, buurt_code: Optional[str]) -> tuple[float, str]:
        """Return (m2_price, source) for the given buurt_code.

        Source is one of: "buurt", "gemeente", "regio", "default".
        Uses targeted DB queries with module-level TTL cache — no bulk load.
        """
        if not buurt_code:
            return self.DEFAULT_M2_PRICE, "default"

        # ── buurt-level lookup ──
        if not self._buurt_cached(buurt_code):
            self._fetch_buurt(buurt_code)

        entry = _buurt_cache.get(buurt_code)
        if entry and entry[0] is not None:
            return entry[0], "buurt"

        # ── gemeente-level fallback ──
        gemeente_code = buurt_code[:4] if len(buurt_code) >= 4 else None
        if gemeente_code:
            if not self._gemeente_cached(gemeente_code):
                self._fetch_gemeente(gemeente_code)
            g_entry = _gemeente_cache.get(gemeente_code)
            if g_entry and g_entry[0] is not None:
                return g_entry[0], "gemeente"

            # ── regional hardcoded fallback ──
            for gm_code, price in self.REGIONAL_M2_PRICES.items():
                if buurt_code.startswith(gm_code):
                    return price, "regio"

        return self.DEFAULT_M2_PRICE, "default"

    def set_buurt_m2_prices(self, prices: Dict[str, float]) -> None:
        """Override neighborhood m2 prices (populates module-level cache)."""
        now = datetime.now()
        for code, price in prices.items():
            existing = _buurt_cache.get(code)
            score = existing[1] if existing else None
            gemeente = existing[2] if existing else None
            _buurt_cache[code] = (price, score, gemeente, now)

    def set_market_overbid(self, percentage: float) -> None:
        """Set current market overbidding percentage."""
        self._market_overbid = percentage

    def get_energy_correction(self, label: Optional[str]) -> float:
        if not label:
            return 0.0
        normalized = label.upper().strip()
        return self.ENERGY_CORRECTIONS.get(normalized, 0.0)

    def get_build_year_correction(self, year: Optional[int]) -> float:
        if not year:
            return 0.0
        for (start, end), correction in self.BUILD_YEAR_CORRECTIONS.items():
            if start <= year <= end:
                return correction
        return 0.0

    def get_type_correction(self, property_type: Optional[str]) -> float:
        if not property_type:
            return 0.0
        normalized = property_type.lower().strip()
        for key, correction in self.TYPE_CORRECTIONS.items():
            if key in normalized:
                return correction
        return 0.0

    def get_perceel_correction(
        self,
        grondoppervlakte: Optional[int],
        woningtype: Optional[str] = None,
    ) -> float:
        if not grondoppervlakte or grondoppervlakte <= 0:
            return 0.0

        reference = self.DEFAULT_REFERENCE_PLOT_SIZE
        if woningtype:
            normalized = woningtype.lower().strip()
            for key, ref_size in self.REFERENCE_PLOT_SIZES.items():
                if key in normalized:
                    reference = ref_size
                    break

        if reference == 0:
            return 0.0

        difference = grondoppervlakte - reference

        if difference > 0:
            correction = (difference / 50) * 0.05
            return min(correction, 0.15)
        else:
            correction = (difference / 50) * 0.03
            return max(correction, -0.10)

    def get_buurt_quality_correction(self, buurt_code: Optional[str]) -> float:
        """
        Get neighborhood quality correction factor based on score_totaal.

        Returns correction between -0.06 and +0.08.
        """
        if not buurt_code:
            return 0.0

        score = self.get_buurt_score(buurt_code)
        if score is None:
            return 0.0

        for low, high, correction in BUURT_QUALITY_CORRECTIONS:
            if low <= score <= high:
                return correction

        return 0.0

    def estimate_value(
        self,
        woonoppervlakte: int,
        buurt_code: Optional[str] = None,
        buurt_m2_prijs: Optional[float] = None,
        energielabel: Optional[str] = None,
        bouwjaar: Optional[int] = None,
        woningtype: Optional[str] = None,
        vraagprijs: Optional[int] = None,
        comparables: Optional[List[Dict[str, Any]]] = None,
        grondoppervlakte: Optional[int] = None,
    ) -> ValuationResult:
        """
        Estimate the market value of a property.

        Parameters
        ----------
        woonoppervlakte : int
            Living area in m2
        buurt_code : str, optional
            Neighborhood code for m2 price lookup
        buurt_m2_prijs : float, optional
            Override m2 price (used if buurt_code not available)
        energielabel : str, optional
            Energy efficiency label (A-G)
        bouwjaar : int, optional
            Year of construction
        woningtype : str, optional
            Property type (appartement, tussenwoning, etc.)
        vraagprijs : int, optional
            Current asking price for comparison
        comparables : list, optional
            List of comparable transactions for additional validation
        grondoppervlakte : int, optional
            Plot area in m2

        Returns
        -------
        ValuationResult
            Detailed valuation with breakdown and advice
        """
        # Determine m2 price with source tracking
        price_source = "default"
        if buurt_m2_prijs is not None:
            price_source = "manual"
        elif buurt_code:
            buurt_m2_prijs, price_source = self.get_buurt_m2_price(buurt_code)
        else:
            buurt_m2_prijs = self.DEFAULT_M2_PRICE

        # If we have comparables, use their average m² price as validation
        comparables_m2_price = None
        if comparables:
            comp_prices = [
                c.get("prijs_per_m2") for c in comparables
                if c.get("prijs_per_m2")
            ]
            if comp_prices:
                comparables_m2_price = sum(comp_prices) / len(comp_prices)

        # Blend buurt price with comparables if available (70% buurt, 30% comparables)
        if comparables_m2_price and price_source != "manual":
            buurt_m2_prijs = buurt_m2_prijs * 0.7 + comparables_m2_price * 0.3

        # Calculate base value
        basis_waarde = int(buurt_m2_prijs * woonoppervlakte)

        # Calculate corrections
        energy_factor = self.get_energy_correction(energielabel)
        energy_correctie = int(basis_waarde * energy_factor)

        year_factor = self.get_build_year_correction(bouwjaar)
        year_correctie = int(basis_waarde * year_factor)

        type_factor = self.get_type_correction(woningtype)
        type_correctie = int(basis_waarde * type_factor)

        perceel_factor = self.get_perceel_correction(grondoppervlakte, woningtype)
        perceel_correctie = int(basis_waarde * perceel_factor)

        buurt_quality_factor = self.get_buurt_quality_correction(buurt_code)
        buurt_kwaliteit_correctie = int(basis_waarde * buurt_quality_factor)

        market_correctie = int(basis_waarde * self._market_overbid)

        # Calculate estimated value
        waarde_midden = (
            basis_waarde
            + energy_correctie
            + year_correctie
            + type_correctie
            + perceel_correctie
            + buurt_kwaliteit_correctie
            + market_correctie
        )

        # Value range (±10%, narrower if we have good data)
        range_factor = 0.10
        if price_source == "buurt":
            range_factor = 0.08
        if comparables_m2_price:
            range_factor = 0.07

        waarde_laag = int(waarde_midden * (1 - range_factor))
        waarde_hoog = int(waarde_midden * (1 + range_factor))

        # Compare with asking price
        verschil_percentage = None
        if vraagprijs:
            verschil_percentage = (vraagprijs - waarde_midden) / waarde_midden * 100

        # Determine bidding advice
        bied_advies, bied_laag, bied_hoog = self._calculate_bid_advice(
            waarde_midden, waarde_laag, waarde_hoog, vraagprijs
        )

        # Calculate confidence
        has_buurt_data = price_source in ("buurt", "gemeente")
        has_buurt_scores = buurt_code is not None and self.get_buurt_score(buurt_code) is not None
        confidence, confidence_factors = self._calculate_confidence(
            buurt_m2_prijs=buurt_m2_prijs,
            energielabel=energielabel,
            bouwjaar=bouwjaar,
            woningtype=woningtype,
            has_buurt_data=has_buurt_data,
            has_comparables=bool(comparables),
            has_buurt_scores=has_buurt_scores,
            price_source=price_source,
        )

        return ValuationResult(
            waarde_laag=waarde_laag,
            waarde_hoog=waarde_hoog,
            waarde_midden=waarde_midden,
            vraagprijs=vraagprijs,
            verschil_percentage=verschil_percentage,
            bied_advies=bied_advies,
            bied_range_laag=bied_laag,
            bied_range_hoog=bied_hoog,
            basis_waarde=basis_waarde,
            energielabel_correctie=energy_correctie,
            bouwjaar_correctie=year_correctie,
            woningtype_correctie=type_correctie,
            perceel_correctie=perceel_correctie,
            buurt_kwaliteit_correctie=buurt_kwaliteit_correctie,
            markt_correctie=market_correctie,
            confidence=confidence,
            confidence_factors=confidence_factors,
            vergelijkbare_woningen=comparables or [],
        )

    def _calculate_bid_advice(
        self,
        waarde_midden: int,
        waarde_laag: int,
        waarde_hoog: int,
        vraagprijs: Optional[int],
    ) -> tuple[BiedAdvies, int, int]:
        """Calculate bidding advice and range."""
        if not vraagprijs:
            return BiedAdvies.VRAAGPRIJS, waarde_laag, waarde_hoog

        ratio = vraagprijs / waarde_midden

        if ratio > 1.10:
            advies = BiedAdvies.ONDER_VRAAGPRIJS
            bied_laag = waarde_laag
            bied_hoog = int(vraagprijs * 0.95)
        elif ratio > 1.02:
            advies = BiedAdvies.VRAAGPRIJS
            bied_laag = int(vraagprijs * 0.98)
            bied_hoog = vraagprijs
        elif ratio > 0.95:
            advies = BiedAdvies.LICHT_BOVEN
            bied_laag = vraagprijs
            bied_hoog = int(vraagprijs * 1.03)
        else:
            advies = BiedAdvies.BOVEN_VRAAGPRIJS
            bied_laag = vraagprijs
            bied_hoog = int(waarde_midden * 1.05)

        return advies, bied_laag, bied_hoog

    def _calculate_confidence(
        self,
        buurt_m2_prijs: Optional[float],
        energielabel: Optional[str],
        bouwjaar: Optional[int],
        woningtype: Optional[str],
        has_buurt_data: bool,
        has_comparables: bool = False,
        has_buurt_scores: bool = False,
        price_source: str = "default",
    ) -> tuple[float, Dict[str, Any]]:
        """Calculate confidence score based on data completeness."""
        factors: Dict[str, Any] = {
            "buurt_m2_prijs": buurt_m2_prijs is not None,
            "buurt_data": has_buurt_data,
            "energielabel": energielabel is not None,
            "bouwjaar": bouwjaar is not None,
            "woningtype": woningtype is not None,
            "comparables": has_comparables,
            "buurt_scores": has_buurt_scores,
            "price_source": price_source,
        }

        weights = {
            "buurt_m2_prijs": 0.18,
            "buurt_data": 0.18,
            "energielabel": 0.14,
            "bouwjaar": 0.08,
            "woningtype": 0.08,
            "comparables": 0.24,
            "buurt_scores": 0.10,
        }

        confidence = sum(
            weights[k] for k, v in factors.items()
            if k in weights and v is True
        )

        source_bonus = {
            "buurt": 0.10,
            "gemeente": 0.05,
            "regio": 0.02,
            "manual": 0.08,
            "default": 0.00,
        }
        confidence += source_bonus.get(price_source, 0)

        confidence = min(confidence, 1.0)

        return confidence, factors

    def find_comparables(
        self,
        buurt_code: str,
        woonoppervlakte: int,
        woningtype: Optional[str] = None,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """Find comparable recently sold properties."""
        return []
