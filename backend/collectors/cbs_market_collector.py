"""
CBS StatLine Market Data Collector.

Fetches housing market transaction prices and indicators from CBS Open Data:
- Dataset 83625NED: Bestaande koopwoningen; verkoopprijzen (monthly)
- Dataset 83913NED: Bestaande koopwoningen; verkooptijd en prijsontwikkeling (quarterly)

Data is cached for 7 days since CBS updates monthly/quarterly.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


# CBS OData API base URL
CBS_API_BASE = "https://opendata.cbs.nl/ODataApi/odata"

# Datasets
DATASET_PRICES = "83625NED"  # Verkoopprijzen bestaande koopwoningen
DATASET_INDICATORS = "83913NED"  # Woningmarktindicatoren

# Cache settings
CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CACHE_DURATION_SECONDS = 7 * 24 * 60 * 60  # 7 days

# Municipality codes for target region
GEMEENTE_CODES = {
    "'s-Gravenhage": "0518",
    "Den Haag": "0518",
    "Leidschendam-Voorburg": "1916",
    "Rijswijk (ZH)": "0603",
    "Rijswijk": "0603",
}


@dataclass
class MarketDataResult:
    """Market data result for a municipality."""

    gemeente_code: str
    gemeente_naam: str
    gemiddelde_prijs: Optional[int] = None
    prijsindex: Optional[float] = None
    aantal_transacties: Optional[int] = None
    gemiddelde_verkooptijd: Optional[int] = None  # days
    overbiedingspercentage: Optional[float] = None  # percentage above/below asking
    peildatum: Optional[str] = None
    bron: str = "CBS StatLine"
    datasets: List[str] = field(default_factory=list)


@dataclass
class CBSMarketCollector:
    """
    Collector for CBS housing market data.

    Fetches transaction prices and market indicators per municipality.
    Results are cached for 7 days.
    """

    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    _cache: Dict[str, Any] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        """Ensure cache directory exists."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._load_cache()

    def _cache_path(self, dataset_id: str) -> Path:
        """Get cache file path for a dataset."""
        return self.cache_dir / f"cbs_{dataset_id}.json"

    def _load_cache(self) -> None:
        """Load cached data from disk."""
        for dataset_id in [DATASET_PRICES, DATASET_INDICATORS]:
            cache_path = self._cache_path(dataset_id)
            if cache_path.exists():
                try:
                    with cache_path.open("r", encoding="utf-8") as f:
                        data = json.load(f)
                        # Check if cache is still valid
                        if data.get("timestamp", 0) + CACHE_DURATION_SECONDS > time.time():
                            self._cache[dataset_id] = data
                except (json.JSONDecodeError, IOError):
                    pass

    def _save_cache(self, dataset_id: str, data: Dict[str, Any]) -> None:
        """Save data to cache."""
        cache_path = self._cache_path(dataset_id)
        cache_data = {"timestamp": time.time(), "data": data}
        try:
            with cache_path.open("w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            self._cache[dataset_id] = cache_data
        except IOError:
            pass

    def _get_cached(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """Get cached data if available and valid."""
        cached = self._cache.get(dataset_id)
        if cached and cached.get("timestamp", 0) + CACHE_DURATION_SECONDS > time.time():
            return cached.get("data")
        return None

    def _fetch_dataset(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Fetch data from CBS OData API.

        Returns the raw records from the TypedDataSet endpoint.
        """
        # Check cache first
        cached = self._get_cached(dataset_id)
        if cached:
            return cached.get("records", [])

        base_url = f"{CBS_API_BASE}/{dataset_id}/TypedDataSet"
        records = []
        url = base_url

        while url:
            try:
                response = requests.get(url, timeout=60)
                response.raise_for_status()
                data = response.json()
            except requests.RequestException as exc:
                raise RuntimeError(
                    f"Failed to fetch CBS dataset {dataset_id}: {exc}"
                ) from exc

            records.extend(data.get("value", []))
            url = data.get("odata.nextLink") or data.get("@odata.nextLink")

        # Cache the results
        self._save_cache(dataset_id, {"records": records})
        return records

    def _get_latest_period(self, records: List[Dict[str, Any]], period_key: str = "Perioden") -> str:
        """Find the most recent period in the records."""
        periods = set()
        for record in records:
            period = record.get(period_key, "")
            if period:
                periods.add(period)

        if not periods:
            return ""

        # Sort periods (format is typically "2024MM12" or "2024KW04")
        sorted_periods = sorted(periods, reverse=True)
        return sorted_periods[0] if sorted_periods else ""

    def _parse_period_to_date(self, period: str) -> str:
        """Convert CBS period format to readable date."""
        if not period:
            return ""

        # Monthly: "2024MM12" -> "december 2024"
        if "MM" in period:
            year = period[:4]
            month_num = period[-2:]
            months = [
                "", "januari", "februari", "maart", "april", "mei", "juni",
                "juli", "augustus", "september", "oktober", "november", "december"
            ]
            try:
                month = months[int(month_num)]
                return f"{month} {year}"
            except (ValueError, IndexError):
                return period

        # Quarterly: "2024KW04" -> "Q4 2024"
        if "KW" in period:
            year = period[:4]
            quarter = period[-2:]
            return f"Q{quarter} {year}"

        return period

    def _filter_for_gemeente(
        self,
        records: List[Dict[str, Any]],
        gemeente_naam: str,
        region_key: str = "RegioS",
    ) -> List[Dict[str, Any]]:
        """Filter records for a specific municipality."""
        filtered = []
        target_lower = gemeente_naam.lower()

        for record in records:
            region = str(record.get(region_key, "")).strip()
            if target_lower in region.lower():
                filtered.append(record)

        return filtered

    def get_transaction_prices(
        self,
        gemeente_naam: str,
    ) -> Dict[str, Any]:
        """
        Get average transaction prices for a municipality.

        Fetches from CBS dataset 83625NED (Bestaande koopwoningen; verkoopprijzen).

        Returns dict with:
        - gemiddelde_prijs: Average transaction price
        - prijsindex: Price index (2015=100)
        - aantal_transacties: Number of transactions
        - peildatum: Reference date
        """
        try:
            records = self._fetch_dataset(DATASET_PRICES)
        except RuntimeError:
            return {}

        if not records:
            return {}

        # Find the latest period
        latest_period = self._get_latest_period(records, "Perioden")

        # Filter for the gemeente and latest period
        gemeente_records = self._filter_for_gemeente(records, gemeente_naam, "RegioS")
        period_records = [
            r for r in gemeente_records
            if r.get("Perioden", "") == latest_period
        ]

        if not period_records:
            # Fallback to any records for this gemeente
            if gemeente_records:
                period_records = gemeente_records[-1:]

        if not period_records:
            return {}

        record = period_records[0]

        # Extract price data (CBS uses various column names)
        gemiddelde_prijs = None
        prijsindex = None
        aantal = None

        # Common column names for average price
        for key in ["GemiddeldeVerkoopprijs_1", "GemiddeldeVerkoopprijs", "Verkoopprijs_1"]:
            val = record.get(key)
            if val is not None:
                try:
                    gemiddelde_prijs = int(float(val) * 1000) if val < 10000 else int(val)
                except (ValueError, TypeError):
                    pass
                break

        # Price index
        for key in ["Prijsindex_2", "Prijsindex", "PrijsindexBestaandeKoopwoningen_2"]:
            val = record.get(key)
            if val is not None:
                try:
                    prijsindex = float(val)
                except (ValueError, TypeError):
                    pass
                break

        # Number of transactions
        for key in ["AantalVerkopen_3", "AantalVerkopen", "Aantal_3"]:
            val = record.get(key)
            if val is not None:
                try:
                    aantal = int(val)
                except (ValueError, TypeError):
                    pass
                break

        return {
            "gemiddelde_prijs": gemiddelde_prijs,
            "prijsindex": prijsindex,
            "aantal_transacties": aantal,
            "peildatum": self._parse_period_to_date(latest_period),
            "periode_code": latest_period,
        }

    def get_market_indicators(
        self,
        gemeente_naam: str,
    ) -> Dict[str, Any]:
        """
        Get market indicators for a municipality.

        Fetches from CBS dataset 83913NED (verkooptijd en prijsontwikkeling).

        Returns dict with:
        - gemiddelde_verkooptijd: Average days on market
        - overbiedingspercentage: Average % above/below asking price
        - peildatum: Reference date
        """
        try:
            records = self._fetch_dataset(DATASET_INDICATORS)
        except RuntimeError:
            return {}

        if not records:
            return {}

        # Find the latest period
        latest_period = self._get_latest_period(records, "Perioden")

        # Filter for the gemeente and latest period
        gemeente_records = self._filter_for_gemeente(records, gemeente_naam, "RegioS")
        period_records = [
            r for r in gemeente_records
            if r.get("Perioden", "") == latest_period
        ]

        if not period_records:
            if gemeente_records:
                period_records = gemeente_records[-1:]

        if not period_records:
            return {}

        record = period_records[0]

        # Extract indicators
        verkooptijd = None
        overbied_pct = None

        # Average time to sell (days)
        for key in ["GemiddeldeVerkooptijd_1", "GemiddeldeLooptijd_1", "Verkooptijd_1"]:
            val = record.get(key)
            if val is not None:
                try:
                    verkooptijd = int(float(val))
                except (ValueError, TypeError):
                    pass
                break

        # Overbidding percentage (difference from asking price)
        for key in ["VerschilTovVraagprijs_2", "VerschilMetVraagprijs_2", "Overbieden_2"]:
            val = record.get(key)
            if val is not None:
                try:
                    overbied_pct = float(val)
                except (ValueError, TypeError):
                    pass
                break

        return {
            "gemiddelde_verkooptijd": verkooptijd,
            "overbiedingspercentage": overbied_pct,
            "peildatum": self._parse_period_to_date(latest_period),
            "periode_code": latest_period,
        }

    def get_market_data(self, gemeente_naam: str) -> MarketDataResult:
        """
        Get comprehensive market data for a municipality.

        Combines transaction prices and market indicators.
        """
        gemeente_code = GEMEENTE_CODES.get(gemeente_naam, "")

        # Fetch both datasets
        prices = self.get_transaction_prices(gemeente_naam)
        indicators = self.get_market_indicators(gemeente_naam)

        # Combine peildatum (use most recent)
        peildatum = prices.get("peildatum") or indicators.get("peildatum")

        # Track which datasets provided data
        datasets = []
        if prices:
            datasets.append(DATASET_PRICES)
        if indicators:
            datasets.append(DATASET_INDICATORS)

        return MarketDataResult(
            gemeente_code=gemeente_code,
            gemeente_naam=gemeente_naam,
            gemiddelde_prijs=prices.get("gemiddelde_prijs"),
            prijsindex=prices.get("prijsindex"),
            aantal_transacties=prices.get("aantal_transacties"),
            gemiddelde_verkooptijd=indicators.get("gemiddelde_verkooptijd"),
            overbiedingspercentage=indicators.get("overbiedingspercentage"),
            peildatum=peildatum,
            bron="CBS StatLine",
            datasets=datasets,
        )

    def get_regional_market_data(self) -> List[MarketDataResult]:
        """Get market data for all target municipalities."""
        results = []
        seen_codes = set()

        for gemeente_naam in GEMEENTE_CODES.keys():
            code = GEMEENTE_CODES[gemeente_naam]
            if code in seen_codes:
                continue
            seen_codes.add(code)

            try:
                result = self.get_market_data(gemeente_naam)
                results.append(result)
            except Exception:
                pass

        return results

    def get_overbid_percentage(self, gemeente_naam: str) -> Optional[float]:
        """
        Get the overbidding percentage for a municipality.

        Returns the percentage as a decimal (e.g., 0.05 for 5%).
        Returns None if data is not available.
        """
        indicators = self.get_market_indicators(gemeente_naam)
        pct = indicators.get("overbiedingspercentage")
        if pct is not None:
            # CBS provides as percentage, convert to decimal
            return pct / 100.0
        return None


def create_cbs_market_collector() -> CBSMarketCollector:
    """Create a CBS market collector instance."""
    return CBSMarketCollector()
