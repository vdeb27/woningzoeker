"""
Kadaster transaction history collector.

Fetches historical property transactions and comparable sales.
Uses publicly available Kadaster data sources.

Data sources:
- OpenKadaster (openkadaster.com) - community-driven transaction data
- Kadaster Open Data - official open datasets
- PDOK - for address/parcel linking
"""

from __future__ import annotations

import json
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


@dataclass
class TransactionRecord:
    """A single property transaction."""

    postcode: str
    huisnummer: int
    straat: Optional[str] = None
    woonplaats: Optional[str] = None
    transactie_datum: Optional[str] = None
    transactie_prijs: Optional[int] = None
    koopsom: Optional[int] = None  # Alternative name for transaction price
    oppervlakte: Optional[int] = None
    prijs_per_m2: Optional[float] = None
    bouwjaar: Optional[int] = None
    woningtype: Optional[str] = None
    perceeloppervlakte: Optional[int] = None
    kadastrale_aanduiding: Optional[str] = None  # Cadastral designation
    koopjaar: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        price = self.transactie_prijs or self.koopsom
        return {
            "postcode": self.postcode,
            "huisnummer": self.huisnummer,
            "straat": self.straat,
            "woonplaats": self.woonplaats,
            "transactie_datum": self.transactie_datum,
            "transactie_prijs": price,
            "oppervlakte": self.oppervlakte,
            "prijs_per_m2": self.prijs_per_m2,
            "bouwjaar": self.bouwjaar,
            "woningtype": self.woningtype,
            "perceeloppervlakte": self.perceeloppervlakte,
            "kadastrale_aanduiding": self.kadastrale_aanduiding,
            "koopjaar": self.koopjaar,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TransactionRecord":
        """Create from dictionary."""
        return cls(
            postcode=data.get("postcode", ""),
            huisnummer=data.get("huisnummer", 0),
            straat=data.get("straat"),
            woonplaats=data.get("woonplaats"),
            transactie_datum=data.get("transactie_datum"),
            transactie_prijs=data.get("transactie_prijs") or data.get("koopsom"),
            koopsom=data.get("koopsom"),
            oppervlakte=data.get("oppervlakte"),
            prijs_per_m2=data.get("prijs_per_m2"),
            bouwjaar=data.get("bouwjaar"),
            woningtype=data.get("woningtype"),
            perceeloppervlakte=data.get("perceeloppervlakte"),
            kadastrale_aanduiding=data.get("kadastrale_aanduiding"),
            koopjaar=data.get("koopjaar"),
        )


@dataclass
class ComparablesResult:
    """Result from comparables search."""

    target_postcode: str
    target_huisnummer: int
    target_address: Optional[str] = None
    transactions: List[TransactionRecord] = field(default_factory=list)
    fetch_date: datetime = field(default_factory=datetime.now)
    search_radius_pc4: bool = True  # Whether searched within PC4 area
    min_oppervlakte: Optional[int] = None
    max_oppervlakte: Optional[int] = None
    max_years: int = 2
    source: str = "openkadaster"
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "target_postcode": self.target_postcode,
            "target_huisnummer": self.target_huisnummer,
            "target_address": self.target_address,
            "transactions": [t.to_dict() for t in self.transactions],
            "fetch_date": self.fetch_date.isoformat(),
            "search_radius_pc4": self.search_radius_pc4,
            "min_oppervlakte": self.min_oppervlakte,
            "max_oppervlakte": self.max_oppervlakte,
            "max_years": self.max_years,
            "source": self.source,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ComparablesResult":
        """Create from dictionary."""
        fetch_date = data.get("fetch_date")
        if isinstance(fetch_date, str):
            fetch_date = datetime.fromisoformat(fetch_date)
        elif fetch_date is None:
            fetch_date = datetime.now()

        transactions = [
            TransactionRecord.from_dict(t)
            for t in data.get("transactions", [])
        ]

        return cls(
            target_postcode=data.get("target_postcode", ""),
            target_huisnummer=data.get("target_huisnummer", 0),
            target_address=data.get("target_address"),
            transactions=transactions,
            fetch_date=fetch_date,
            search_radius_pc4=data.get("search_radius_pc4", True),
            min_oppervlakte=data.get("min_oppervlakte"),
            max_oppervlakte=data.get("max_oppervlakte"),
            max_years=data.get("max_years", 2),
            source=data.get("source", "kadaster"),
            error=data.get("error"),
        )

    @property
    def count(self) -> int:
        """Number of comparable transactions found."""
        return len(self.transactions)

    @property
    def avg_prijs_per_m2(self) -> Optional[float]:
        """Average price per m² across comparables."""
        prices = [t.prijs_per_m2 for t in self.transactions if t.prijs_per_m2]
        return sum(prices) / len(prices) if prices else None


@dataclass
class KadasterCollector:
    """
    Collector for Kadaster transaction data.

    Fetches historical transaction prices and comparable sales
    from publicly available sources.

    Parameters
    ----------
    min_delay : float
        Minimum delay between requests in seconds
    max_delay : float
        Maximum delay between requests in seconds
    cache_dir : Path, optional
        Directory for caching results
    cache_days : int
        Number of days to cache results
    """

    min_delay: float = 2.0
    max_delay: float = 4.0
    cache_dir: Optional[Path] = None
    cache_days: int = 7  # Cache for a week
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    max_retries: int = 3

    # API endpoints
    PDOK_API_URL = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"
    OPENKADASTER_URL = "https://openkadaster.com/transactions"
    CBS_ODATA_URL = "https://opendata.cbs.nl/ODataApi/odata"

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json",
            "Accept-Language": "nl-NL,nl;q=0.9",
        }

    def _rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        now = time.perf_counter()
        elapsed = now - self._last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.perf_counter()

    def _get_cache_path(self, cache_key: str) -> Optional[Path]:
        """Get cache file path."""
        if not self.cache_dir:
            return None
        return self.cache_dir / f"kadaster_{cache_key}.json"

    def _load_from_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Load cached result if valid."""
        cache_path = self._get_cache_path(cache_key)
        if not cache_path or not cache_path.exists():
            return None

        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            fetch_date = data.get("fetch_date")
            if isinstance(fetch_date, str):
                fetch_date = datetime.fromisoformat(fetch_date)
                if datetime.now() - fetch_date > timedelta(days=self.cache_days):
                    return None

            return data

        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    def _save_to_cache(self, cache_key: str, data: Dict[str, Any]) -> None:
        """Save result to cache."""
        cache_path = self._get_cache_path(cache_key)
        if not cache_path:
            return

        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except IOError:
            pass

    def _get_address_info(
        self,
        postcode: str,
        huisnummer: int,
    ) -> Optional[Dict[str, Any]]:
        """
        Get address information from PDOK.

        Returns address details including coordinates and identifiers.
        """
        pc = postcode.replace(" ", "")
        query = f"{pc} {huisnummer}"

        params = {
            "q": query,
            "fq": "type:adres",
            "rows": 1,
            "fl": "*",  # All fields
        }

        try:
            self._rate_limit()
            response = self.session.get(
                self.PDOK_API_URL,
                params=params,
                headers=self._get_headers(),
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            docs = data.get("response", {}).get("docs", [])
            return docs[0] if docs else None

        except requests.RequestException:
            return None

    def _fetch_page(self, url: str, params: Optional[Dict[str, str]] = None) -> Optional[str]:
        """
        Fetch a page with retry logic and rate limiting.

        Returns the HTML content or None on failure.
        """
        headers = self._get_headers()
        headers["Accept"] = "text/html,application/xhtml+xml"

        for attempt in range(self.max_retries):
            self._rate_limit()
            try:
                response = self.session.get(
                    url, params=params, headers=headers, timeout=30
                )
                if response.status_code == 429:
                    wait_time = (2 ** attempt) * self.max_delay
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()
                return response.text
            except requests.RequestException:
                if attempt == self.max_retries - 1:
                    return None
                time.sleep(self.max_delay)
        return None

    def _parse_openkadaster_price(self, price_text: str) -> Optional[int]:
        """Parse price string like '€375,000.00' to integer."""
        if not price_text:
            return None
        cleaned = re.sub(r"[€\s]", "", price_text)
        # Format is €375,000.00 (comma as thousands, dot as decimal)
        cleaned = cleaned.replace(",", "").replace(".", "")
        # After removing both separators we have e.g. "37500000" for €375,000.00
        # The last two digits were decimals, so divide by 100
        try:
            return int(cleaned) // 100
        except (ValueError, ZeroDivisionError):
            return None

    def _parse_address(self, address_text: str) -> tuple[Optional[str], Optional[int]]:
        """Parse address like 'Preludeweg 478' into street and house number."""
        if not address_text:
            return None, None
        match = re.match(r"^(.+?)\s+(\d+)\s*.*$", address_text.strip())
        if match:
            return match.group(1), int(match.group(2))
        return address_text.strip(), None

    def _parse_transactions_page(self, html: str) -> List[TransactionRecord]:
        """Parse OpenKadaster transactions page HTML into TransactionRecords."""
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", class_="table")
        if not table:
            return []

        tbody = table.find("tbody")
        if not tbody:
            return []

        transactions = []
        for row in tbody.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            try:
                address_text = cells[0].get_text(strip=True)
                postcode = cells[1].get_text(strip=True).replace(" ", "").upper()
                city = cells[2].get_text(strip=True)
                date_text = cells[3].get_text(strip=True)
                price_text = cells[4].get_text(strip=True)

                straat, huisnummer = self._parse_address(address_text)
                prijs = self._parse_openkadaster_price(price_text)

                if not postcode or huisnummer is None:
                    continue

                koopjaar = None
                if date_text:
                    try:
                        koopjaar = int(date_text[:4])
                    except (ValueError, IndexError):
                        pass

                transactions.append(TransactionRecord(
                    postcode=postcode,
                    huisnummer=huisnummer,
                    straat=straat,
                    woonplaats=city,
                    transactie_datum=date_text,
                    transactie_prijs=prijs,
                    koopsom=prijs,
                    koopjaar=koopjaar,
                ))
            except Exception:
                continue

        return transactions

    def _search_transactions_in_area(
        self,
        pc4: str,
        min_oppervlakte: Optional[int] = None,
        max_oppervlakte: Optional[int] = None,
        max_years: int = 2,
    ) -> List[TransactionRecord]:
        """
        Search for transactions in a PC4 area via OpenKadaster.com.

        Scrapes the OpenKadaster search page for transactions matching
        the given postal code area and filters by date.
        """
        # Calculate date filter
        date_since = (datetime.now() - timedelta(days=max_years * 365)).strftime("%Y-%m-%d")

        params = {
            "query": pc4,
            "date_since": date_since,
            "order_by": "date_desc",
        }

        html = self._fetch_page(self.OPENKADASTER_URL, params=params)
        if not html:
            return []

        return self._parse_transactions_page(html)

    def _fetch_cbs_price_data(
        self,
        gemeente_code: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch CBS housing price statistics for a municipality.

        Uses CBS dataset "83913NED" (Bestaande koopwoningen; gemiddelde verkoopprijs).
        """
        try:
            self._rate_limit()

            # CBS housing prices dataset
            dataset_id = "83913NED"
            url = f"{self.CBS_ODATA_URL}/{dataset_id}/TypedDataSet"

            params = {
                "$filter": f"startswith(RegioS, 'GM{gemeente_code}')",
                "$select": "Perioden,RegioS,GemiddeldeVerkoopprijs_1",
                "$orderby": "Perioden desc",
                "$top": 10,
            }

            response = self.session.get(
                url,
                params=params,
                headers=self._get_headers(),
                timeout=30,
            )

            if response.status_code == 200:
                return response.json()

        except requests.RequestException:
            pass

        return None

    def get_property_history(
        self,
        postcode: str,
        huisnummer: int,
        huisletter: Optional[str] = None,
        toevoeging: Optional[str] = None,
        use_cache: bool = True,
    ) -> List[TransactionRecord]:
        """
        Get transaction history for a specific property.

        Parameters
        ----------
        postcode : str
            Dutch postcode
        huisnummer : int
            House number
        huisletter : str, optional
            House letter
        toevoeging : str, optional
            House number suffix
        use_cache : bool
            Whether to use cached results

        Returns
        -------
        list
            List of TransactionRecord objects for this property
        """
        pc = postcode.replace(" ", "").upper()
        cache_key = f"history_{pc}_{huisnummer}"
        if huisletter:
            cache_key += f"_{huisletter}"
        if toevoeging:
            cache_key += f"_{toevoeging}"

        if use_cache:
            cached = self._load_from_cache(cache_key)
            if cached:
                return [TransactionRecord.from_dict(t) for t in cached.get("transactions", [])]

        # Search OpenKadaster by postcode, then filter to this property
        params = {
            "query": pc,
            "order_by": "date_desc",
        }

        html = self._fetch_page(self.OPENKADASTER_URL, params=params)
        transactions: List[TransactionRecord] = []

        if html:
            all_transactions = self._parse_transactions_page(html)
            # Filter to only this specific property
            for t in all_transactions:
                if t.postcode == pc and t.huisnummer == huisnummer:
                    transactions.append(t)

        if use_cache and transactions:
            self._save_to_cache(cache_key, {
                "transactions": [t.to_dict() for t in transactions],
                "fetch_date": datetime.now().isoformat(),
            })

        return transactions

    def get_comparables(
        self,
        postcode: str,
        huisnummer: int,
        oppervlakte: Optional[int] = None,
        max_years: int = 2,
        max_results: int = 10,
        use_cache: bool = True,
    ) -> ComparablesResult:
        """
        Get comparable sales in the neighborhood.

        Searches for recent transactions with similar characteristics
        in the same postal code area.

        Parameters
        ----------
        postcode : str
            Dutch postcode (PC6)
        huisnummer : int
            House number
        oppervlakte : int, optional
            Target living area in m² (for filtering)
        max_years : int
            Maximum age of transactions in years (default: 2)
        max_results : int
            Maximum number of comparables to return (default: 10)
        use_cache : bool
            Whether to use cached results

        Returns
        -------
        ComparablesResult
            Object containing comparable transactions
        """
        pc = postcode.replace(" ", "").upper()
        pc4 = pc[:4]  # Use PC4 for area search

        # Calculate surface area range (±20%)
        min_opp = int(oppervlakte * 0.8) if oppervlakte else None
        max_opp = int(oppervlakte * 1.2) if oppervlakte else None

        cache_key = f"comparables_{pc4}_{min_opp}_{max_opp}_{max_years}"

        if use_cache:
            cached = self._load_from_cache(cache_key)
            if cached:
                return ComparablesResult.from_dict(cached)

        result = ComparablesResult(
            target_postcode=pc,
            target_huisnummer=huisnummer,
            min_oppervlakte=min_opp,
            max_oppervlakte=max_opp,
            max_years=max_years,
        )

        # Get target address info
        addr_info = self._get_address_info(pc, huisnummer)
        if addr_info:
            result.target_address = addr_info.get("weergavenaam")

        # Search for transactions in the area
        transactions = self._search_transactions_in_area(
            pc4=pc4,
            min_oppervlakte=min_opp,
            max_oppervlakte=max_opp,
            max_years=max_years,
        )

        # Filter and sort by relevance
        # Prioritize: closer postcode, similar surface, more recent
        filtered = []
        for t in transactions:
            # Skip the target property itself
            if t.postcode == pc and t.huisnummer == huisnummer:
                continue

            # Calculate relevance score
            score = 0

            # Same PC6 is better than just PC4
            if t.postcode and t.postcode.startswith(pc):
                score += 10
            elif t.postcode and t.postcode.startswith(pc4):
                score += 5

            # Surface area similarity
            if t.oppervlakte and oppervlakte:
                diff_pct = abs(t.oppervlakte - oppervlakte) / oppervlakte
                if diff_pct < 0.1:
                    score += 5
                elif diff_pct < 0.2:
                    score += 3

            filtered.append((score, t))

        # Sort by score (descending) and take top results
        filtered.sort(key=lambda x: x[0], reverse=True)
        result.transactions = [t for _, t in filtered[:max_results]]

        if use_cache:
            self._save_to_cache(cache_key, result.to_dict())

        return result

    def get_gemeente_prices(
        self,
        gemeente_code: str,
        use_cache: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Get average housing prices for a municipality from CBS.

        Parameters
        ----------
        gemeente_code : str
            Municipality code (e.g., "0518" for Den Haag)
        use_cache : bool
            Whether to use cached results

        Returns
        -------
        dict or None
            Dictionary with price statistics or None if not found
        """
        cache_key = f"gemeente_prices_{gemeente_code}"

        if use_cache:
            cached = self._load_from_cache(cache_key)
            if cached:
                return cached

        cbs_data = self._fetch_cbs_price_data(gemeente_code)

        if cbs_data and "value" in cbs_data:
            records = cbs_data["value"]
            result = {
                "gemeente_code": gemeente_code,
                "prices": [],
                "fetch_date": datetime.now().isoformat(),
            }

            for record in records:
                price = record.get("GemiddeldeVerkoopprijs_1")
                period = record.get("Perioden", "").strip()

                if price and period:
                    result["prices"].append({
                        "period": period,
                        "gemiddelde_verkoopprijs": int(price * 1000),  # CBS uses thousands
                    })

            if use_cache and result["prices"]:
                self._save_to_cache(cache_key, result)

            return result

        return None


def create_kadaster_collector(cache_dir: Optional[Path] = None) -> KadasterCollector:
    """
    Factory function to create a Kadaster collector with default cache directory.

    Parameters
    ----------
    cache_dir : Path, optional
        Cache directory. If None, uses data/cache/kadaster.

    Returns
    -------
    KadasterCollector
        Configured collector instance
    """
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "kadaster"

    return KadasterCollector(cache_dir=cache_dir)
