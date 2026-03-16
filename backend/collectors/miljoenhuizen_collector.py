"""
Miljoenhuizen.nl property data collector.

Scrapes historical asking prices and property characteristics from Miljoenhuizen.nl.
This data supplements CBS neighborhood-level data with address-specific information.

Note: For personal, non-commercial use only. Rate limiting is enforced.
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
class PrijsHistorieEntry:
    """A single price history entry."""

    datum: str  # DD-MM-YYYY
    actie: str  # "te koop", "veranderd", "verkocht"
    prijs: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "datum": self.datum,
            "actie": self.actie,
            "prijs": self.prijs,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PrijsHistorieEntry":
        return cls(
            datum=data.get("datum", ""),
            actie=data.get("actie", ""),
            prijs=data.get("prijs"),
        )


@dataclass
class MiljoenhuizenWoning:
    """Represents a property from Miljoenhuizen.nl."""

    # Basis
    url: str
    adres: str
    postcode: str
    plaats: str

    # Prijzen
    laatste_vraagprijs: Optional[int] = None
    geschatte_waarde_laag: Optional[int] = None
    geschatte_waarde_hoog: Optional[int] = None

    # Status
    status: str = "te_koop"  # "te_koop" or "verkocht"
    verkoopdatum: Optional[str] = None
    status_datum: Optional[str] = None  # Date shown with status

    # Kenmerken
    woningtype: Optional[str] = None
    bouwjaar: Optional[int] = None
    woonoppervlakte: Optional[int] = None
    perceeloppervlakte: Optional[int] = None
    inhoud: Optional[int] = None
    slaapkamers: Optional[int] = None

    # Prijshistorie
    prijshistorie: List[PrijsHistorieEntry] = field(default_factory=list)

    # Metadata
    bron: str = "Miljoenhuizen.nl"
    scraped_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "adres": self.adres,
            "postcode": self.postcode,
            "plaats": self.plaats,
            "laatste_vraagprijs": self.laatste_vraagprijs,
            "geschatte_waarde_laag": self.geschatte_waarde_laag,
            "geschatte_waarde_hoog": self.geschatte_waarde_hoog,
            "status": self.status,
            "verkoopdatum": self.verkoopdatum,
            "status_datum": self.status_datum,
            "woningtype": self.woningtype,
            "bouwjaar": self.bouwjaar,
            "woonoppervlakte": self.woonoppervlakte,
            "perceeloppervlakte": self.perceeloppervlakte,
            "inhoud": self.inhoud,
            "slaapkamers": self.slaapkamers,
            "prijshistorie": [p.to_dict() for p in self.prijshistorie],
            "bron": self.bron,
            "scraped_at": self.scraped_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MiljoenhuizenWoning":
        scraped_at = data.get("scraped_at")
        if isinstance(scraped_at, str):
            scraped_at = datetime.fromisoformat(scraped_at)
        elif scraped_at is None:
            scraped_at = datetime.now()

        prijshistorie = [
            PrijsHistorieEntry.from_dict(p)
            for p in data.get("prijshistorie", [])
        ]

        return cls(
            url=data.get("url", ""),
            adres=data.get("adres", ""),
            postcode=data.get("postcode", ""),
            plaats=data.get("plaats", ""),
            laatste_vraagprijs=data.get("laatste_vraagprijs"),
            geschatte_waarde_laag=data.get("geschatte_waarde_laag"),
            geschatte_waarde_hoog=data.get("geschatte_waarde_hoog"),
            status=data.get("status", "te_koop"),
            verkoopdatum=data.get("verkoopdatum"),
            status_datum=data.get("status_datum"),
            woningtype=data.get("woningtype"),
            bouwjaar=data.get("bouwjaar"),
            woonoppervlakte=data.get("woonoppervlakte"),
            perceeloppervlakte=data.get("perceeloppervlakte"),
            inhoud=data.get("inhoud"),
            slaapkamers=data.get("slaapkamers"),
            prijshistorie=prijshistorie,
            bron=data.get("bron", "Miljoenhuizen.nl"),
            scraped_at=scraped_at,
        )

    @property
    def huisnummer(self) -> Optional[int]:
        """Extract house number from address."""
        if not self.adres:
            return None
        match = re.search(r"(\d+)", self.adres)
        return int(match.group(1)) if match else None

    @property
    def prijs_per_m2(self) -> Optional[float]:
        """Calculate price per m2 if data available."""
        if self.laatste_vraagprijs and self.woonoppervlakte:
            return self.laatste_vraagprijs / self.woonoppervlakte
        return None


@dataclass
class MiljoenhuizenCollector:
    """
    Collector for property data from Miljoenhuizen.nl.

    Scrapes overview pages and detail pages with respectful rate limiting.
    Uses caching to minimize requests.

    Parameters
    ----------
    min_delay : float
        Minimum delay between requests in seconds (default: 2.0)
    max_delay : float
        Maximum delay between requests in seconds (default: 3.0)
    cache_dir : Path, optional
        Directory for caching results
    overview_cache_days : int
        Days to cache overview pages (default: 1, new listings)
    detail_cache_days : int
        Days to cache detail pages (default: 30, historical data)
    max_retries : int
        Maximum retry attempts on rate limiting (default: 3)
    """

    min_delay: float = 2.0
    max_delay: float = 3.0
    cache_dir: Optional[Path] = None
    overview_cache_days: int = 1
    detail_cache_days: int = 30
    max_retries: int = 3
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    BASE_URL = "https://www.miljoenhuizen.nl"
    PLAATSEN = ["den-haag", "rijswijk", "voorburg", "leidschendam"]

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
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
        # Sanitize cache key for filesystem
        safe_key = re.sub(r"[^\w\-_]", "_", cache_key)
        return self.cache_dir / f"miljoenhuizen_{safe_key}.json"

    def _load_from_cache(
        self, cache_key: str, max_age_days: int
    ) -> Optional[Dict[str, Any]]:
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
                if datetime.now() - fetch_date > timedelta(days=max_age_days):
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
            data["fetch_date"] = datetime.now().isoformat()
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except IOError:
            pass

    def _fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch a page with rate limiting and retries.

        Returns the HTML content or None on failure.
        """
        for attempt in range(self.max_retries):
            self._rate_limit()

            try:
                response = self.session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=30,
                )

                if response.status_code == 429:
                    # Rate limited - exponential backoff
                    wait_time = (2 ** attempt) * self.max_delay
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                return response.text

            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    return None
                time.sleep(self.max_delay)

        return None

    def _parse_price(self, price_str: str) -> Optional[int]:
        """Parse price string containing '€ 729.000' to integer."""
        if not price_str:
            return None

        # First, try to find a price pattern like "€ 729.000" or "€729.000"
        match = re.search(r"€\s*([\d.]+)", price_str)
        if match:
            # Remove thousands separators (dots)
            cleaned = match.group(1).replace(".", "")
            try:
                return int(cleaned)
            except ValueError:
                pass

        # Fallback: try to parse as plain number with dots as thousands separator
        cleaned = re.sub(r"[€\s]", "", price_str)
        # If it looks like a price (digits and dots only), parse it
        if re.match(r"^[\d.]+$", cleaned):
            try:
                return int(cleaned.replace(".", ""))
            except ValueError:
                pass

        return None

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string like '14-03-26' to 'DD-MM-YYYY'."""
        if not date_str:
            return None

        # Try patterns like "14-03-'26" or "14-03-26"
        match = re.search(r"(\d{1,2})-(\d{1,2})-'?(\d{2,4})", date_str)
        if match:
            day, month, year = match.groups()
            if len(year) == 2:
                # Assume 2000s for 2-digit years
                year = f"20{year}"
            return f"{day.zfill(2)}-{month.zfill(2)}-{year}"

        return None

    def scrape_overzicht(
        self, plaats: str, page: int = 1, use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Parse overview page, return list of properties.

        Parameters
        ----------
        plaats : str
            City name (e.g., 'den-haag', 'voorburg')
        page : int
            Page number (default: 1)
        use_cache : bool
            Whether to use cached results (default: True)

        Returns
        -------
        list
            List of dicts with {url, adres, plaats, prijs, status, status_datum}
        """
        cache_key = f"overzicht_{plaats}_p{page}"

        if use_cache:
            cached = self._load_from_cache(cache_key, self.overview_cache_days)
            if cached:
                return cached.get("woningen", [])

        url = f"{self.BASE_URL}/{plaats}"
        if page > 1:
            url += f"?page={page}"

        html = self._fetch_page(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        woningen = []

        # Find the house list table
        house_list = soup.select_one("#city-house-list table tbody")
        if not house_list:
            return []

        for row in house_list.select("tr"):
            cells = row.select("td")
            if len(cells) < 4:
                continue

            try:
                # Extract link and address
                link = row.select_one("a.btn-link-primary")
                if not link:
                    continue

                href = link.get("href", "")
                adres = link.get_text(strip=True)

                # Extract plaats from 3rd cell
                plaats_cell = cells[2].get_text(strip=True) if len(cells) > 2 else plaats

                # Extract price from cell with class d-none d-md-table-cell
                prijs_cell = row.select_one("td.d-none.d-md-table-cell")
                prijs = self._parse_price(prijs_cell.get_text(strip=True)) if prijs_cell else None

                # Extract status from last cell (e.g., "Te koop 14-03-'26" or "Verkocht 28-10-'25")
                status_cell = cells[-1].get_text(strip=True) if cells else ""
                status = "te_koop"
                status_datum = None

                if "verkocht" in status_cell.lower():
                    status = "verkocht"
                    status_datum = self._parse_date(status_cell)
                elif "te koop" in status_cell.lower():
                    status = "te_koop"
                    status_datum = self._parse_date(status_cell)

                # Extract postcode from URL: /voorburg/2271ve/straat/nummer
                postcode = ""
                url_parts = href.strip("/").split("/")
                if len(url_parts) >= 2:
                    potential_pc = url_parts[1].upper()
                    if re.match(r"^\d{4}[A-Z]{2}$", potential_pc):
                        postcode = potential_pc

                woning = {
                    "url": f"{self.BASE_URL}{href}" if href.startswith("/") else href,
                    "adres": adres,
                    "plaats": plaats_cell,
                    "postcode": postcode,
                    "prijs": prijs,
                    "status": status,
                    "status_datum": status_datum,
                }
                woningen.append(woning)

            except Exception:
                continue

        if use_cache and woningen:
            self._save_to_cache(cache_key, {"woningen": woningen})

        return woningen

    def scrape_detail(self, url: str, use_cache: bool = True) -> Optional[MiljoenhuizenWoning]:
        """
        Parse detail page for full property data.

        Parameters
        ----------
        url : str
            Full URL to the detail page
        use_cache : bool
            Whether to use cached results (default: True)

        Returns
        -------
        MiljoenhuizenWoning or None
            Parsed property data or None on failure
        """
        # Create cache key from URL path
        url_path = url.replace(self.BASE_URL, "").strip("/")
        cache_key = f"detail_{url_path}"

        if use_cache:
            cached = self._load_from_cache(cache_key, self.detail_cache_days)
            if cached and "woning" in cached:
                return MiljoenhuizenWoning.from_dict(cached["woning"])

        html = self._fetch_page(url)
        if not html:
            return None

        soup = BeautifulSoup(html, "lxml")

        # Extract address and location from URL or page
        # URL format: /voorburg/2271ve/schellinglaan/14
        url_parts = url.replace(self.BASE_URL, "").strip("/").split("/")
        plaats = url_parts[0] if len(url_parts) > 0 else ""
        postcode = url_parts[1].upper() if len(url_parts) > 1 else ""
        straat = url_parts[2] if len(url_parts) > 2 else ""
        huisnummer = url_parts[3] if len(url_parts) > 3 else ""

        # Format address
        adres = f"{straat.replace('-', ' ').title()} {huisnummer}" if straat else ""

        # Initialize woning
        woning = MiljoenhuizenWoning(
            url=url,
            adres=adres,
            postcode=postcode,
            plaats=plaats.title(),
        )

        # Parse price/estimated value from #price div
        price_div = soup.select_one("#price")
        if price_div:
            h2 = price_div.select_one("h2")
            if h2:
                # Check if it contains spans (estimated value range format)
                spans = h2.select("span")
                if len(spans) >= 3:
                    # Range format: € X - € Y
                    woning.geschatte_waarde_laag = self._parse_price(spans[0].get_text(strip=True))
                    woning.geschatte_waarde_hoog = self._parse_price(spans[2].get_text(strip=True))
                else:
                    # Single price format (active listing)
                    single_price = self._parse_price(h2.get_text(strip=True))
                    if single_price:
                        woning.laatste_vraagprijs = single_price

        # Parse price history: #price-history .price-history-entry
        price_history = soup.select_one("#price-history")
        if price_history:
            entries = price_history.select(".price-history-entry")
            for entry in entries:
                text = entry.get_text(strip=True)
                # Parse: "17-06-2018: te koop voor € 799.000"
                # Or: "14-07-2025: veranderd naar € 1.015.000"
                # Or: "28-10-2025: verkocht met vraagprijs € 985.000"

                match = re.match(r"(\d{2}-\d{2}-\d{4}):\s*(.+)", text)
                if match:
                    datum = match.group(1)
                    rest = match.group(2)

                    # Determine action
                    actie = "onbekend"
                    if "te koop" in rest.lower():
                        actie = "te_koop"
                    elif "veranderd" in rest.lower():
                        actie = "veranderd"
                    elif "verkocht" in rest.lower():
                        actie = "verkocht"
                        woning.status = "verkocht"
                        woning.verkoopdatum = datum

                    # Extract price
                    prijs = self._parse_price(rest)

                    woning.prijshistorie.append(
                        PrijsHistorieEntry(datum=datum, actie=actie, prijs=prijs)
                    )

            # Set laatste_vraagprijs from history
            if woning.prijshistorie:
                # Get most recent price
                for entry in reversed(woning.prijshistorie):
                    if entry.prijs:
                        woning.laatste_vraagprijs = entry.prijs
                        break

        # Parse properties: #properties .prop
        properties = soup.select_one("#properties")
        if properties:
            props = properties.select(".prop")
            for prop in props:
                divs = prop.select("div")
                if len(divs) < 2:
                    continue

                label = divs[0].get_text(strip=True).lower()
                value = divs[1].get_text(strip=True)

                if "soort" in label or "type" in label:
                    woning.woningtype = value
                elif "bouwjaar" in label:
                    try:
                        woning.bouwjaar = int(value)
                    except ValueError:
                        pass
                elif "woonoppervlakte" in label:
                    match = re.search(r"(\d+)", value)
                    if match:
                        woning.woonoppervlakte = int(match.group(1))
                elif "perceel" in label:
                    match = re.search(r"(\d+)", value)
                    if match:
                        woning.perceeloppervlakte = int(match.group(1))
                elif "inhoud" in label:
                    match = re.search(r"(\d+)", value)
                    if match:
                        woning.inhoud = int(match.group(1))
                elif "slaapkamer" in label:
                    match = re.search(r"(\d+)", value)
                    if match:
                        woning.slaapkamers = int(match.group(1))

        if use_cache:
            self._save_to_cache(cache_key, {"woning": woning.to_dict()})

        return woning

    def zoek_in_postcode(
        self,
        postcode: str,
        max_results: int = 20,
        use_cache: bool = True,
    ) -> List[MiljoenhuizenWoning]:
        """
        Search for properties in a postcode area.

        Parameters
        ----------
        postcode : str
            Dutch postcode (4 or 6 characters)
        max_results : int
            Maximum number of results to return (default: 20)
        use_cache : bool
            Whether to use cached results (default: True)

        Returns
        -------
        list
            List of MiljoenhuizenWoning objects
        """
        pc = postcode.replace(" ", "").upper()
        pc4 = pc[:4]

        cache_key = f"postcode_{pc4}"

        if use_cache:
            cached = self._load_from_cache(cache_key, self.detail_cache_days)
            if cached and "woningen" in cached:
                woningen = [MiljoenhuizenWoning.from_dict(w) for w in cached["woningen"]]
                # Filter by full postcode if 6 chars provided
                if len(pc) == 6:
                    woningen = [w for w in woningen if w.postcode.upper() == pc]
                return woningen[:max_results]

        # Search across all plaatsen
        all_woningen: List[MiljoenhuizenWoning] = []
        seen_urls: set = set()

        for plaats in self.PLAATSEN:
            # Fetch up to 3 pages per plaats
            for page in range(1, 4):
                listings = self.scrape_overzicht(plaats, page, use_cache=use_cache)

                if not listings:
                    break

                # Filter by postcode
                for listing in listings:
                    listing_pc = listing.get("postcode", "").upper()
                    if not listing_pc.startswith(pc4):
                        continue

                    url = listing.get("url", "")
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)

                    # Fetch detail page
                    woning = self.scrape_detail(url, use_cache=use_cache)
                    if woning:
                        all_woningen.append(woning)

                    if len(all_woningen) >= max_results:
                        break

                if len(all_woningen) >= max_results:
                    break

            if len(all_woningen) >= max_results:
                break

        # Cache results
        if use_cache and all_woningen:
            self._save_to_cache(
                cache_key,
                {"woningen": [w.to_dict() for w in all_woningen]},
            )

        # Filter by full postcode if provided
        if len(pc) == 6:
            all_woningen = [w for w in all_woningen if w.postcode.upper() == pc]

        return all_woningen[:max_results]

    def get_vergelijkbare_verkopen(
        self,
        postcode: str,
        huisnummer: Optional[int] = None,
        woonoppervlakte: Optional[int] = None,
        max_results: int = 10,
    ) -> List[MiljoenhuizenWoning]:
        """
        Get comparable sold properties in the area.

        Parameters
        ----------
        postcode : str
            Dutch postcode
        huisnummer : int, optional
            House number (to exclude from results)
        woonoppervlakte : int, optional
            Living area in m2 (for filtering similar properties)
        max_results : int
            Maximum number of results (default: 10)

        Returns
        -------
        list
            List of sold MiljoenhuizenWoning objects sorted by relevance
        """
        pc4 = postcode.replace(" ", "").upper()[:4]

        # Get properties in the area
        woningen = self.zoek_in_postcode(pc4, max_results=50)

        # Filter for sold properties
        verkocht = [w for w in woningen if w.status == "verkocht"]

        # Exclude the target property
        if huisnummer:
            verkocht = [w for w in verkocht if w.huisnummer != huisnummer]

        # Score and sort by relevance
        scored = []
        for w in verkocht:
            score = 0

            # Same PC6 is better
            if w.postcode.upper().startswith(postcode.replace(" ", "").upper()):
                score += 10

            # Similar living area (±20%)
            if woonoppervlakte and w.woonoppervlakte:
                diff_pct = abs(w.woonoppervlakte - woonoppervlakte) / woonoppervlakte
                if diff_pct < 0.1:
                    score += 5
                elif diff_pct < 0.2:
                    score += 3

            # More recent is better
            if w.verkoopdatum:
                try:
                    parts = w.verkoopdatum.split("-")
                    if len(parts) == 3:
                        year = int(parts[2])
                        if year >= 2024:
                            score += 3
                        elif year >= 2022:
                            score += 1
                except ValueError:
                    pass

            scored.append((score, w))

        # Sort by score (descending)
        scored.sort(key=lambda x: x[0], reverse=True)

        return [w for _, w in scored[:max_results]]


def create_miljoenhuizen_collector(
    cache_dir: Optional[Path] = None,
) -> MiljoenhuizenCollector:
    """
    Factory function to create a Miljoenhuizen collector with default cache directory.

    Parameters
    ----------
    cache_dir : Path, optional
        Cache directory. If None, uses data/cache/miljoenhuizen.

    Returns
    -------
    MiljoenhuizenCollector
        Configured collector instance
    """
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "miljoenhuizen"

    return MiljoenhuizenCollector(cache_dir=cache_dir)
