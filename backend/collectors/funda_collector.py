"""
Funda property listing collector.

Scrapes property listings from Funda with respectful rate limiting.
"""

from __future__ import annotations

import hashlib
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]


@dataclass
class PropertyListing:
    """Represents a Funda property listing."""

    url: str
    address: str
    postcode: Optional[str] = None
    city: Optional[str] = None
    price: Optional[int] = None
    living_area: Optional[int] = None
    plot_area: Optional[int] = None
    rooms: Optional[int] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    building_type: Optional[str] = None
    year_built: Optional[int] = None
    energy_label: Optional[str] = None
    status: str = "active"  # active, sold, withdrawn
    date_listed: Optional[datetime] = None
    date_scraped: datetime = field(default_factory=datetime.now)
    raw_data: Dict[str, Any] = field(default_factory=dict)

    @property
    def funda_id(self) -> str:
        """Extract Funda property ID from URL."""
        match = re.search(r"/(\d+)-", self.url)
        return match.group(1) if match else hashlib.md5(self.url.encode()).hexdigest()[:12]

    @property
    def pc6(self) -> Optional[str]:
        """Extract 6-digit postcode."""
        if self.postcode:
            clean = self.postcode.replace(" ", "").upper()
            if len(clean) >= 6:
                return clean[:6]
        return None


@dataclass
class FundaCollector:
    """
    Collector for Funda property listings.

    Uses respectful rate limiting (1-2 requests per second by default).
    """

    min_delay: float = 1.0
    max_delay: float = 2.0
    session: Optional[requests.Session] = None
    cache_dir: Optional[Path] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    BASE_URL = "https://www.funda.nl"

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with random user agent."""
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

    def fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch a page with rate limiting.

        Returns the HTML content or None on failure.
        """
        self._rate_limit()

        try:
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=30,
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Failed to fetch {url}: {e}")
            return None

    def parse_listing_card(self, html_element: Any) -> Optional[Dict[str, Any]]:
        """
        Parse a listing card from search results.

        This is a placeholder - actual implementation depends on HTML structure.
        """
        # Note: Funda's HTML structure changes frequently.
        # This would need to be updated based on current structure.
        # Consider using BeautifulSoup or lxml for parsing.
        return None

    def parse_detail_page(self, html: str) -> Dict[str, Any]:
        """
        Parse a property detail page.

        Returns a dict with extracted property data.
        """
        data: Dict[str, Any] = {}

        # Price
        price_match = re.search(r'€\s*([\d.,]+)', html)
        if price_match:
            price_str = price_match.group(1).replace(".", "").replace(",", "")
            try:
                data["price"] = int(price_str)
            except ValueError:
                pass

        # Living area
        area_match = re.search(r'(\d+)\s*m²\s*(?:wonen|woonoppervlakte)', html, re.I)
        if area_match:
            data["living_area"] = int(area_match.group(1))

        # Year built
        year_match = re.search(r'Bouwjaar[:\s]*(\d{4})', html)
        if year_match:
            data["year_built"] = int(year_match.group(1))

        # Energy label
        label_match = re.search(r'Energielabel[:\s]*([A-G][+]*)', html)
        if label_match:
            data["energy_label"] = label_match.group(1)

        # Rooms
        rooms_match = re.search(r'(\d+)\s*kamers?', html, re.I)
        if rooms_match:
            data["rooms"] = int(rooms_match.group(1))

        return data

    def build_search_url(
        self,
        location: str,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        min_area: Optional[int] = None,
        property_type: str = "koop",
    ) -> str:
        """
        Build a Funda search URL.

        Parameters
        ----------
        location : str
            City or region name (e.g., "den-haag", "leidschendam-voorburg")
        min_price : int, optional
            Minimum asking price
        max_price : int, optional
            Maximum asking price
        min_area : int, optional
            Minimum living area in m²
        property_type : str
            "koop" for buy, "huur" for rent

        Returns
        -------
        str
            Complete Funda search URL
        """
        base = f"{self.BASE_URL}/zoeken/{property_type}/"
        params = [f"?selected_area=[\"{location}\"]"]

        if min_price:
            params.append(f"price=\"{min_price}-{max_price or ''}\"")
        if min_area:
            params.append(f"floor_area=\"{min_area}-\"")

        return base + "&".join(params)

    def search_listings(
        self,
        location: str,
        max_pages: int = 10,
        **filters: Any,
    ) -> List[PropertyListing]:
        """
        Search for property listings in a location.

        Note: This is a skeleton implementation. Full scraping would require
        parsing Funda's HTML structure which changes frequently.
        """
        listings: List[PropertyListing] = []
        url = self.build_search_url(location, **filters)

        for page in range(1, max_pages + 1):
            page_url = f"{url}&search_result={page}"
            html = self.fetch_page(page_url)

            if not html:
                break

            # Parse listings from page
            # This would require BeautifulSoup/lxml and knowledge of current structure
            # For now, return empty list

            # Check for "no more results" indicator
            if "geen resultaten" in html.lower():
                break

        return listings


def parse_funda_url(url: str) -> Dict[str, Optional[str]]:
    """
    Parse property info from a Funda URL.

    Example:
        https://www.funda.nl/koop/den-haag/huis-12345678-straatnaam-10/
        -> {"type": "koop", "city": "den-haag", "id": "12345678", "address": "straatnaam-10"}
    """
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]

    result: Dict[str, Optional[str]] = {
        "type": None,
        "city": None,
        "id": None,
        "address": None,
    }

    if len(parts) >= 2:
        result["type"] = parts[0]  # koop or huur
        result["city"] = parts[1]

    if len(parts) >= 3:
        # Parse "huis-12345678-straatnaam-10" format
        listing_part = parts[2]
        match = re.match(r"(?:huis|appartement)-(\d+)-(.+)", listing_part)
        if match:
            result["id"] = match.group(1)
            result["address"] = match.group(2).replace("-", " ")

    return result
