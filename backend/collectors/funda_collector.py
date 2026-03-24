"""
Funda property listing collector.

Fetches property listings from Funda for a specific address (postcode + huisnummer).
Uses respectful rate limiting (2-3s between requests) and file-based caching (1 day).

Note: For personal, non-commercial use only. Rate limiting is enforced.
Only fetches individual addresses — no bulk scraping.
"""

from __future__ import annotations

import hashlib
import json
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


@dataclass
class PropertyListing:
    """Represents a Funda property listing."""

    # Basis
    url: str
    address: str
    postcode: Optional[str] = None
    city: Optional[str] = None
    price: Optional[int] = None
    price_suffix: Optional[str] = None  # "kosten koper", "vrij op naam"

    # Woningkenmerken
    living_area: Optional[int] = None
    plot_area: Optional[int] = None
    volume: Optional[int] = None  # inhoud m³
    rooms: Optional[int] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    building_type: Optional[str] = None  # "Herenhuis, hoekwoning"
    construction_type: Optional[str] = None  # "Bestaande bouw", "Nieuwbouw"
    year_built: Optional[int] = None
    energy_label: Optional[str] = None

    # Eigendomsituatie
    eigendom_type: Optional[str] = None  # "Volle eigendom", "Erfpacht"
    erfpacht_bedrag: Optional[int] = None
    erfpacht_einddatum: Optional[str] = None
    vve_bijdrage: Optional[int] = None  # per maand

    # Tuin & buitenruimte
    tuin_type: Optional[str] = None  # "Achtertuin en voortuin"
    tuin_oppervlakte: Optional[int] = None  # m²
    tuin_orientatie: Optional[str] = None  # "Gelegen op het noordoosten"
    buitenruimte: Optional[int] = None  # gebouwgebonden buitenruimte m²
    balkon: Optional[bool] = None
    dakterras: Optional[bool] = None

    # Indeling & parkeren
    verdiepingen: Optional[int] = None
    garage_type: Optional[str] = None  # "Aangebouwde stenen garage"
    parkeerplaatsen: Optional[int] = None
    parkeer_type: Optional[str] = None  # "Betaald parkeren en openbaar parkeren"
    kelder: Optional[bool] = None
    zolder: Optional[str] = None  # "Niet bereikbaar", "Bereikbaar", "Ingericht"
    berging: Optional[str] = None  # "Vrijstaand kunststof"

    # Extra
    isolatie: Optional[str] = None
    verwarming: Optional[str] = None
    warm_water: Optional[str] = None
    cv_ketel: Optional[str] = None
    dak_type: Optional[str] = None
    aangeboden_sinds: Optional[str] = None  # datum, vereist login
    status: str = "beschikbaar"

    # Verkocht-specifiek
    verkoopdatum: Optional[str] = None  # "2025-01-15"
    looptijd_dagen: Optional[int] = None  # dagen op markt

    date_scraped: datetime = field(default_factory=datetime.now)

    @property
    def funda_id(self) -> Optional[str]:
        """Extract Funda property ID from URL."""
        match = re.search(r"/(\d{6,})/?$", self.url.rstrip("/") + "/")
        return match.group(1) if match else None

    @property
    def pc6(self) -> Optional[str]:
        """Extract 6-digit postcode."""
        if self.postcode:
            clean = self.postcode.replace(" ", "").upper()
            if len(clean) >= 6:
                return clean[:6]
        return None

    @property
    def price_per_m2(self) -> Optional[float]:
        """Calculate price per m²."""
        if self.price and self.living_area and self.living_area > 0:
            return round(self.price / self.living_area, 2)
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "url": self.url,
            "address": self.address,
            "postcode": self.postcode,
            "city": self.city,
            "price": self.price,
            "price_suffix": self.price_suffix,
            "living_area": self.living_area,
            "plot_area": self.plot_area,
            "volume": self.volume,
            "rooms": self.rooms,
            "bedrooms": self.bedrooms,
            "bathrooms": self.bathrooms,
            "building_type": self.building_type,
            "construction_type": self.construction_type,
            "year_built": self.year_built,
            "energy_label": self.energy_label,
            "eigendom_type": self.eigendom_type,
            "erfpacht_bedrag": self.erfpacht_bedrag,
            "erfpacht_einddatum": self.erfpacht_einddatum,
            "vve_bijdrage": self.vve_bijdrage,
            "tuin_type": self.tuin_type,
            "tuin_oppervlakte": self.tuin_oppervlakte,
            "tuin_orientatie": self.tuin_orientatie,
            "buitenruimte": self.buitenruimte,
            "balkon": self.balkon,
            "dakterras": self.dakterras,
            "verdiepingen": self.verdiepingen,
            "garage_type": self.garage_type,
            "parkeerplaatsen": self.parkeerplaatsen,
            "parkeer_type": self.parkeer_type,
            "kelder": self.kelder,
            "zolder": self.zolder,
            "berging": self.berging,
            "isolatie": self.isolatie,
            "verwarming": self.verwarming,
            "warm_water": self.warm_water,
            "cv_ketel": self.cv_ketel,
            "dak_type": self.dak_type,
            "aangeboden_sinds": self.aangeboden_sinds,
            "status": self.status,
            "verkoopdatum": self.verkoopdatum,
            "looptijd_dagen": self.looptijd_dagen,
            "price_per_m2": self.price_per_m2,
            "date_scraped": self.date_scraped.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PropertyListing":
        """Deserialize from dictionary."""
        scraped_at = data.get("date_scraped")
        if isinstance(scraped_at, str):
            scraped_at = datetime.fromisoformat(scraped_at)
        elif scraped_at is None:
            scraped_at = datetime.now()

        return cls(
            url=data.get("url", ""),
            address=data.get("address", ""),
            postcode=data.get("postcode"),
            city=data.get("city"),
            price=data.get("price"),
            price_suffix=data.get("price_suffix"),
            living_area=data.get("living_area"),
            plot_area=data.get("plot_area"),
            volume=data.get("volume"),
            rooms=data.get("rooms"),
            bedrooms=data.get("bedrooms"),
            bathrooms=data.get("bathrooms"),
            building_type=data.get("building_type"),
            construction_type=data.get("construction_type"),
            year_built=data.get("year_built"),
            energy_label=data.get("energy_label"),
            eigendom_type=data.get("eigendom_type"),
            erfpacht_bedrag=data.get("erfpacht_bedrag"),
            erfpacht_einddatum=data.get("erfpacht_einddatum"),
            vve_bijdrage=data.get("vve_bijdrage"),
            tuin_type=data.get("tuin_type"),
            tuin_oppervlakte=data.get("tuin_oppervlakte"),
            tuin_orientatie=data.get("tuin_orientatie"),
            buitenruimte=data.get("buitenruimte"),
            balkon=data.get("balkon"),
            dakterras=data.get("dakterras"),
            verdiepingen=data.get("verdiepingen"),
            garage_type=data.get("garage_type"),
            parkeerplaatsen=data.get("parkeerplaatsen"),
            parkeer_type=data.get("parkeer_type"),
            kelder=data.get("kelder"),
            zolder=data.get("zolder"),
            berging=data.get("berging"),
            isolatie=data.get("isolatie"),
            verwarming=data.get("verwarming"),
            warm_water=data.get("warm_water"),
            cv_ketel=data.get("cv_ketel"),
            dak_type=data.get("dak_type"),
            aangeboden_sinds=data.get("aangeboden_sinds"),
            status=data.get("status", "beschikbaar"),
            verkoopdatum=data.get("verkoopdatum"),
            looptijd_dagen=data.get("looptijd_dagen"),
            date_scraped=scraped_at,
        )


@dataclass
class FundaCollector:
    """
    Collector for Funda property listings.

    Fetches a single property listing by address (postcode + huisnummer).
    Uses respectful rate limiting and file-based caching.
    """

    min_delay: float = 2.0
    max_delay: float = 3.0
    max_retries: int = 3
    cache_dir: Optional[Path] = None
    cache_days: int = 1
    cookies_file: Optional[Path] = None
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    BASE_URL = "https://www.funda.nl"

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        # Load cookies from file if available (enables logged-in access)
        if self.cookies_file and self.cookies_file.exists():
            self._load_cookies()

    def _load_cookies(self) -> None:
        """
        Load cookies from a JSON file into the session.

        The cookies file should be a JSON array of cookie objects with
        at least 'name' and 'value' fields (EditThisCookie export format),
        or a simple {name: value} dict.
        """
        try:
            with open(self.cookies_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, list):
                # EditThisCookie / browser extension format
                for cookie in data:
                    if "name" in cookie and "value" in cookie:
                        self.session.cookies.set(
                            cookie["name"],
                            cookie["value"],
                            domain=cookie.get("domain", ".funda.nl"),
                        )
            elif isinstance(data, dict):
                # Simple {name: value} format
                for name, value in data.items():
                    self.session.cookies.set(name, value, domain=".funda.nl")
        except (json.JSONDecodeError, IOError):
            pass

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with random user agent."""
        ua = random.choice(USER_AGENTS)
        return {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Linux"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
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

    # ---- Caching ----

    def _get_cache_path(self, cache_key: str) -> Optional[Path]:
        """Get cache file path."""
        if not self.cache_dir:
            return None
        safe_key = re.sub(r"[^\w\-_]", "_", cache_key)
        return self.cache_dir / f"funda_{safe_key}.json"

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

    # ---- HTTP ----

    def _fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch a page with rate limiting and retries.

        Returns HTML content or None on failure.
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
                    wait_time = (2 ** attempt) * self.max_delay
                    time.sleep(wait_time)
                    continue

                if response.status_code == 403:
                    return None

                response.raise_for_status()
                return response.text

            except requests.RequestException:
                if attempt == self.max_retries - 1:
                    return None
                time.sleep(self.max_delay)

        return None

    # ---- Parsing ----

    def _parse_json_ld(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract JSON-LD structured data from page."""
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                # Look for Product/Huis type (detail page)
                types = data.get("@type", [])
                if isinstance(types, str):
                    types = [types]
                if "Product" in types or "Huis" in types or "Appartement" in types:
                    return data
            except (json.JSONDecodeError, TypeError):
                continue
        return {}

    def _parse_kenmerken(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract all dt/dd kenmerken pairs from the page."""
        kenmerken = {}
        for dt in soup.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if dd:
                label = dt.get_text(strip=True)
                value = dd.get_text(strip=True)
                if label and value:
                    kenmerken[label] = value
        return kenmerken

    @staticmethod
    def _parse_int(text: str) -> Optional[int]:
        """Extract integer from text like '€ 1.300.000' or '175 m²'."""
        if not text:
            return None
        # Remove currency, m², m³, and other suffixes
        cleaned = re.sub(r"[€m²m³\s]", "", text)
        # Remove dots as thousands separator
        cleaned = cleaned.replace(".", "")
        # Take first number
        match = re.match(r"(\d+)", cleaned)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                pass
        return None

    def parse_detail_page(self, html: str, url: str = "") -> Optional[PropertyListing]:
        """
        Parse a Funda detail page into a PropertyListing.

        Uses JSON-LD for basic data and dt/dd kenmerken for details.
        """
        soup = BeautifulSoup(html, "lxml")

        # JSON-LD for basic info
        json_ld = self._parse_json_ld(soup)
        if not json_ld and not soup.find("dt"):
            return None

        # Address from JSON-LD
        address_data = json_ld.get("address", {})
        address = json_ld.get("name", "")
        city = address_data.get("addressLocality", "")

        # Price from JSON-LD
        offers = json_ld.get("offers", {})
        price = None
        if isinstance(offers, dict) and offers.get("price"):
            try:
                price = int(float(offers["price"]))
            except (ValueError, TypeError):
                pass

        # Extract postcode from description or address
        description = json_ld.get("description", "")
        postcode = None
        pc_match = re.search(r"(\d{4}\s?[A-Z]{2})", description)
        if pc_match:
            postcode = pc_match.group(1).replace(" ", "").upper()

        # Parse all kenmerken
        km = self._parse_kenmerken(soup)

        # Build listing
        listing = PropertyListing(
            url=url or json_ld.get("url", ""),
            address=address,
            postcode=postcode,
            city=city,
        )

        # Price
        if price:
            listing.price = price
        elif "Vraagprijs" in km:
            listing.price = self._parse_int(km["Vraagprijs"])

        # Price suffix (kosten koper / vrij op naam)
        vraagprijs_text = km.get("Vraagprijs", "")
        if "kosten koper" in vraagprijs_text.lower():
            listing.price_suffix = "kosten koper"
        elif "vrij op naam" in vraagprijs_text.lower():
            listing.price_suffix = "vrij op naam"

        # Status
        status_text = km.get("Status", "").lower()
        if "verkocht" in status_text:
            listing.status = "verkocht"
        elif "onder bod" in status_text:
            listing.status = "onder bod"
        else:
            listing.status = "beschikbaar"

        # Woningkenmerken
        if "Wonen" in km:
            listing.living_area = self._parse_int(km["Wonen"])
        elif "Woonoppervlakte" in km:
            listing.living_area = self._parse_int(km["Woonoppervlakte"])

        if "Perceel" in km:
            listing.plot_area = self._parse_int(km["Perceel"])

        if "Inhoud" in km:
            listing.volume = self._parse_int(km["Inhoud"])

        if "Aantal kamers" in km:
            kamers_text = km["Aantal kamers"]
            kamers_match = re.match(r"(\d+)\s*kamers?", kamers_text)
            if kamers_match:
                listing.rooms = int(kamers_match.group(1))
            slaap_match = re.search(r"\((\d+)\s*slaapkamers?\)", kamers_text)
            if slaap_match:
                listing.bedrooms = int(slaap_match.group(1))

        if "Aantal badkamers" in km:
            bad_match = re.match(r"(\d+)", km["Aantal badkamers"])
            if bad_match:
                listing.bathrooms = int(bad_match.group(1))

        listing.building_type = km.get("Soort woonhuis") or km.get("Soort appartement")
        listing.construction_type = km.get("Soort bouw")

        if "Bouwjaar" in km:
            year_match = re.search(r"(\d{4})", km["Bouwjaar"])
            if year_match:
                listing.year_built = int(year_match.group(1))

        if "Energielabel" in km:
            label_match = re.match(r"([A-G][+]*)", km["Energielabel"])
            if label_match:
                listing.energy_label = label_match.group(1)

        # Eigendomsituatie
        listing.eigendom_type = km.get("Eigendomssituatie")

        if "Erfpacht" in km:
            erfpacht_text = km["Erfpacht"]
            listing.erfpacht_bedrag = self._parse_int(erfpacht_text)
            date_match = re.search(r"tot\s+(\d{2}-\d{2}-\d{4})", erfpacht_text)
            if date_match:
                listing.erfpacht_einddatum = date_match.group(1)

        # VvE bijdrage - look for "Bijdrage VvE" or "Servicekosten"
        for vve_key in ["Bijdrage VvE", "Servicekosten", "VvE bijdrage"]:
            if vve_key in km:
                listing.vve_bijdrage = self._parse_int(km[vve_key])
                break

        # Tuin & buitenruimte
        listing.tuin_type = km.get("Tuin")

        # Look for specific garden entries (Achtertuin, Voortuin, etc.)
        for tuin_key in ["Achtertuin", "Voortuin", "Tuin", "Patio/atrium"]:
            if tuin_key in km and "m²" in km[tuin_key]:
                listing.tuin_oppervlakte = self._parse_int(km[tuin_key])
                break

        listing.tuin_orientatie = km.get("Ligging tuin")

        # Gebouwgebonden buitenruimte (balkon/terras oppervlakte)
        if "Gebouwgebonden buitenruimte" in km:
            listing.buitenruimte = self._parse_int(km["Gebouwgebonden buitenruimte"])

        # Balkon/dakterras
        for dt_text in km:
            lower = dt_text.lower()
            if "balkon" in lower:
                listing.balkon = True
            if "dakterras" in lower:
                listing.dakterras = True

        # Indeling & parkeren
        if "Aantal woonlagen" in km:
            verd_match = re.match(r"(\d+)", km["Aantal woonlagen"])
            if verd_match:
                listing.verdiepingen = int(verd_match.group(1))

        listing.garage_type = km.get("Soort garage")
        listing.parkeer_type = km.get("Soort parkeergelegenheid")

        if "Capaciteit" in km:
            cap_match = re.match(r"(\d+)", km["Capaciteit"])
            if cap_match:
                listing.parkeerplaatsen = int(cap_match.group(1))

        # Kelder
        for key in km:
            if "kelder" in key.lower():
                listing.kelder = True
                break

        # Zolder
        for key in km:
            if "zolder" in key.lower():
                listing.zolder = km[key]
                break

        listing.berging = km.get("Schuur/berging")

        # Extra technische kenmerken
        listing.isolatie = km.get("Isolatie")
        listing.verwarming = km.get("Verwarming")
        listing.warm_water = km.get("Warm water")
        listing.cv_ketel = km.get("Cv-ketel")
        listing.dak_type = km.get("Soort dak")

        # Aangeboden sinds (requires login)
        aangeboden = km.get("Aangeboden sinds", "")
        if aangeboden and "log in" not in aangeboden.lower():
            listing.aangeboden_sinds = aangeboden

        # Verkocht-specifieke velden
        if "Verkoopdatum" in km:
            vd_text = km["Verkoopdatum"]
            # Parse "15 januari 2025" format
            listing.verkoopdatum = vd_text

        if "Looptijd" in km:
            looptijd_text = km["Looptijd"]
            # Parse "45 dagen" or "2 maanden en 15 dagen"
            dagen_match = re.search(r"(\d+)\s*dag", looptijd_text)
            if dagen_match:
                listing.looptijd_dagen = int(dagen_match.group(1))
            maanden_match = re.search(r"(\d+)\s*maand", looptijd_text)
            if maanden_match:
                maanden = int(maanden_match.group(1))
                extra_dagen = int(dagen_match.group(1)) if dagen_match else 0
                listing.looptijd_dagen = maanden * 30 + extra_dagen

        return listing

    # ---- Address resolution ----

    def _resolve_street_from_pdok(
        self, postcode: str, huisnummer: int
    ) -> Optional[Dict[str, str]]:
        """
        Resolve street name and city from PDOK Locatieserver.

        Returns dict with 'straatnaam', 'woonplaats', or None on failure.
        """
        pc = postcode.replace(" ", "").upper()
        url = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"
        params = {
            "q": f"{pc} {huisnummer}",
            "fq": "type:adres",
            "rows": 1,
            "fl": "straatnaam,woonplaatsnaam",
        }
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            docs = data.get("response", {}).get("docs", [])
            if docs:
                doc = docs[0]
                straatnaam = doc.get("straatnaam")
                woonplaats = doc.get("woonplaatsnaam")
                if straatnaam and woonplaats:
                    return {"straatnaam": straatnaam, "woonplaats": woonplaats}
        except requests.RequestException:
            pass
        return None

    @staticmethod
    def _slugify(text: str) -> str:
        """Convert text to a Funda-compatible URL slug.

        Examples: "Gerard Reijnststraat" -> "gerard-reijnststraat"
                  "'s-Gravenhage" -> "s-gravenhage"
        """
        slug = text.lower().strip()
        slug = slug.replace("'", "")
        # Replace non-alphanumeric chars (except dash) with dashes
        slug = re.sub(r"[^a-z0-9-]", "-", slug)
        # Collapse multiple dashes
        slug = re.sub(r"-+", "-", slug)
        return slug.strip("-")

    # Known mappings from PDOK woonplaats to Funda city slug
    _CITY_SLUG_MAP: ClassVar[Dict[str, str]] = {
        "'s-gravenhage": "den-haag",
        "'s-hertogenbosch": "s-hertogenbosch",
    }

    def _build_street_geo_identifier(
        self, straatnaam: str, woonplaats: str
    ) -> str:
        """
        Build a street-level GeoIdentifier for Funda search.

        Constructs the identifier directly from PDOK address data.
        Format: '{city-slug}/straat-{street-slug}'
        """
        city_lower = woonplaats.lower().strip()
        city_slug = self._CITY_SLUG_MAP.get(city_lower)
        if not city_slug:
            city_slug = self._slugify(woonplaats)

        street_slug = self._slugify(straatnaam)
        return f"{city_slug}/straat-{street_slug}"

    # ---- Search ----

    @staticmethod
    def _extract_huisnummer_from_url(url: str) -> Optional[str]:
        """Extract house number from a Funda listing URL.

        URL format: /detail/koop/city/type-straatnaam-73-c/43381496/
        The house number is in the address part, followed by an optional letter suffix.
        """
        match = re.search(r"/(?:huis|appartement)-(.+?)/(\d+)/?$", url)
        if not match:
            return None
        addr_part = match.group(1)
        # House number is at the end of the address slug, possibly with letter suffix
        hn_match = re.search(r"-(\d+)(?:-[a-z])?$", addr_part)
        if hn_match:
            return hn_match.group(1)
        return None

    def _find_listing_url_for_address(
        self, search_html: str, huisnummer: int
    ) -> Optional[str]:
        """
        Find the listing URL matching a specific house number from search results.

        Parses the JSON-LD ItemList from the search page and matches on house number.
        """
        soup = BeautifulSoup(search_html, "lxml")
        huisnummer_str = str(huisnummer)

        # Extract listing URLs from JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if "itemListElement" not in data:
                    continue

                items = data["itemListElement"]

                for item in items:
                    url = item.get("url", "")
                    url_hn = self._extract_huisnummer_from_url(url)
                    if url_hn == huisnummer_str:
                        return url

                # If only one result, return it regardless of number match
                if len(items) == 1:
                    return items[0].get("url")

            except (json.JSONDecodeError, TypeError):
                continue

        return None

    def search_by_address(
        self,
        postcode: str,
        huisnummer: int,
        huisletter: Optional[str] = None,
        include_sold: bool = True,
    ) -> Optional[PropertyListing]:
        """
        Search for a property listing by postcode and house number.

        Uses the Funda suggest API to resolve a street-level GeoIdentifier,
        then searches with that for precise results. Falls back to
        postcode-based search if the suggest API fails.

        Parameters
        ----------
        postcode : str
            Dutch postcode (e.g., "2596PD" or "2596 PD")
        huisnummer : int
            House number
        huisletter : str, optional
            House letter suffix (e.g., "A")
        include_sold : bool
            If True, also searches for sold/negotiating properties.

        Returns
        -------
        PropertyListing or None
            Parsed listing data, or None if not found on Funda.
        """
        pc = postcode.replace(" ", "").upper()
        pc4 = pc[:4]
        cache_key = f"address_{pc}_{huisnummer}"
        if huisletter:
            cache_key += f"_{huisletter}"

        # Try cache first
        cached = self._load_from_cache(cache_key, self.cache_days)
        if cached and "listing" in cached:
            return PropertyListing.from_dict(cached["listing"])
        if cached and cached.get("not_found"):
            return None

        # Build availability filter for including sold properties
        availability_param = ""
        if include_sold:
            availability_param = '&availability=["available","negotiations","unavailable"]'

        detail_url = None

        # Strategy 1: Resolve street name via PDOK, build street-level GeoIdentifier
        pdok_result = self._resolve_street_from_pdok(pc, huisnummer)
        if pdok_result:
            geo_id = self._build_street_geo_identifier(
                pdok_result["straatnaam"], pdok_result["woonplaats"]
            )
            if geo_id:
                search_url = (
                    f"{self.BASE_URL}/zoeken/koop/"
                    f'?selected_area=["{geo_id}"]'
                    f"{availability_param}"
                )
                search_html = self._fetch_page(search_url)
                if search_html and "Je bent bijna op de pagina" not in search_html:
                    detail_url = self._find_listing_url_for_address(
                        search_html, huisnummer
                    )

        # Strategy 2: Fallback to PC6 → PC4 search
        if not detail_url:
            for search_area in [pc, pc4]:
                search_url = (
                    f"{self.BASE_URL}/zoeken/koop/"
                    f'?selected_area=["{search_area}"]'
                    f"{availability_param}"
                )
                search_html = self._fetch_page(search_url)
                if not search_html:
                    continue

                if "Je bent bijna op de pagina" in search_html:
                    return None

                detail_url = self._find_listing_url_for_address(
                    search_html, huisnummer
                )
                if detail_url:
                    break

                if "0 resultaten" in search_html.lower():
                    continue
                else:
                    break

        if not detail_url:
            self._save_to_cache(cache_key, {"not_found": True})
            return None

        # Fetch and parse detail page
        detail_html = self._fetch_page(detail_url)
        if not detail_html:
            return None

        listing = self.parse_detail_page(detail_html, url=detail_url)
        if not listing:
            return None

        if not listing.postcode:
            listing.postcode = pc

        self._save_to_cache(cache_key, {"listing": listing.to_dict()})

        return listing

    def get_listing(self, url: str) -> Optional[PropertyListing]:
        """
        Fetch and parse a single Funda detail page by URL.

        Parameters
        ----------
        url : str
            Full Funda listing URL.

        Returns
        -------
        PropertyListing or None
            Parsed listing data, or None on failure.
        """
        # Cache based on URL path
        url_path = url.replace(self.BASE_URL, "").strip("/")
        cache_key = f"url_{url_path}"

        cached = self._load_from_cache(cache_key, self.cache_days)
        if cached and "listing" in cached:
            return PropertyListing.from_dict(cached["listing"])

        html = self._fetch_page(url)
        if not html:
            return None

        if "Je bent bijna op de pagina" in html:
            return None

        listing = self.parse_detail_page(html, url=url)
        if listing:
            self._save_to_cache(cache_key, {"listing": listing.to_dict()})

        return listing


def parse_funda_url(url: str) -> Dict[str, Optional[str]]:
    """
    Parse property info from a Funda URL.

    Example:
        https://www.funda.nl/detail/koop/den-haag/huis-straatnaam-10/43380320/
        -> {"type": "koop", "city": "den-haag", "id": "43380320", "address": "straatnaam-10"}
    """
    from urllib.parse import urlparse

    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]

    result: Dict[str, Optional[str]] = {
        "type": None,
        "city": None,
        "id": None,
        "address": None,
    }

    # New URL format: /detail/koop/city/huis-address/id/
    if len(parts) >= 2 and parts[0] == "detail":
        result["type"] = parts[1] if len(parts) > 1 else None
        result["city"] = parts[2] if len(parts) > 2 else None

        if len(parts) >= 4:
            listing_part = parts[3]
            match = re.match(r"(?:huis|appartement)-(.+)", listing_part)
            if match:
                result["address"] = match.group(1).replace("-", " ")

        if len(parts) >= 5:
            result["id"] = parts[4]

    # Old URL format: /koop/city/huis-id-address/
    elif len(parts) >= 2:
        result["type"] = parts[0]
        result["city"] = parts[1]

        if len(parts) >= 3:
            listing_part = parts[2]
            match = re.match(r"(?:huis|appartement)-(\d+)-(.+)", listing_part)
            if match:
                result["id"] = match.group(1)
                result["address"] = match.group(2).replace("-", " ")

    return result


def create_funda_collector(
    cache_dir: Optional[Path] = None,
    cookies_file: Optional[Path] = None,
) -> FundaCollector:
    """
    Factory function to create a Funda collector with default cache directory.

    Parameters
    ----------
    cache_dir : Path, optional
        Cache directory. If None, uses data/cache/funda.
    cookies_file : Path, optional
        Path to Funda cookies JSON file for logged-in access.
        If None, checks for config/funda_cookies.json.

    Returns
    -------
    FundaCollector
        Configured collector instance.
    """
    project_root = Path(__file__).parent.parent.parent

    if cache_dir is None:
        cache_dir = project_root / "data" / "cache" / "funda"

    if cookies_file is None:
        default_cookies = project_root / "config" / "funda_cookies.json"
        if default_cookies.exists():
            cookies_file = default_cookies

    return FundaCollector(cache_dir=cache_dir, cookies_file=cookies_file)
