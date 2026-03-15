"""
BAG (Basisregistratie Adressen en Gebouwen) API client.

Wraps the Kadaster BAG Individuele Bevragingen API with rate limiting.

API Limits:
- Maximum 50 requests per second
- Maximum 50,000 requests per day per API key
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import requests


class BagRateLimitError(RuntimeError):
    """Raised when the BAG daily quota has been exhausted."""


@dataclass
class BagClient:
    """
    Client for the BAG Individuele Bevragingen API.

    Parameters
    ----------
    api_key:
        API key provided by PDOK/Kadaster.
    min_interval:
        Minimum delay between requests in seconds (default: 0.02 = 50 req/s).
    daily_quota:
        Maximum requests per day (default: 50,000).
    """

    api_key: str
    min_interval: float = 0.02
    daily_quota: int = 50_000
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)
    _request_count: int = field(default=0, init=False, repr=False)

    BASE_URL = "https://api.bag.kadaster.nl/lvbag/individuelebevragingen/v2"

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()

    @property
    def requests_remaining(self) -> int:
        """Number of requests remaining in the daily quota."""
        return max(0, self.daily_quota - self._request_count)

    def get_nummeraanduiding(
        self,
        postcode: str,
        huisnummer: int,
        huisletter: Optional[str] = None,
        toevoeging: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Look up a nummeraanduiding by postcode and house number.

        Returns the raw JSON for the first match, or None if not found.
        """
        params: Dict[str, Any] = {"postcode": postcode, "huisnummer": huisnummer}
        if huisletter:
            params["huisletter"] = huisletter
        if toevoeging:
            params["huisnummertoevoeging"] = toevoeging

        data = self._request("/nummeraanduidingen", params=params)
        items = data.get("_embedded", {}).get("nummeraanduidingen", [])
        return items[0] if items else None

    def get_adres(self, nummeraanduiding_id: str) -> Dict[str, Any]:
        """Fetch address details by nummeraanduiding ID."""
        return self._request(f"/adressen/{nummeraanduiding_id}")

    def get_verblijfsobject(self, identificatie: str) -> Dict[str, Any]:
        """Fetch verblijfsobject (dwelling) details."""
        return self._request(
            f"/verblijfsobjecten/{identificatie}",
            extra_headers={"Accept-Crs": "EPSG:28992"},
        )

    def get_pand(self, identificatie: str) -> Dict[str, Any]:
        """Fetch pand (building) details."""
        return self._request(
            f"/panden/{identificatie}",
            extra_headers={"Accept-Crs": "EPSG:28992"},
        )

    def fetch_resource(self, url: str) -> Dict[str, Any]:
        """Fetch an arbitrary BAG resource by URL."""
        return self._request(url)

    def enrich_address(
        self,
        postcode: str,
        huisnummer: int,
        huisletter: Optional[str] = None,
        toevoeging: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Fetch complete BAG data for an address.

        Returns a dict with all available BAG fields:
        - nummeraanduiding_id, postcode_official, huisnummer_official
        - openbareruimte_naam, woonplaats_naam
        - verblijfsobject_id, status, gebruiksdoelen, oppervlakte
        - pand_bouwjaar, pand_status
        """
        result: Dict[str, Any] = {
            "nummeraanduiding_id": None,
            "postcode_official": None,
            "huisnummer_official": None,
            "openbareruimte_naam": None,
            "woonplaats_naam": None,
            "verblijfsobject_id": None,
            "verblijfsobject_status": None,
            "gebruiksdoelen": None,
            "oppervlakte": None,
            "pand_identificaties": None,
            "pand_bouwjaar": None,
            "pand_status": None,
        }

        bag_info = self.get_nummeraanduiding(postcode, huisnummer, huisletter, toevoeging)
        if not bag_info:
            return result

        nummer = bag_info.get("nummeraanduiding", {})
        result["nummeraanduiding_id"] = nummer.get("identificatie")
        result["postcode_official"] = nummer.get("postcode")
        result["huisnummer_official"] = nummer.get("huisnummer")

        nummer_id = result["nummeraanduiding_id"]
        if nummer_id:
            nummer_id = str(nummer_id).split(".")[0]
            try:
                adres_info = self.get_adres(nummer_id)
                result["openbareruimte_naam"] = adres_info.get("openbareRuimteNaam")
                result["woonplaats_naam"] = adres_info.get("woonplaatsNaam")

                vobj_id = adres_info.get("adresseerbaarObjectIdentificatie")
                pand_ids = adres_info.get("pandIdentificaties") or []

                if vobj_id:
                    vobj_data = self.get_verblijfsobject(str(vobj_id))
                    vobj = vobj_data.get("verblijfsobject", {})
                    result["verblijfsobject_id"] = vobj.get("identificatie")
                    result["verblijfsobject_status"] = vobj.get("status")
                    doelen = vobj.get("gebruiksdoelen") or []
                    result["gebruiksdoelen"] = doelen
                    result["oppervlakte"] = vobj.get("oppervlakte")
                    pand_ids = vobj.get("maaktDeelUitVan") or pand_ids

                if pand_ids:
                    result["pand_identificaties"] = pand_ids
                    pand_data = self.get_pand(str(pand_ids[0]))
                    pand = pand_data.get("pand", {})
                    result["pand_bouwjaar"] = pand.get("oorspronkelijkBouwjaar")
                    result["pand_status"] = pand.get("status")
            except Exception:
                pass  # Keep partial results

        return result

    def _request(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Execute a rate-limited HTTP GET request."""
        if self._request_count >= self.daily_quota:
            raise BagRateLimitError(
                f"Daily BAG quota of {self.daily_quota} requests exhausted."
            )

        # Rate limiting
        now = time.perf_counter()
        elapsed = now - self._last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

        headers = {"X-Api-Key": self.api_key}
        if extra_headers:
            headers.update(extra_headers)

        url = path if path.startswith("http") else f"{self.BASE_URL}{path}"
        response = self.session.get(url, params=params, headers=headers, timeout=30)
        self._last_request = time.perf_counter()
        self._request_count += 1
        response.raise_for_status()
        return response.json()


def parse_address_components(address: str) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Extract house number, letter, and suffix from a Dutch address.

    Examples:
        "Hoofdstraat 10" -> (10, None, None)
        "Hoofdstraat 10A" -> (10, "A", None)
        "Hoofdstraat 10A-2" -> (10, "A", "2")
    """
    import re

    if not isinstance(address, str):
        return (None, None, None)

    match = re.search(r"(\d+)\s*([A-Za-z]{1,3})?\s*(?:[-\s]?([A-Za-z0-9]{1,4}))?", address)
    if not match:
        return (None, None, None)

    number = int(match.group(1))
    letter = match.group(2)
    suffix = match.group(3)

    # Handle duplicate letters (e.g., "10AA" -> 10, A, A)
    if letter and suffix and suffix.upper().startswith(letter.upper()):
        suffix = suffix[len(letter):]

    return number, letter, suffix if suffix else None
