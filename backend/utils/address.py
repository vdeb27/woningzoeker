"""Address parsing utilities."""

import re
from typing import Optional, Tuple


def parse_huisnummer(adres: str) -> Tuple[Optional[int], Optional[str]]:
    """Parse huisnummer and optional huisletter from an address string.

    Returns:
        (huisnummer, huisletter) tuple. Both may be None if parsing fails.
    """
    if not adres:
        return None, None
    match = re.search(r'(\d+)\s*([A-Za-z])?', adres)
    if not match:
        return None, None
    huisnummer = int(match.group(1))
    huisletter = match.group(2)
    return huisnummer, huisletter
