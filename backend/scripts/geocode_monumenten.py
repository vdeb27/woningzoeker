"""
Convert raw gemeentelijke monumenten data to standardized CSV with postcodes.

Sources:
- Den Haag: CKAN CSV (downloaded to data/gemeentelijke_monumenten/denhaag_raw.csv)
- Rijswijk: Hardcoded from gemeente PDF
- Leidschendam-Voorburg: Wikipedia pages

Geocodes addresses via PDOK to get postcodes.

Usage:
    cd backend && source venv/bin/activate
    python scripts/geocode_monumenten.py
"""

import csv
import json
import re
import sys
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "gemeentelijke_monumenten"

PDOK_URL = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"


def geocode_address(straat: str, huisnummer: str, plaats: str) -> dict | None:
    """Geocode an address via PDOK locatieserver. Returns {postcode, huisnummer, lat, lng}.

    Uses multiple strategies to find a match:
    1. Exact match on straat + huisnummer + plaats
    2. Broader search with more results, picking the best one on the same street
    """
    # Extract pure number for comparison
    nr_match = re.match(r'(\d+)', huisnummer)
    target_nr = int(nr_match.group(1)) if nr_match else 0

    query = f"{straat} {huisnummer} {plaats}"
    try:
        r = requests.get(
            PDOK_URL,
            params={"q": query, "fq": "type:adres", "rows": 5},
            headers={"Accept": "application/json"},
            timeout=10,
        )
        r.raise_for_status()
        docs = r.json().get("response", {}).get("docs", [])

        # Find best match: same street, closest huisnummer, has postcode
        best = None
        best_dist = 999
        for doc in docs:
            pc = doc.get("postcode", "").replace(" ", "")
            if not pc:
                continue

            doc_straat = (doc.get("straatnaam") or "").lower()
            doc_nr = int(doc.get("huisnummer") or 0)

            # Check street name matches (fuzzy: contains)
            if straat.lower() not in doc_straat and doc_straat not in straat.lower():
                continue

            dist = abs(doc_nr - target_nr)
            if dist < best_dist:
                best_dist = dist
                best = doc

        if best and best_dist <= 2:
            return {
                "postcode": best.get("postcode", "").replace(" ", ""),
                "huisnummer": best.get("huisnummer"),
                "huisletter": best.get("huisletter"),
                "adres": best.get("weergavenaam", ""),
            }

    except Exception as e:
        print(f"  Geocode error for '{query}': {e}")
    return None


def parse_denhaag():
    """Parse Den Haag CSV and geocode addresses."""
    csv_path = DATA_DIR / "denhaag_raw.csv"
    if not csv_path.exists():
        print("Den Haag raw CSV not found, skipping")
        return []

    with open(csv_path, "r", encoding="latin-1") as f:
        reader = csv.DictReader(f, delimiter=";")
        rows = list(reader)

    # Deduplicate by REGISTERNUMMER (keep first occurrence)
    seen = set()
    unique_rows = []
    for row in rows:
        regnr = row.get("REGISTERNUMMER", "")
        if regnr and regnr not in seen:
            seen.add(regnr)
            unique_rows.append(row)

    print(f"Den Haag: {len(unique_rows)} unique monuments")

    # Parse address from OMSCHRIJVING
    address_patterns = [
        # "van Straatnaam 123" or "van Straatnaam 123A"
        r'(?:van|:)\s+(?:het pand\s+)?(.+?)\s+(\d+\w*)\s*$',
        # "van Straatnaam 123 tm 125"
        r'(?:van|:)\s+(?:het pand\s+)?(.+?)\s+(\d+)\s+(?:t/?m|en)\s+\d+',
    ]

    results = []
    geocode_count = 0

    for row in unique_rows:
        omschr = row.get("OMSCHRIJVING", "")
        regnr = row.get("REGISTERNUMMER", "")

        # Try to extract street + number from omschrijving
        straat = None
        huisnr_str = None

        for pattern in address_patterns:
            m = re.search(pattern, omschr, re.IGNORECASE)
            if m:
                straat = m.group(1).strip()
                huisnr_str = m.group(2).strip()
                # Clean up street: remove leading "het pand", extra words
                straat = re.sub(r'^(het|de|een)\s+', '', straat, flags=re.IGNORECASE)
                break

        if not straat or not huisnr_str:
            # Try from URL
            url = row.get("MONUMENTENZORGSITE", "")
            m = re.search(r'/monumenten/([\w-]+)$', url)
            if m:
                slug = m.group(1)
                # Find last number in slug
                parts = re.match(r'^(.*?)-?(\d+\w*)$', slug)
                if parts:
                    straat = parts.group(1).replace('-', ' ').title()
                    huisnr_str = parts.group(2)

        if not straat or not huisnr_str:
            continue

        # Extract pure huisnummer (digits only) and huisletter
        nr_match = re.match(r'(\d+)([A-Za-z])?', huisnr_str)
        if not nr_match:
            continue

        huisnummer = nr_match.group(1)

        # Geocode via PDOK
        time.sleep(0.1)  # Light rate limiting for PDOK
        geo = geocode_address(straat, huisnr_str, "'s-Gravenhage")
        geocode_count += 1

        if geo and geo["postcode"]:
            results.append({
                "postcode": geo["postcode"],
                "huisnummer": geo["huisnummer"] or huisnummer,
                "huisletter": geo.get("huisletter") or "",
                "toevoeging": "",
                "adres": geo["adres"],
                "gemeente": "'s-Gravenhage",
                "omschrijving": omschr[:300],
                "bron_url": row.get("MONUMENTENZORGSITE", ""),
            })

        if geocode_count % 50 == 0:
            print(f"  Geocoded {geocode_count}/{len(unique_rows)}... ({len(results)} matched)")

    print(f"  Done: {len(results)} geocoded out of {len(unique_rows)}")
    return results


# Rijswijk monuments from gemeente PDF (beschermde gemeentelijke monumentenlijst)
RIJSWIJK_MONUMENTS = [
    ("Delftweg 40", "Woonhuis"),
    ("Delftweg 60", "Woonhuis"),
    ("Delftweg 64", "Woonhuis/stalgebouw"),
    ("Einsteinlaan 1", "Plaspoelpolder-gebouw (kantoor)"),
    ("Geestbrugweg 22-24", "Woonhuis"),
    ("Haagweg 11", "Hofje van Londen"),
    ("Haagweg 13", "Hofje van Londen"),
    ("Herenstraat 38", "Voorm. burgemeesterswoning"),
    ("Herenstraat 40", "Woonhuis"),
    ("Herenstraat 42", "Woonhuis"),
    ("Herenstraat 51", "Woonhuis"),
    ("Herenstraat 79", "Woonhuis"),
    ("Herenstraat 83", "Woonhuis"),
    ("Herenstraat 91", "Pakhuis Meestoof"),
    ("Herenstraat 95", "Voorm. gemeentehuis"),
    ("Herenstraat 97", "Woonhuis"),
    ("Herenstraat 99", "Woonhuis"),
    ("Herenstraat 101", "Woonhuis met winkel"),
    ("Herenstraat 103", "Woonhuis met winkel"),
    ("Julialaantje 4", "Woonhuis"),
    ("Julialaantje 6", "Woonhuis"),
    ("Kerklaan 2", "Ned. Herv. Kerk"),
    ("Kerklaan 10", "Pastorie Ned. Herv. Kerk"),
    ("Laan van Hoornwijck 4", "Landgoed Te Werve"),
    ("Lindelaan 2", "Woonhuis"),
    ("Oranjelaan 15", "Woonhuis"),
    ("Oranjelaan 31", "Villa"),
    ("Oranjelaan 41", "Villa"),
    ("Park Hoornwijck 1-22", "Portiekflats"),
    ("Prins Willem Alexanderlaan 1", "Voorm. raadhuis"),
    ("Schoolstraat 2", "Woonhuis"),
    ("Schoolstraat 8", "Woonhuis"),
    ("Sir Winston Churchilllaan 275", "RK Kerk"),
    ("Steenplaetsstraat 2", "Pakhuis"),
    ("Steenplaetsstraat 6", "Woonhuis"),
    ("Trekvlietplein 1", "Voorm. station Hoornbrug"),
    ("Van Vredenburchweg 32", "Woonhuis"),
    ("Wennetjessloot 1-3", "Woonhuizen"),
    ("Schoolstraat 4", "Woonhuis"),
    ("Schoolstraat 6", "Woonhuis"),
    ("Geestbrugkade 30", "Woonhuis"),
    ("Geestbrugkade 32", "Woonhuis"),
    ("Geestbrugkade 34", "Woonhuis"),
    ("Geestbrugkade 36", "Woonhuis"),
    ("Geestbrugkade 38", "Woonhuis"),
    ("Geestbrugkade 40", "Woonhuis"),
    ("Geestbrugkade 42", "Woonhuis"),
    ("Geestbrugkade 44", "Woonhuis"),
]


def parse_rijswijk():
    """Geocode Rijswijk monuments."""
    print(f"Rijswijk: {len(RIJSWIJK_MONUMENTS)} monuments")
    results = []

    for adres, omschrijving in RIJSWIJK_MONUMENTS:
        # Parse street + number
        m = re.match(r'^(.+?)\s+(\d+(?:-\d+)?)', adres)
        if not m:
            continue

        straat = m.group(1)
        huisnr_str = m.group(2).split("-")[0]  # Take first number from ranges

        time.sleep(0.1)
        geo = geocode_address(straat, huisnr_str, "Rijswijk")

        if geo and geo["postcode"]:
            results.append({
                "postcode": geo["postcode"],
                "huisnummer": geo["huisnummer"] or huisnr_str,
                "huisletter": geo.get("huisletter") or "",
                "toevoeging": "",
                "adres": geo["adres"],
                "gemeente": "Rijswijk",
                "omschrijving": omschrijving,
                "bron_url": "https://www.rijswijk.nl/monumenten",
            })
        else:
            print(f"  Failed to geocode: {adres}")

    print(f"  Done: {len(results)} geocoded")
    return results


def parse_leidschendam_voorburg():
    """Fetch Leidschendam-Voorburg monuments from Wikipedia and geocode."""
    pages = [
        "Lijst_van_gemeentelijke_monumenten_in_Leidschendam",
        "Lijst_van_gemeentelijke_monumenten_in_Voorburg",
        "Lijst_van_gemeentelijke_monumenten_in_Veur",
        "Lijst_van_gemeentelijke_monumenten_in_Stompwijk",
    ]

    all_entries = []
    for page in pages:
        entries = _fetch_wikipedia_monuments(page)
        all_entries.extend(entries)
        print(f"  {page}: {len(entries)} entries")

    print(f"Leidschendam-Voorburg: {len(all_entries)} total from Wikipedia")

    results = []
    for entry in all_entries:
        adres = entry.get("adres", "")
        if not adres:
            continue

        # Parse street + number
        m = re.match(r'^(.+?)\s+(\d+\w*)', adres)
        if not m:
            continue

        straat = m.group(1)
        huisnr_str = m.group(2)

        # Determine plaats from page
        plaats = "Leidschendam-Voorburg"
        for p in ["Leidschendam", "Voorburg", "Stompwijk"]:
            if p.lower() in entry.get("page", "").lower():
                plaats = p
                break
        if "Veur" in entry.get("page", ""):
            plaats = "Voorburg"  # Veur is now part of Voorburg

        time.sleep(0.1)
        geo = geocode_address(straat, huisnr_str, plaats)

        if geo and geo["postcode"]:
            results.append({
                "postcode": geo["postcode"],
                "huisnummer": geo["huisnummer"] or huisnr_str,
                "huisletter": geo.get("huisletter") or "",
                "toevoeging": "",
                "adres": geo["adres"],
                "gemeente": "Leidschendam-Voorburg",
                "omschrijving": entry.get("object", ""),
                "bron_url": f"https://nl.wikipedia.org/wiki/{entry.get('page', '')}",
            })

        if len(results) % 50 == 0 and len(results) > 0:
            print(f"  Geocoded... {len(results)} matched so far")

    print(f"  Done: {len(results)} geocoded")
    return results


def _fetch_wikipedia_monuments(page_title: str) -> list:
    """Fetch monument entries from a Wikipedia page using the API."""
    url = "https://nl.wikipedia.org/w/api.php"
    params = {
        "action": "parse",
        "page": page_title,
        "prop": "wikitext",
        "format": "json",
    }

    try:
        r = requests.get(
            url, params=params, timeout=30,
            headers={"User-Agent": "Woningzoeker/1.0 (gemeentelijke monumenten import)"},
        )
        r.raise_for_status()
        wikitext = r.json()["parse"]["wikitext"]["*"]
    except Exception as e:
        print(f"  Error fetching {page_title}: {e}")
        return []

    # Parse {{Tabelrij gemeentelijk monument}} templates
    entries = []
    # Match template calls
    pattern = r'\{\{Tabelrij gemeentelijk monument[^}]*\}\}'

    for match in re.finditer(pattern, wikitext, re.DOTALL):
        template = match.group(0)
        entry = {"page": page_title}

        # Extract named parameters
        for param_match in re.finditer(r'\|\s*(\w+)\s*=\s*([^|}\n]+)', template):
            key = param_match.group(1).strip()
            value = param_match.group(2).strip()
            if value:
                entry[key] = value

        if entry.get("adres") or entry.get("object"):
            entries.append(entry)

    return entries


def write_csv(results: list, filename: str):
    """Write results to CSV."""
    if not results:
        return

    output_path = DATA_DIR / filename
    fieldnames = ["postcode", "huisnummer", "huisletter", "toevoeging", "adres", "gemeente", "omschrijving", "bron_url"]

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"Written {len(results)} records to {output_path.name}")


def main():
    print("=" * 60)
    print("Geocoding gemeentelijke monumenten")
    print("=" * 60)

    # Rijswijk (smallest, good for testing)
    print("\n--- Rijswijk ---")
    rijswijk = parse_rijswijk()
    write_csv(rijswijk, "rijswijk.csv")

    # Leidschendam-Voorburg
    print("\n--- Leidschendam-Voorburg ---")
    lv = parse_leidschendam_voorburg()
    write_csv(lv, "leidschendam_voorburg.csv")

    # Den Haag (largest, takes longest)
    print("\n--- Den Haag ---")
    denhaag = parse_denhaag()
    write_csv(denhaag, "denhaag.csv")

    # Summary
    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Rijswijk: {len(rijswijk)} monuments")
    print(f"  Leidschendam-Voorburg: {len(lv)} monuments")
    print(f"  Den Haag: {len(denhaag)} monuments")
    print(f"  Total: {len(rijswijk) + len(lv) + len(denhaag)}")
    print("=" * 60)
    print("\nRun 'python import_gemeentelijke_monumenten.py' to import into database.")


if __name__ == "__main__":
    main()
