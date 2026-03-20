"""Import buurtgrenzen van PDOK WFS naar de database.

Haalt CBS buurtgrenzen op via PDOK WFS en slaat ze op als GeoJSON
in de geometrie kolom van de buurten tabel.

Gebruik:
    python import_buurt_geometrie.py
    python import_buurt_geometrie.py --dry-run
    python import_buurt_geometrie.py --gemeente "'s-Gravenhage"
"""

import argparse
import json
import sys
import time
from pathlib import Path

import requests
from pyproj import Transformer
from sqlalchemy import text

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

from models.database import SessionLocal


# PDOK WFS endpoint voor CBS Wijken en Buurten 2023
WFS_URL = "https://service.pdok.nl/cbs/wijkenbuurten/2023/wfs/v1_0"

# Transformer: RD (EPSG:28992) → WGS84 (EPSG:4326)
transformer = Transformer.from_crs("EPSG:28992", "EPSG:4326", always_xy=True)

# Page size for WFS requests
PAGE_SIZE = 500


def transform_coordinates(coords, geom_type: str):
    """Transform coordinates from RD to WGS84 recursively."""
    if geom_type == "Point":
        lon, lat = transformer.transform(coords[0], coords[1])
        return [round(lon, 7), round(lat, 7)]
    elif geom_type in ("LineString", "MultiPoint"):
        return [transform_coordinates(c, "Point") for c in coords]
    elif geom_type in ("Polygon", "MultiLineString"):
        return [transform_coordinates(ring, "LineString") for ring in coords]
    elif geom_type in ("MultiPolygon",):
        return [transform_coordinates(poly, "Polygon") for poly in coords]
    return coords


def transform_geometry(geometry: dict) -> dict:
    """Transform a GeoJSON geometry from RD to WGS84."""
    geom_type = geometry["type"]
    transformed_coords = transform_coordinates(geometry["coordinates"], geom_type)
    return {"type": geom_type, "coordinates": transformed_coords}


def fetch_buurten_bulk(gemeente_code: str | None = None, start_index: int = 0) -> list[dict]:
    """Fetch buurt geometries from PDOK WFS in bulk using XML FILTER.

    CQL_FILTER is ignored by this WFS server, so we use the XML FILTER parameter.
    """
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": "wijkenbuurten:buurten",
        "outputFormat": "json",
        "srsName": "EPSG:28992",
        "count": PAGE_SIZE,
        "startIndex": start_index,
    }

    if gemeente_code:
        xml_filter = (
            f'<Filter><PropertyIsEqualTo>'
            f'<PropertyName>gemeentecode</PropertyName>'
            f'<Literal>{gemeente_code}</Literal>'
            f'</PropertyIsEqualTo></Filter>'
        )
        params["FILTER"] = xml_filter

    response = requests.get(WFS_URL, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()
    return data.get("features", [])


def fetch_buurt_single(buurtcode: str) -> dict | None:
    """Fetch a single buurt geometry from PDOK WFS using XML FILTER."""
    xml_filter = (
        f'<Filter><PropertyIsEqualTo>'
        f'<PropertyName>buurtcode</PropertyName>'
        f'<Literal>{buurtcode}</Literal>'
        f'</PropertyIsEqualTo></Filter>'
    )
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": "wijkenbuurten:buurten",
        "outputFormat": "json",
        "srsName": "EPSG:28992",
        "FILTER": xml_filter,
    }

    response = requests.get(WFS_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    features = data.get("features", [])
    if features:
        return features[0]
    return None


def import_geometrie(dry_run: bool = False, gemeente_filter: str | None = None):
    """Import buurtgrenzen voor alle buurten in de database."""
    db = SessionLocal()
    updated = 0
    not_found = 0

    try:
        # First clear all bad geometry (the "Buitenland" polygon that was saved for everything)
        bad_count = db.execute(text(
            "SELECT COUNT(*) FROM buurten WHERE geometrie IS NOT NULL"
        )).scalar()
        if bad_count > 0:
            # Check if all geometries are the same (the bug)
            distinct = db.execute(text(
                "SELECT COUNT(DISTINCT geometrie) FROM buurten WHERE geometrie IS NOT NULL"
            )).scalar()
            if distinct == 1:
                print(f"Alle {bad_count} buurten hebben dezelfde (foutieve) geometrie — wordt gewist.")
                if not dry_run:
                    db.execute(text("UPDATE buurten SET geometrie = NULL"))
                    db.commit()

        # Get gemeente codes we need to fetch
        if gemeente_filter:
            gemeente_rows = db.execute(text(
                "SELECT DISTINCT gemeente_code, gemeente_naam FROM buurten "
                "WHERE gemeente_naam LIKE :naam"
            ), {"naam": f"%{gemeente_filter}%"}).fetchall()
        else:
            gemeente_rows = db.execute(text(
                "SELECT DISTINCT gemeente_code, gemeente_naam FROM buurten "
                "WHERE geometrie IS NULL"
            )).fetchall()

        if not gemeente_rows:
            print("Geen buurten zonder geometrie gevonden.")
            return

        # Get set of buurt codes in our DB
        if gemeente_filter:
            db_codes = set(r[0] for r in db.execute(text(
                "SELECT code FROM buurten WHERE gemeente_naam LIKE :naam"
            ), {"naam": f"%{gemeente_filter}%"}).fetchall())
        else:
            db_codes = set(r[0] for r in db.execute(text(
                "SELECT code FROM buurten WHERE geometrie IS NULL"
            )).fetchall())

        print(f"{len(db_codes)} buurten in {len(gemeente_rows)} gemeenten ophalen\n")

        for gm_code, gm_naam in gemeente_rows:
            if not gm_code:
                continue
            # PDOK uses GM prefix for gemeente codes
            pdok_gm_code = f"GM{gm_code}" if not gm_code.startswith("GM") else gm_code
            print(f"  Gemeente {gm_naam} ({pdok_gm_code})...")
            start_index = 0
            gemeente_count = 0

            while True:
                try:
                    features = fetch_buurten_bulk(pdok_gm_code, start_index)
                except Exception as e:
                    print(f"    FOUT bij ophalen: {e}")
                    break

                if not features:
                    break

                for feature in features:
                    props = feature.get("properties", {})
                    buurtcode = props.get("buurtcode", "")

                    if buurtcode not in db_codes:
                        continue

                    geometry = feature.get("geometry")
                    if not geometry:
                        not_found += 1
                        continue

                    wgs84_geometry = transform_geometry(geometry)

                    if not dry_run:
                        db.execute(
                            text("UPDATE buurten SET geometrie = :geom WHERE code = :code"),
                            {"geom": json.dumps(wgs84_geometry), "code": buurtcode},
                        )

                    updated += 1
                    gemeente_count += 1

                start_index += PAGE_SIZE
                if len(features) < PAGE_SIZE:
                    break

                time.sleep(0.5)  # Rate limiting

            # Commit per gemeente to preserve progress
            if not dry_run and gemeente_count > 0:
                db.commit()
                print(f"    {gemeente_count} buurten bijgewerkt")

        # Handle remaining buurten that weren't found in bulk (different gemeente codes)
        remaining = db.execute(text(
            "SELECT code, naam FROM buurten WHERE geometrie IS NULL"
        )).fetchall()

        if remaining and not gemeente_filter:
            print(f"\n  {len(remaining)} resterende buurten individueel ophalen...")
            batch_count = 0
            for code, naam in remaining:
                try:
                    feature = fetch_buurt_single(code)
                except Exception as e:
                    print(f"    {code} FOUT: {e}")
                    continue

                if not feature or not feature.get("geometry"):
                    not_found += 1
                    continue

                wgs84_geometry = transform_geometry(feature["geometry"])

                if not dry_run:
                    db.execute(
                        text("UPDATE buurten SET geometrie = :geom WHERE code = :code"),
                        {"geom": json.dumps(wgs84_geometry), "code": code},
                    )

                updated += 1
                batch_count += 1

                # Commit every 100 buurten
                if batch_count % 100 == 0 and not dry_run:
                    db.commit()
                    print(f"    {batch_count}/{len(remaining)} verwerkt...")

                time.sleep(0.3)

            if not dry_run and batch_count > 0:
                db.commit()

        if dry_run:
            print(f"\n[DRY RUN] {updated} buurten zouden bijgewerkt worden")
        else:
            print(f"\n{updated} buurten bijgewerkt met geometrie")

        if not_found:
            print(f"{not_found} buurten niet gevonden in PDOK")

    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Import buurtgrenzen van PDOK WFS")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Toon wat er zou worden gedaan zonder te schrijven",
    )
    parser.add_argument(
        "--gemeente",
        type=str,
        default=None,
        help="Alleen buurten in deze gemeente (naam of deel van naam)",
    )
    args = parser.parse_args()

    print("=== Buurtgrenzen importeren van PDOK WFS ===\n")
    import_geometrie(dry_run=args.dry_run, gemeente_filter=args.gemeente)


if __name__ == "__main__":
    main()
