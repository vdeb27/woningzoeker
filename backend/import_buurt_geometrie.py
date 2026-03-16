"""Import buurtgrenzen van PDOK WFS naar de database.

Haalt CBS buurtgrenzen op via PDOK WFS en slaat ze op als GeoJSON
in de geometrie kolom van de buurten tabel.

Gebruik:
    python import_buurt_geometrie.py
    python import_buurt_geometrie.py --dry-run
"""

import argparse
import json
import sys
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


def fetch_buurt_wfs(buurtcode: str) -> dict | None:
    """Fetch a single buurt geometry from PDOK WFS."""
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": "wijkenbuurten:buurten",
        "outputFormat": "json",
        "srsName": "EPSG:28992",
        "CQL_FILTER": f"buurtcode='{buurtcode}'",
    }

    response = requests.get(WFS_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    features = data.get("features", [])
    if features:
        return features[0]
    return None


def import_geometrie(dry_run: bool = False):
    """Import buurtgrenzen voor alle buurten in de database."""
    db = SessionLocal()
    updated = 0
    not_found = 0

    try:
        # Get all buurt codes from our database
        rows = db.execute(
            text("SELECT code, naam FROM buurten WHERE geometrie IS NULL")
        ).fetchall()

        if not rows:
            print("Alle buurten hebben al geometrie, niets te doen.")
            # Also check if there are any buurten at all
            total = db.execute(text("SELECT COUNT(*) FROM buurten")).scalar()
            print(f"({total} buurten in database)")
            return

        print(f"{len(rows)} buurten zonder geometrie gevonden\n")

        for code, naam in rows:
            print(f"  Ophalen {code} ({naam})...", end=" ")
            try:
                feature = fetch_buurt_wfs(code)
            except Exception as e:
                print(f"FOUT: {e}")
                continue

            if not feature or not feature.get("geometry"):
                print("niet gevonden in PDOK")
                not_found += 1
                continue

            wgs84_geometry = transform_geometry(feature["geometry"])

            if not dry_run:
                db.execute(
                    text("UPDATE buurten SET geometrie = :geom WHERE code = :code"),
                    {"geom": json.dumps(wgs84_geometry), "code": code},
                )

            print("OK")
            updated += 1

        if not dry_run:
            db.commit()
            print(f"\n{updated} buurten bijgewerkt met geometrie")
        else:
            print(f"\n[DRY RUN] {updated} buurten zouden bijgewerkt worden")

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
    args = parser.parse_args()

    print("=== Buurtgrenzen importeren van PDOK WFS ===\n")
    import_geometrie(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
