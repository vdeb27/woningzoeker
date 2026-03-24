"""Import PC6 postcodegebieden van Esri ArcGIS FeatureServer naar de database.

Haalt postcode-6 polygons op via de Esri Nederland FeatureServer,
transformeert van RD (EPSG:28992) naar WGS84, en slaat ze op in de
postcode6 tabel.

Gebruik:
    python import_postcode6_geometrie.py
    python import_postcode6_geometrie.py --dry-run
    python import_postcode6_geometrie.py --gemeente "Den Haag"

Databron:
    Esri Nederland Postcodevlakken PC6
    https://www.arcgis.com/home/item.html?id=6ddc8fa5f502495782cd031da6ad42da

Backup databron (PDOK CBS):
    https://service.pdok.nl/cbs/postcode6/2023/wfs/v1_0
"""

import argparse
import json
import sys
import time
from pathlib import Path

import requests
import yaml
from pyproj import Transformer
from sqlalchemy import text

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

from models.database import SessionLocal, init_db

# Esri ArcGIS FeatureServer
ARCGIS_URL = "https://services.arcgis.com/nSZVuSZjHpEZZbRo/arcgis/rest/services/Postcodevlakken_PC6/FeatureServer/0/query"

# Transformer: RD (EPSG:28992) → WGS84 (EPSG:4326)
transformer = Transformer.from_crs("EPSG:28992", "EPSG:4326", always_xy=True)

# Page size for ArcGIS requests (max 2000)
PAGE_SIZE = 2000

# CBS-namen mapping
GEMEENTE_CBS_NAAM = {
    "Den Haag": "'s-Gravenhage",
    "0518": "'s-Gravenhage",
    "1916": "Leidschendam-Voorburg",
    "0603": "Rijswijk",
}

# Fallback bounding boxes in RD (EPSG:28992) voor wanneer er geen buurtgeometrie is
GEMEENTE_BBOX_RD = {
    "'s-Gravenhage": (72000, 447000, 85000, 463000),
    "Leidschendam-Voorburg": (80000, 451000, 88000, 460000),
    "Rijswijk": (77000, 446000, 84000, 453000),
}


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


def get_gemeente_bbox_rd(db, gemeente_naam: str) -> tuple[float, float, float, float] | None:
    """Get the bounding box (in RD coordinates) for a gemeente from existing buurt geometries.

    Since buurten geometries are stored in WGS84, we transform the bbox back to RD
    for the ArcGIS spatial query.
    """
    # Get all buurt geometries for this gemeente
    rows = db.execute(
        text("SELECT geometrie FROM buurten WHERE gemeente_naam LIKE :naam AND geometrie IS NOT NULL"),
        {"naam": f"%{gemeente_naam}%"},
    ).fetchall()

    if not rows:
        return None

    # Find WGS84 bounding box from all buurt polygons
    min_lon, min_lat = float("inf"), float("inf")
    max_lon, max_lat = float("-inf"), float("-inf")

    reverse_transformer = Transformer.from_crs("EPSG:4326", "EPSG:28992", always_xy=True)

    for (geom_json,) in rows:
        geom = geom_json if isinstance(geom_json, dict) else json.loads(geom_json)
        coords = geom.get("coordinates", [])

        # Flatten all coordinate pairs
        def extract_points(c):
            if isinstance(c[0], (int, float)):
                return [c]
            result = []
            for item in c:
                result.extend(extract_points(item))
            return result

        for lon, lat in extract_points(coords):
            min_lon = min(min_lon, lon)
            max_lon = max(max_lon, lon)
            min_lat = min(min_lat, lat)
            max_lat = max(max_lat, lat)

    # Transform WGS84 bbox to RD with some padding
    padding = 0.005  # ~500m in degrees
    rd_min_x, rd_min_y = reverse_transformer.transform(min_lon - padding, min_lat - padding)
    rd_max_x, rd_max_y = reverse_transformer.transform(max_lon + padding, max_lat + padding)

    return (rd_min_x, rd_min_y, rd_max_x, rd_max_y)


def fetch_pc6_page(bbox_rd: tuple[float, float, float, float], offset: int = 0) -> list[dict]:
    """Fetch a page of PC6 features from the ArcGIS FeatureServer."""
    params = {
        "where": "1=1",
        "geometry": f"{bbox_rd[0]},{bbox_rd[1]},{bbox_rd[2]},{bbox_rd[3]}",
        "geometryType": "esriGeometryEnvelope",
        "inSR": "28992",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "PC6,Aantal_adreslocaties",
        "f": "geojson",
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
    }

    response = requests.get(ARCGIS_URL, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()
    return data.get("features", [])


def import_postcode6(dry_run: bool = False, gemeente_filter: str | None = None):
    """Import PC6 postcodegebieden voor doelgemeenten."""
    init_db()
    db = SessionLocal()

    try:
        # Load target municipalities from config
        config_path = Path(__file__).parent.parent / "config" / "areas.yaml"
        with open(config_path) as f:
            config = yaml.safe_load(f)

        municipalities = config.get("municipalities", [])
        if gemeente_filter:
            municipalities = [
                m for m in municipalities
                if gemeente_filter.lower() in m["name"].lower()
                or gemeente_filter in m.get("code", "")
            ]

        if not municipalities:
            print("Geen gemeenten gevonden.")
            return

        total_imported = 0

        for muni in municipalities:
            muni_name = muni["name"]
            cbs_naam = GEMEENTE_CBS_NAAM.get(muni_name, muni_name)
            cbs_naam_code = GEMEENTE_CBS_NAAM.get(muni.get("code", ""), cbs_naam)
            # Use whichever resolves
            lookup_naam = cbs_naam_code if cbs_naam_code != muni_name else cbs_naam

            print(f"\n  Gemeente {muni_name} ({lookup_naam})...")

            bbox = get_gemeente_bbox_rd(db, lookup_naam)
            if not bbox:
                # Fallback to hardcoded bbox
                bbox = GEMEENTE_BBOX_RD.get(lookup_naam)
                if not bbox:
                    print(f"    Geen buurtgeometrie of fallback BBOX gevonden — sla over")
                    continue
                print(f"    Gebruik fallback BBOX")

            print(f"    BBOX (RD): {bbox[0]:.0f}, {bbox[1]:.0f}, {bbox[2]:.0f}, {bbox[3]:.0f}")

            # Paginate through all PC6 features in this bbox
            offset = 0
            gemeente_count = 0

            while True:
                try:
                    features = fetch_pc6_page(bbox, offset)
                except Exception as e:
                    print(f"    FOUT bij ophalen (offset {offset}): {e}")
                    break

                if not features:
                    break

                for feature in features:
                    props = feature.get("properties", {})
                    postcode = props.get("PC6", "")
                    if not postcode:
                        continue

                    geometry = feature.get("geometry")
                    if not geometry:
                        continue

                    # ArcGIS f=geojson already returns WGS84 coordinates
                    wgs84_geometry = geometry
                    aantal_adressen = props.get("Aantal_adreslocaties")

                    if not dry_run:
                        # Upsert: insert or update existing
                        existing = db.execute(
                            text("SELECT id FROM postcode6 WHERE postcode = :pc"),
                            {"pc": postcode},
                        ).fetchone()

                        if existing:
                            db.execute(
                                text(
                                    "UPDATE postcode6 SET geometrie = :geom, "
                                    "gemeente_naam = :gem, aantal_adressen = :aantal "
                                    "WHERE postcode = :pc"
                                ),
                                {
                                    "geom": json.dumps(wgs84_geometry),
                                    "gem": lookup_naam,
                                    "aantal": aantal_adressen,
                                    "pc": postcode,
                                },
                            )
                        else:
                            db.execute(
                                text(
                                    "INSERT INTO postcode6 (postcode, gemeente_naam, aantal_adressen, geometrie) "
                                    "VALUES (:pc, :gem, :aantal, :geom)"
                                ),
                                {
                                    "pc": postcode,
                                    "gem": lookup_naam,
                                    "aantal": aantal_adressen,
                                    "geom": json.dumps(wgs84_geometry),
                                },
                            )

                    gemeente_count += 1

                offset += PAGE_SIZE
                if len(features) < PAGE_SIZE:
                    break

                time.sleep(0.5)  # Rate limiting

            # Commit per gemeente
            if not dry_run and gemeente_count > 0:
                db.commit()

            print(f"    {gemeente_count} postcodegebieden {'gevonden' if dry_run else 'geimporteerd'}")
            total_imported += gemeente_count

        if dry_run:
            print(f"\n[DRY RUN] {total_imported} postcodegebieden zouden geimporteerd worden")
        else:
            print(f"\n{total_imported} postcodegebieden geimporteerd")

    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Import PC6 postcodegebieden van Esri ArcGIS")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Toon wat er zou worden gedaan zonder te schrijven",
    )
    parser.add_argument(
        "--gemeente",
        type=str,
        default=None,
        help="Alleen postcodes in deze gemeente (naam of code)",
    )
    args = parser.parse_args()

    print("=== PC6 postcodegebieden importeren van Esri ArcGIS ===\n")
    import_postcode6(dry_run=args.dry_run, gemeente_filter=args.gemeente)


if __name__ == "__main__":
    main()
