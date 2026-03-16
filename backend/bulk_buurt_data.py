"""
Bulk download and scoring of neighborhood data.

Fetches data from all collectors (CBS Kerncijfers, Leefbaarometer, CBS Nabijheid, RIVM),
merges into a single dataset, calculates scores, and writes to the database.

Usage:
    cd backend && source venv/bin/activate
    python bulk_buurt_data.py [--skip-rivm] [--clear-cache]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import text

# Ensure backend is on path
sys.path.insert(0, str(Path(__file__).parent))

from models.database import SessionLocal, init_db
from models.buurt import Buurt
from collectors.cbs_buurt_collector import CBSBuurtCollector, HOUSING_COLUMNS
from collectors.leefbaarometer_collector import create_leefbaarometer_collector
from collectors.cbs_nabijheid_collector import create_cbs_nabijheid_collector
from collectors.rivm_collector import create_rivm_collector
from collectors.cbs_extra_collector import create_cbs_extra_collector
from services.scoring import ScoringService


def fetch_all_data(skip_rivm: bool = False):
    """Fetch data from all collectors and merge."""
    print("=== CBS Kerncijfers laden ===")
    cbs = CBSBuurtCollector()
    buurten = cbs.get_all_buurten()
    print(f"  {len(buurten)} buurten geladen")

    print("\n=== Leefbaarometer laden ===")
    try:
        lbm = create_leefbaarometer_collector()
        lbm_data = lbm.get_all()
        print(f"  {len(lbm_data)} buurten met Leefbaarometer data")
    except Exception as e:
        print(f"  Leefbaarometer laden mislukt: {e}")
        lbm_data = {}

    print("\n=== CBS Nabijheid laden ===")
    try:
        nabijheid = create_cbs_nabijheid_collector()
        nabijheid_data = nabijheid.get_all()
        print(f"  {len(nabijheid_data)} buurten met nabijheiddata")
    except Exception as e:
        print(f"  CBS Nabijheid laden mislukt: {e}")
        nabijheid_data = {}

    print("\n=== CBS Extra (misdrijven, arbeid, SES, opleiding, bodem) laden ===")
    try:
        extra = create_cbs_extra_collector()
        extra_data = extra.get_all_buurten()
        print(f"  {len(extra_data)} buurten met extra CBS data")
    except Exception as e:
        print(f"  CBS Extra laden mislukt: {e}")
        extra_data = {}

    rivm_data = {}
    if not skip_rivm:
        print("\n=== RIVM Atlas laden ===")
        try:
            rivm = create_rivm_collector()
            rivm_data = rivm.get_all()
            print(f"  {len(rivm_data)} buurten met RIVM data (uit cache)")
            if not rivm_data:
                print("  Geen RIVM data in cache. Gebruik bulk_buurt_data.py --fetch-rivm om te downloaden.")
        except Exception as e:
            print(f"  RIVM laden mislukt: {e}")
    else:
        print("\n=== RIVM overgeslagen (--skip-rivm) ===")

    return buurten, lbm_data, nabijheid_data, rivm_data, extra_data


def merge_and_score(buurten, lbm_data, nabijheid_data, rivm_data, extra_data=None):
    """Merge all data sources and calculate scores."""
    print("\n=== Data samenvoegen ===")
    extra_data = extra_data or {}

    records = []
    for buurt in buurten:
        record = {
            "buurt_code": buurt.buurt_code,
            "buurt_naam": buurt.buurt_naam,
            "gemeente_code": buurt.gemeente_code,
            "gemeente_naam": buurt.gemeente_naam,
            "gem_woz_waarde": buurt.gem_woz_waarde,
            "koopwoningen_pct": buurt.koopwoningen_pct,
            "huurwoningen_pct": buurt.huurwoningen_pct,
            "gem_inkomen": buurt.gem_inkomen,
            "huishoudens_laag_inkomen_pct": buurt.huishoudens_laag_inkomen_pct,
        }

        # Merge CBS extended indicators
        indicatoren = dict(buurt.indicatoren) if buurt.indicatoren else {}

        # Merge Leefbaarometer
        lbm = lbm_data.get(buurt.buurt_code)
        if lbm:
            record["lbm"] = lbm.lbm_score
            record["leefbaarometer_fysiek"] = lbm.fysieke_omgeving
            record["leefbaarometer_voorzieningen"] = lbm.voorzieningen
            record["leefbaarometer_veiligheid"] = lbm.veiligheid
            record["leefbaarometer_bevolking"] = lbm.bevolkingssamenstelling
            record["leefbaarometer_woningen"] = lbm.woningvoorraad
            indicatoren["leefbaarometer_veiligheid"] = lbm.veiligheid
            indicatoren["leefbaarometer_fysiek"] = lbm.fysieke_omgeving

        # Merge Nabijheid
        nab = nabijheid_data.get(buurt.buurt_code)
        if nab:
            for key, val in nab.afstanden.items():
                indicatoren[key] = val

        # Merge RIVM
        rivm = rivm_data.get(buurt.buurt_code)
        if rivm:
            if rivm.no2_concentratie is not None:
                indicatoren["no2_concentratie"] = rivm.no2_concentratie
            if rivm.pm25_concentratie is not None:
                indicatoren["pm25_concentratie"] = rivm.pm25_concentratie
            if rivm.pm10_concentratie is not None:
                indicatoren["pm10_concentratie"] = rivm.pm10_concentratie
            if rivm.geluid_weg_lden is not None:
                indicatoren["geluid_weg_lden"] = rivm.geluid_weg_lden

        # Merge CBS Extra (misdrijven, arbeid, SES, opleiding, bodemgebruik)
        extra = extra_data.get(buurt.buurt_code)
        if extra:
            for key, val in extra.items():
                indicatoren[key] = val

        record["indicatoren"] = indicatoren

        # Add indicator values as flat columns for scoring
        for key, val in indicatoren.items():
            if val is not None:
                record[key] = val

        records.append(record)

    df = pd.DataFrame(records)
    print(f"  {len(df)} buurten, {len(df.columns)} kolommen")

    # Calculate scores
    print("\n=== Scores berekenen ===")
    scorer = ScoringService()
    scored = scorer.calculate_scores(df)

    # Show score distribution
    if "score" in scored.columns:
        valid_scores = scored["score"].dropna()
        if len(valid_scores) > 0:
            print(f"  Score range: {valid_scores.min():.2f} - {valid_scores.max():.2f}")
            print(f"  Score gemiddelde: {valid_scores.mean():.2f}")
            print(f"  Buurten met score: {len(valid_scores)}/{len(scored)}")

    # Show category scores
    for cat_id in scorer.categories:
        col = f"score_{cat_id}"
        if col in scored.columns:
            valid = scored[col].dropna()
            if len(valid) > 0:
                print(f"  {cat_id}: {valid.mean():.2f} avg ({len(valid)} buurten)")

    return scored


def write_to_database(scored_df: pd.DataFrame):
    """Write scored buurt data to database."""
    print("\n=== Database schrijven ===")

    init_db()
    session = SessionLocal()

    try:
        # Drop and recreate buurten data (all data is re-downloadable)
        existing = session.query(Buurt).count()
        print(f"  Bestaande buurten: {existing}")

        updated = 0
        created = 0

        for _, row in scored_df.iterrows():
            code = row.get("buurt_code", "")
            if not code:
                continue

            buurt = session.query(Buurt).filter(Buurt.code == code).first()
            if not buurt:
                buurt = Buurt(code=code)
                session.add(buurt)
                created += 1
            else:
                updated += 1

            buurt.naam = row.get("buurt_naam", "")
            buurt.gemeente_code = row.get("gemeente_code", "")
            buurt.gemeente_naam = row.get("gemeente_naam", "")

            # CBS core fields
            ind = row.get("indicatoren", {})
            buurt.inwoners = _safe_int(ind.get("inwoners"))
            buurt.huishoudens = _safe_int(ind.get("huishoudens_totaal"))
            buurt.gemiddeld_inkomen = row.get("gem_inkomen")
            buurt.woz_waarde = row.get("gem_woz_waarde")
            buurt.gasverbruik = _safe_float(ind.get("gem_gasverbruik"))
            buurt.elektraverbruik = _safe_float(ind.get("gem_elektraverbruik"))
            buurt.arbeidsparticipatie = _safe_float(ind.get("arbeidsparticipatie"))

            # Extended indicators (JSON)
            buurt.indicatoren = ind if ind else None

            # Scores
            buurt.score_totaal = _safe_float(row.get("score"))
            buurt.score_coverage = _safe_float(row.get("score_coverage"))
            buurt.score_inkomen = _safe_float(row.get("score_inkomen"))
            buurt.score_veiligheid = _safe_float(row.get("score_veiligheid"))
            buurt.score_voorzieningen = _safe_float(row.get("score_voorzieningen"))
            buurt.score_woningen = _safe_float(row.get("score_woningen"))
            buurt.score_bereikbaarheid = _safe_float(row.get("score_bereikbaarheid"))
            buurt.score_leefbaarheid = _safe_float(row.get("score_leefbaarheid"))
            buurt.score_energie = _safe_float(row.get("score_energie"))
            buurt.score_demografie = _safe_float(row.get("score_demografie"))

            # Leefbaarometer
            buurt.leefbaarometer_score = _safe_float(row.get("lbm"))
            buurt.leefbaarometer_fysiek = _safe_float(row.get("leefbaarometer_fysiek"))
            buurt.leefbaarometer_voorzieningen = _safe_float(row.get("leefbaarometer_voorzieningen"))
            buurt.leefbaarometer_veiligheid = _safe_float(row.get("leefbaarometer_veiligheid"))
            buurt.leefbaarometer_bevolking = _safe_float(row.get("leefbaarometer_bevolking"))
            buurt.leefbaarometer_woningen = _safe_float(row.get("leefbaarometer_woningen"))

            buurt.data_jaar = 2024

        session.commit()
        print(f"  {created} aangemaakt, {updated} bijgewerkt")

    except Exception as e:
        session.rollback()
        print(f"  FOUT: {e}")
        raise
    finally:
        session.close()


def _safe_float(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def fetch_rivm_fresh():
    """Fetch RIVM data for all target buurten using centroids from PDOK WFS."""
    print("\n=== RIVM Atlas ophalen (vers) ===")
    print("  Buurt centroids ophalen via PDOK WFS...")

    from collectors.cbs_buurt_collector import TARGET_MUNICIPALITIES

    centroids = {}
    for muni_code in TARGET_MUNICIPALITIES:
        try:
            url = "https://service.pdok.nl/cbs/wijkenbuurten/2023/wfs/v1_0"
            ogc_filter = (
                '<Filter xmlns="http://www.opengis.net/ogc">'
                '<PropertyIsLike wildCard="*" singleChar="." escapeChar="!">'
                '<PropertyName>buurtcode</PropertyName>'
                f'<Literal>BU{muni_code}*</Literal>'
                '</PropertyIsLike>'
                '</Filter>'
            )
            params = {
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeName": "wijkenbuurten:buurten",
                "outputFormat": "application/json",
                "filter": ogc_filter,
                "propertyName": "buurtcode,geom",
            }
            resp = requests.get(url, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()

            for feature in data.get("features", []):
                code = feature.get("properties", {}).get("buurtcode", "")
                geom = feature.get("geometry")
                if code and geom:
                    # Calculate centroid from geometry
                    coords = _extract_centroid(geom)
                    if coords:
                        centroids[code] = coords

            print(f"  {muni_code}: {sum(1 for c in centroids if c.startswith(f'BU{muni_code}'))} buurten")
        except Exception as e:
            print(f"  Fout bij {muni_code}: {e}")

    print(f"  Totaal: {len(centroids)} centroids")

    if centroids:
        rivm = create_rivm_collector()
        print(f"  RIVM data ophalen voor {len(centroids)} buurten (dit duurt lang)...")
        rivm.fetch_for_centroids(centroids)
        print(f"  Klaar: {len(rivm.get_all())} buurten met RIVM data")


def _extract_centroid(geom: dict) -> tuple | None:
    """Calculate approximate centroid from GeoJSON geometry."""
    gtype = geom.get("type", "")
    coords = geom.get("coordinates", [])

    def avg_coords(coord_list):
        """Average all coordinate pairs in a nested list."""
        flat = []
        _flatten_coords(coord_list, flat)
        if not flat:
            return None
        avg_lon = sum(c[0] for c in flat) / len(flat)
        avg_lat = sum(c[1] for c in flat) / len(flat)
        return (avg_lat, avg_lon)

    def _flatten_coords(nested, result):
        if not nested:
            return
        if isinstance(nested[0], (int, float)):
            result.append(nested)
        else:
            for item in nested:
                _flatten_coords(item, result)

    return avg_coords(coords)


def main():
    parser = argparse.ArgumentParser(description="Bulk download buurtdata")
    parser.add_argument("--skip-rivm", action="store_true", help="Skip RIVM Atlas data")
    parser.add_argument("--fetch-rivm", action="store_true",
                        help="Fetch fresh RIVM data (slow, uses WMS per buurt)")
    parser.add_argument("--clear-cache", action="store_true", help="Clear cache first")
    args = parser.parse_args()

    if args.clear_cache:
        import shutil
        cache_dirs = [
            Path(__file__).parent.parent / "data" / "cache" / d
            for d in ["leefbaarometer", "cbs_nabijheid", "rivm", "cbs_extra"]
        ]
        for d in cache_dirs:
            if d.exists():
                shutil.rmtree(d)
                print(f"Cache verwijderd: {d}")

    if args.fetch_rivm:
        fetch_rivm_fresh()

    buurten, lbm_data, nabijheid_data, rivm_data, extra_data = fetch_all_data(
        skip_rivm=args.skip_rivm
    )
    scored = merge_and_score(buurten, lbm_data, nabijheid_data, rivm_data, extra_data)
    write_to_database(scored)

    print("\n=== Klaar! ===")


if __name__ == "__main__":
    main()
