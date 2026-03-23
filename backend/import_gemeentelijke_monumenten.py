"""
Import gemeentelijke monumenten into the database.

Reads CSV files from data/gemeentelijke_monumenten/ and imports them
into the gemeentelijke_monumenten table.

CSV format: postcode,huisnummer,huisletter,toevoeging,adres,gemeente,omschrijving,bron_url

Usage:
    cd backend && source venv/bin/activate
    python import_gemeentelijke_monumenten.py
"""

import csv
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from models.database import SessionLocal, init_db
from models.gemeentelijk_monument import GemeentelijkMonument


DATA_DIR = Path(__file__).parent.parent / "data" / "gemeentelijke_monumenten"


def import_csv(csv_path: Path, db) -> int:
    """Import a single CSV file into the database.

    Returns number of records imported.
    """
    count = 0
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            postcode = row.get("postcode", "").replace(" ", "").upper().strip()
            huisnummer_str = row.get("huisnummer", "").strip()

            if not postcode or not huisnummer_str:
                continue

            try:
                huisnummer = int(huisnummer_str)
            except ValueError:
                print(f"  Skipping row with invalid huisnummer: {huisnummer_str}")
                continue

            # Check for duplicates
            existing = (
                db.query(GemeentelijkMonument)
                .filter(
                    GemeentelijkMonument.postcode == postcode,
                    GemeentelijkMonument.huisnummer == huisnummer,
                    GemeentelijkMonument.huisletter == (row.get("huisletter", "").strip() or None),
                )
                .first()
            )
            if existing:
                continue

            monument = GemeentelijkMonument(
                postcode=postcode,
                huisnummer=huisnummer,
                huisletter=row.get("huisletter", "").strip() or None,
                toevoeging=row.get("toevoeging", "").strip() or None,
                adres=row.get("adres", "").strip() or None,
                gemeente=row.get("gemeente", "").strip() or None,
                omschrijving=row.get("omschrijving", "").strip() or None,
                bron_url=row.get("bron_url", "").strip() or None,
            )
            db.add(monument)
            count += 1

    return count


def main():
    print("Initializing database...")
    init_db()

    db = SessionLocal()
    try:
        csv_files = sorted(DATA_DIR.glob("*.csv"))

        if not csv_files:
            print(f"No CSV files found in {DATA_DIR}")
            print(f"Place CSV files with columns: postcode,huisnummer,huisletter,toevoeging,adres,gemeente,omschrijving,bron_url")
            return

        total = 0
        for csv_path in csv_files:
            print(f"Importing {csv_path.name}...")
            count = import_csv(csv_path, db)
            print(f"  {count} records imported")
            total += count

        db.commit()
        print(f"\nTotal: {total} gemeentelijke monumenten imported")

        # Show summary
        for gemeente in ["'s-Gravenhage", "Leidschendam-Voorburg", "Rijswijk"]:
            n = db.query(GemeentelijkMonument).filter(
                GemeentelijkMonument.gemeente == gemeente
            ).count()
            if n > 0:
                print(f"  {gemeente}: {n}")

    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
