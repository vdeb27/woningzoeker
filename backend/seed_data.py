#!/usr/bin/env python3
"""Seed the database with initial data from CBS and sample properties."""

import sys
from pathlib import Path

# Add backend directory to path
backend_dir = Path(__file__).parent
sys.path.insert(0, str(backend_dir))

from models.database import SessionLocal, init_db
from models import Buurt, Woning
from collectors.cbs_collector import download_cbs_dataset, filter_for_region, get_column_for_role

def seed_buurten():
    """Load neighborhoods - using sample data for now."""
    print("Loading sample buurten for Den Haag, Leidschendam-Voorburg, Rijswijk...")
    # CBS API has strict query limits, using curated sample data instead
    return create_sample_buurten()


def create_sample_buurten():
    """Create sample buurten when CBS data is unavailable."""
    return [
        Buurt(code="BU05181234", naam="Centrum", gemeente_naam="Den Haag", inwoners=5000, data_jaar=2024),
        Buurt(code="BU05181235", naam="Scheveningen", gemeente_naam="Den Haag", inwoners=8000, data_jaar=2024),
        Buurt(code="BU05181236", naam="Loosduinen", gemeente_naam="Den Haag", inwoners=6000, data_jaar=2024),
        Buurt(code="BU19160001", naam="Voorburg Oud", gemeente_naam="Leidschendam-Voorburg", inwoners=4500, data_jaar=2024),
        Buurt(code="BU19160002", naam="Leidschendam Centrum", gemeente_naam="Leidschendam-Voorburg", inwoners=5500, data_jaar=2024),
        Buurt(code="BU06030001", naam="Rijswijk Centrum", gemeente_naam="Rijswijk", inwoners=7000, data_jaar=2024),
        Buurt(code="BU06030002", naam="Plaspoelpolder", gemeente_naam="Rijswijk", inwoners=3000, data_jaar=2024),
    ]


def safe_int(val):
    """Convert to int or return None."""
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def safe_float(val):
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def main():
    print("Initializing database...")
    init_db()

    db = SessionLocal()

    try:
        # Check if already seeded
        existing_buurten = db.query(Buurt).count()

        if existing_buurten > 0:
            print(f"Database already has {existing_buurten} buurten, skipping buurt seed")
        else:
            buurten = seed_buurten()
            if buurten:
                db.add_all(buurten)
                db.commit()
                print(f"Added {len(buurten)} buurten to database")

        # Show summary
        total_buurten = db.query(Buurt).count()
        total_woningen = db.query(Woning).count()
        print(f"\nDatabase summary:")
        print(f"  - Buurten: {total_buurten}")
        print(f"  - Woningen: {total_woningen}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
