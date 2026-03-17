"""
Bulk download script for transaction and school data.

Downloads all available transaction data from OpenKadaster and Miljoenhuizen,
and school data from DUO, and stores it in the local SQLite database.

Usage:
    cd backend && source venv/bin/activate
    python bulk_download.py                    # All transaction sources
    python bulk_download.py --source openkadaster
    python bulk_download.py --source miljoenhuizen
    python bulk_download.py --source duo-scholen
    python bulk_download.py --source miljoenhuizen --plaatsen den-haag voorburg
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from typing import List, Optional

from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from collectors.kadaster_collector import create_kadaster_collector
from collectors.miljoenhuizen_collector import create_miljoenhuizen_collector
from collectors.duo_school_collector import create_duo_school_collector
from models.database import SessionLocal, init_db
from models.transactie import Transactie
from models.school import School


# Default cities to scrape from Miljoenhuizen
DEFAULT_PLAATSEN = [
    "den-haag",
    "leidschendam",
    "voorburg",
    "rijswijk",
]


def _parse_date(date_str: Optional[str]) -> Optional[date]:
    """Parse various date formats to date object."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d-%m-%y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def download_openkadaster() -> int:
    """Download all transactions from OpenKadaster.com."""
    print("\n=== OpenKadaster ===")
    collector = create_kadaster_collector()

    # Fetch all transactions (no search filter)
    print("Ophalen van alle transacties...")
    html = collector._fetch_page(collector.OPENKADASTER_URL, params={"order_by": "date_desc"})
    if not html:
        print("  FOUT: Kan OpenKadaster niet bereiken")
        return 0

    transactions = collector._parse_transactions_page(html)
    print(f"  {len(transactions)} transacties gevonden")

    if not transactions:
        return 0

    db = SessionLocal()
    inserted = 0
    try:
        for t in transactions:
            pc = t.postcode.replace(" ", "").upper()
            trans_date = _parse_date(t.transactie_datum)

            stmt = sqlite_insert(Transactie).values(
                postcode=pc,
                pc4=pc[:4],
                huisnummer=t.huisnummer,
                straat=t.straat,
                woonplaats=t.woonplaats,
                transactie_datum=trans_date,
                transactie_prijs=t.transactie_prijs,
                koopjaar=t.koopjaar,
                bron="openkadaster",
            ).on_conflict_do_nothing(
                index_elements=["postcode", "huisnummer", "transactie_datum", "bron"],
            )
            result = db.execute(stmt)
            if result.rowcount > 0:
                inserted += 1

        db.commit()
        print(f"  {inserted} nieuwe transacties opgeslagen")
    finally:
        db.close()

    return inserted


def download_miljoenhuizen(plaatsen: Optional[List[str]] = None, max_pages: int = 20) -> int:
    """
    Download transactions from Miljoenhuizen.nl.

    Scrapes overview pages per city, then fetches detail pages
    for sold properties to get transaction data.
    """
    print("\n=== Miljoenhuizen ===")
    plaatsen = plaatsen or DEFAULT_PLAATSEN
    collector = create_miljoenhuizen_collector()

    db = SessionLocal()
    inserted = 0
    total_woningen = 0

    try:
        for plaats in plaatsen:
            print(f"\n  [{plaats}]")

            # Scrape all overview pages
            all_woningen = []
            for page in range(1, max_pages + 1):
                woningen = collector.scrape_overzicht(plaats, page=page, use_cache=False)
                if not woningen:
                    break
                all_woningen.extend(woningen)
                print(f"    Pagina {page}: {len(woningen)} woningen")

            print(f"    Totaal: {len(all_woningen)} woningen gevonden")
            total_woningen += len(all_woningen)

            # Fetch detail pages for all properties (sold ones have transaction data)
            for i, w in enumerate(all_woningen):
                url = w.get("url", "")
                if not url:
                    continue

                detail = collector.scrape_detail(url, use_cache=True)
                if not detail:
                    continue

                # We want sold properties with a price
                if not detail.laatste_vraagprijs:
                    continue

                pc = detail.postcode.replace(" ", "").upper()
                if not pc or len(pc) < 4:
                    continue

                # Use verkoopdatum if sold, otherwise most recent price history date
                trans_date = _parse_date(detail.verkoopdatum)
                if not trans_date and detail.prijshistorie:
                    for entry in reversed(detail.prijshistorie):
                        trans_date = _parse_date(entry.datum)
                        if trans_date:
                            break

                # Parse huisnummer from adres
                huisnummer = None
                adres_parts = detail.adres.split()
                if adres_parts:
                    try:
                        huisnummer = int(adres_parts[-1])
                    except ValueError:
                        # Try to extract number from last part
                        import re
                        match = re.search(r"(\d+)", adres_parts[-1])
                        if match:
                            huisnummer = int(match.group(1))

                if huisnummer is None:
                    continue

                straat = " ".join(adres_parts[:-1]) if len(adres_parts) > 1 else None

                prijs_per_m2 = None
                if detail.laatste_vraagprijs and detail.woonoppervlakte and detail.woonoppervlakte > 0:
                    prijs_per_m2 = detail.laatste_vraagprijs / detail.woonoppervlakte

                stmt = sqlite_insert(Transactie).values(
                    postcode=pc,
                    pc4=pc[:4],
                    huisnummer=huisnummer,
                    straat=straat,
                    woonplaats=detail.plaats,
                    transactie_datum=trans_date,
                    transactie_prijs=detail.laatste_vraagprijs,
                    koopjaar=trans_date.year if trans_date else None,
                    woonoppervlakte=detail.woonoppervlakte,
                    perceeloppervlakte=detail.perceeloppervlakte,
                    woningtype=detail.woningtype,
                    bouwjaar=detail.bouwjaar,
                    prijs_per_m2=prijs_per_m2,
                    bron="miljoenhuizen",
                    bron_url=detail.url,
                ).on_conflict_do_nothing(
                    index_elements=["postcode", "huisnummer", "transactie_datum", "bron"],
                )
                result = db.execute(stmt)
                if result.rowcount > 0:
                    inserted += 1

                if (i + 1) % 10 == 0:
                    db.commit()
                    print(f"    Verwerkt: {i + 1}/{len(all_woningen)} ({inserted} opgeslagen)")

            db.commit()

        print(f"\n  Totaal: {total_woningen} woningen verwerkt, {inserted} nieuwe transacties opgeslagen")
    finally:
        db.close()

    return inserted


def download_duo_scholen() -> int:
    """Download school data from DUO CKAN API."""
    print("\n=== DUO Scholen ===")
    collector = create_duo_school_collector()

    schools = collector.fetch_all()
    print(f"  {len(schools)} scholen opgehaald")

    if not schools:
        return 0

    db = SessionLocal()
    inserted = 0
    updated = 0

    try:
        for s in schools:
            stmt = sqlite_insert(School).values(
                brin=s.brin,
                vestigingsnummer=s.vestigingsnummer,
                naam=s.naam,
                type=s.type,
                onderwijstype=s.onderwijstype,
                straat=s.straat,
                postcode=s.postcode,
                plaats=s.plaats,
                gemeente=s.gemeente,
                denominatie=s.denominatie,
                leerlingen=s.leerlingen,
                lat=s.lat,
                lng=s.lng,
                advies_havo_vwo_pct=s.advies_havo_vwo_pct,
                gem_eindtoets=s.gem_eindtoets,
                slagingspercentage=s.slagingspercentage,
                gem_examencijfer=s.gem_examencijfer,
                inspectie_oordeel=s.inspectie_oordeel,
                updated_at=datetime.utcnow(),
            ).on_conflict_do_update(
                index_elements=["brin", "vestigingsnummer"],
                set_={
                    "naam": s.naam,
                    "type": s.type,
                    "onderwijstype": s.onderwijstype,
                    "straat": s.straat,
                    "postcode": s.postcode,
                    "plaats": s.plaats,
                    "gemeente": s.gemeente,
                    "denominatie": s.denominatie,
                    "leerlingen": s.leerlingen,
                    "lat": s.lat,
                    "lng": s.lng,
                    "advies_havo_vwo_pct": s.advies_havo_vwo_pct,
                    "gem_eindtoets": s.gem_eindtoets,
                    "slagingspercentage": s.slagingspercentage,
                    "gem_examencijfer": s.gem_examencijfer,
                    "inspectie_oordeel": s.inspectie_oordeel,
                    "updated_at": datetime.utcnow(),
                },
            )
            result = db.execute(stmt)
            if result.rowcount > 0:
                inserted += 1

        db.commit()

        total = db.query(School).count()
        po = db.query(School).filter(School.type == "basisonderwijs").count()
        vo = db.query(School).filter(School.type == "voortgezet").count()
        print(f"  {inserted} scholen opgeslagen/bijgewerkt")
        print(f"  Totaal in DB: {total} ({po} PO, {vo} VO)")

        # Stats
        from sqlalchemy import func
        with_coords = db.query(School).filter(School.lat.isnot(None)).count()
        with_advies = db.query(School).filter(School.advies_havo_vwo_pct.isnot(None)).count()
        with_examen = db.query(School).filter(School.slagingspercentage.isnot(None)).count()
        print(f"  Met coördinaten: {with_coords}")
        print(f"  PO met adviesdata: {with_advies}")
        print(f"  VO met examendata: {with_examen}")
    finally:
        db.close()

    return inserted


def show_stats():
    """Show database statistics."""
    db = SessionLocal()
    try:
        total = db.query(Transactie).count()
        print(f"\n=== Database statistieken ===")
        print(f"  Totaal transacties: {total}")

        if total == 0:
            return

        # Per bron
        from sqlalchemy import func
        bronnen = db.query(
            Transactie.bron, func.count(Transactie.id)
        ).group_by(Transactie.bron).all()
        for bron, count in bronnen:
            print(f"    {bron}: {count}")

        # Per stad
        steden = db.query(
            Transactie.woonplaats, func.count(Transactie.id)
        ).group_by(Transactie.woonplaats).order_by(func.count(Transactie.id).desc()).limit(10).all()
        print(f"  Top plaatsen:")
        for stad, count in steden:
            print(f"    {stad}: {count}")

        # Datum range
        min_date = db.query(func.min(Transactie.transactie_datum)).scalar()
        max_date = db.query(func.max(Transactie.transactie_datum)).scalar()
        if min_date and max_date:
            print(f"  Periode: {min_date} t/m {max_date}")

        # Prijs range
        avg_prijs = db.query(func.avg(Transactie.transactie_prijs)).scalar()
        if avg_prijs:
            print(f"  Gemiddelde prijs: €{int(avg_prijs):,}")
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Bulk download transactiedata")
    parser.add_argument(
        "--source",
        choices=["openkadaster", "miljoenhuizen", "duo-scholen", "all"],
        default="all",
        help="Databron (default: all)",
    )
    parser.add_argument(
        "--plaatsen",
        nargs="+",
        help="Plaatsen voor Miljoenhuizen (default: den-haag leidschendam voorburg rijswijk)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=20,
        help="Max pagina's per plaats voor Miljoenhuizen (default: 20)",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Toon alleen database statistieken",
    )
    args = parser.parse_args()

    # Initialize database tables
    init_db()

    if args.stats:
        show_stats()
        return

    total = 0

    if args.source in ("openkadaster", "all"):
        total += download_openkadaster()

    if args.source in ("miljoenhuizen", "all"):
        total += download_miljoenhuizen(
            plaatsen=args.plaatsen,
            max_pages=args.max_pages,
        )

    if args.source in ("duo-scholen", "all"):
        total += download_duo_scholen()

    show_stats()

    if total == 0:
        print("\nGeen nieuwe transacties gevonden.")
    else:
        print(f"\nKlaar! {total} nieuwe transacties opgeslagen.")


if __name__ == "__main__":
    main()
