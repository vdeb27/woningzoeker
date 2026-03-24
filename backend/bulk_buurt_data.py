"""
Bulk download and scoring of neighborhood data.

Fetches data from all collectors (CBS Kerncijfers, Leefbaarometer, CBS Nabijheid, RIVM),
and writes to the database incrementally after each source completes.
Scores are recalculated at the end using all available data.

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


def _get_or_create_buurt(session, code: str) -> Buurt:
    """Get existing buurt or create a new one."""
    buurt = session.query(Buurt).filter(Buurt.code == code).first()
    if not buurt:
        buurt = Buurt(code=code)
        session.add(buurt)
    return buurt


def _merge_indicatoren(existing: dict | None, new_data: dict) -> dict:
    """Merge new indicator data into existing indicatoren dict."""
    result = dict(existing) if existing else {}
    for key, val in new_data.items():
        if val is not None:
            result[key] = val
    return result


def step_cbs_kerncijfers(session) -> int:
    """Stap 1: CBS Kerncijfers ophalen en direct naar DB schrijven."""
    print("=== Stap 1/5: CBS Kerncijfers laden ===")
    cbs = CBSBuurtCollector()
    buurten = cbs.get_all_buurten()
    print(f"  {len(buurten)} buurten geladen")

    count = 0
    for buurt_data in buurten:
        buurt = _get_or_create_buurt(session, buurt_data.buurt_code)
        buurt.naam = buurt_data.buurt_naam
        buurt.gemeente_code = buurt_data.gemeente_code
        buurt.gemeente_naam = buurt_data.gemeente_naam

        # CBS core fields
        buurt.gemiddeld_inkomen = buurt_data.gem_inkomen
        buurt.woz_waarde = buurt_data.gem_woz_waarde

        # Extended indicators
        ind = dict(buurt_data.indicatoren) if buurt_data.indicatoren else {}
        buurt.inwoners = _safe_int(ind.get("inwoners"))
        buurt.huishoudens = _safe_int(ind.get("huishoudens_totaal"))
        buurt.gasverbruik = _safe_float(ind.get("gem_gasverbruik"))
        buurt.elektraverbruik = _safe_float(ind.get("gem_elektraverbruik"))
        buurt.arbeidsparticipatie = (
            _safe_float(ind.get("netto_arbeidsparticipatie"))
            or _safe_float(ind.get("arbeidsparticipatie"))
        )

        buurt.indicatoren = ind if ind else None
        buurt.data_jaar = 2024
        count += 1

    session.commit()
    print(f"  {count} buurten opgeslagen in DB")
    return count


def step_leefbaarometer(session) -> int:
    """Stap 2: Leefbaarometer data ophalen en mergen in bestaande buurten."""
    print("\n=== Stap 2/5: Leefbaarometer laden ===")
    try:
        lbm = create_leefbaarometer_collector()
        lbm_data = lbm.get_all()
        print(f"  {len(lbm_data)} buurten met Leefbaarometer data")
    except Exception as e:
        print(f"  Leefbaarometer laden mislukt: {e}")
        return 0

    count = 0
    for code, result in lbm_data.items():
        buurt = session.query(Buurt).filter(Buurt.code == code).first()
        if not buurt:
            continue

        buurt.leefbaarometer_score = _safe_float(result.lbm_score)
        buurt.leefbaarometer_fysiek = _safe_float(result.fysieke_omgeving)
        buurt.leefbaarometer_voorzieningen = _safe_float(result.voorzieningen)
        buurt.leefbaarometer_veiligheid = _safe_float(result.veiligheid)
        buurt.leefbaarometer_bevolking = _safe_float(result.bevolkingssamenstelling)
        buurt.leefbaarometer_woningen = _safe_float(result.woningvoorraad)

        # Merge into indicatoren
        buurt.indicatoren = _merge_indicatoren(buurt.indicatoren, {
            "leefbaarometer_veiligheid": result.veiligheid,
            "leefbaarometer_fysiek": result.fysieke_omgeving,
        })
        count += 1

    session.commit()
    print(f"  {count} buurten bijgewerkt met Leefbaarometer")
    return count


def step_cbs_nabijheid(session) -> int:
    """Stap 3: CBS Nabijheid ophalen en mergen."""
    print("\n=== Stap 3/5: CBS Nabijheid laden ===")
    try:
        nabijheid = create_cbs_nabijheid_collector()
        nabijheid_data = nabijheid.get_all()
        print(f"  {len(nabijheid_data)} buurten met nabijheiddata")
    except Exception as e:
        print(f"  CBS Nabijheid laden mislukt: {e}")
        return 0

    count = 0
    for code, result in nabijheid_data.items():
        buurt = session.query(Buurt).filter(Buurt.code == code).first()
        if not buurt:
            continue

        buurt.indicatoren = _merge_indicatoren(buurt.indicatoren, result.afstanden)
        count += 1

    session.commit()
    print(f"  {count} buurten bijgewerkt met nabijheiddata")
    return count


def step_cbs_extra(session) -> int:
    """Stap 4: CBS Extra (misdrijven, arbeid, SES, opleiding, bodem)."""
    print("\n=== Stap 4/5: CBS Extra laden ===")
    try:
        extra = create_cbs_extra_collector()
        extra_data = extra.get_all_buurten()
        print(f"  {len(extra_data)} buurten met extra CBS data")
    except Exception as e:
        print(f"  CBS Extra laden mislukt: {e}")
        return 0

    count = 0
    for code, indicators in extra_data.items():
        buurt = session.query(Buurt).filter(Buurt.code == code).first()
        if not buurt:
            continue

        buurt.indicatoren = _merge_indicatoren(buurt.indicatoren, indicators)
        count += 1

    session.commit()
    print(f"  {count} buurten bijgewerkt met extra CBS data")
    return count


def step_rivm(session) -> int:
    """Stap 5: RIVM Atlas (geluidhinder, slaapverstoring, tevredenheid)."""
    print("\n=== Stap 5/5: RIVM Atlas laden ===")
    try:
        rivm = create_rivm_collector()
        rivm_data = rivm.get_all()
        print(f"  {len(rivm_data)} buurten met RIVM data")
        if not rivm_data:
            print("  Geen RIVM data beschikbaar.")
            return 0
    except Exception as e:
        print(f"  RIVM laden mislukt: {e}")
        return 0

    count = 0
    for code, result in rivm_data.items():
        buurt = session.query(Buurt).filter(Buurt.code == code).first()
        if not buurt:
            continue

        rivm_dict = result.to_dict()
        rivm_indicators = {k: v for k, v in rivm_dict.items() if k != "buurt_code" and v is not None}
        buurt.indicatoren = _merge_indicatoren(buurt.indicatoren, rivm_indicators)
        count += 1

    session.commit()
    print(f"  {count} buurten bijgewerkt met RIVM data")
    return count


def recalculate_scores(session):
    """Herbereken scores op basis van alle data in de DB."""
    print("\n=== Scores herberekenen ===")

    buurten = session.query(Buurt).all()
    if not buurten:
        print("  Geen buurten in database.")
        return

    # Build DataFrame from current DB state
    records = []
    for buurt in buurten:
        record = {
            "buurt_code": buurt.code,
            "buurt_naam": buurt.naam,
            "gemeente_code": buurt.gemeente_code,
            "gemeente_naam": buurt.gemeente_naam,
            "gem_woz_waarde": buurt.woz_waarde,
            "gem_inkomen": buurt.gemiddeld_inkomen,
            "lbm": buurt.leefbaarometer_score,
            "leefbaarometer_fysiek": buurt.leefbaarometer_fysiek,
            "leefbaarometer_voorzieningen": buurt.leefbaarometer_voorzieningen,
            "leefbaarometer_veiligheid": buurt.leefbaarometer_veiligheid,
            "leefbaarometer_bevolking": buurt.leefbaarometer_bevolking,
            "leefbaarometer_woningen": buurt.leefbaarometer_woningen,
        }

        # Flatten indicatoren for scoring
        ind = buurt.indicatoren or {}
        for key, val in ind.items():
            if val is not None:
                record[key] = val

        record["indicatoren"] = ind
        records.append(record)

    df = pd.DataFrame(records)
    print(f"  {len(df)} buurten, {len(df.columns)} kolommen")

    scorer = ScoringService()
    scored = scorer.calculate_scores(df)

    # Show score distribution
    if "score" in scored.columns:
        valid_scores = scored["score"].dropna()
        if len(valid_scores) > 0:
            print(f"  Score range: {valid_scores.min():.2f} - {valid_scores.max():.2f}")
            print(f"  Score gemiddelde: {valid_scores.mean():.2f}")
            print(f"  Buurten met score: {len(valid_scores)}/{len(scored)}")

    for cat_id in scorer.categories:
        col = f"score_{cat_id}"
        if col in scored.columns:
            valid = scored[col].dropna()
            if len(valid) > 0:
                print(f"  {cat_id}: {valid.mean():.2f} avg ({len(valid)} buurten)")

    # Write scores back to DB
    print("\n=== Scores opslaan ===")
    count = 0
    for _, row in scored.iterrows():
        code = row.get("buurt_code", "")
        if not code:
            continue

        buurt = session.query(Buurt).filter(Buurt.code == code).first()
        if not buurt:
            continue

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
        count += 1

    session.commit()
    print(f"  {count} buurten met scores bijgewerkt")


def main():
    parser = argparse.ArgumentParser(description="Bulk download buurtdata")
    parser.add_argument("--skip-rivm", action="store_true", help="Skip RIVM Atlas data")
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

    init_db()
    session = SessionLocal()

    try:
        # Elke stap haalt data op en schrijft direct naar DB
        # Als het script crasht, is alles tot de laatste voltooide stap bewaard
        step_cbs_kerncijfers(session)
        step_leefbaarometer(session)
        step_cbs_nabijheid(session)
        step_cbs_extra(session)

        if not args.skip_rivm:
            step_rivm(session)
        else:
            print("\n=== RIVM overgeslagen (--skip-rivm) ===")

        # Scores berekenen over alle beschikbare data
        recalculate_scores(session)

        total = session.query(Buurt).count()
        with_score = session.query(Buurt).filter(Buurt.score_totaal.isnot(None)).count()
        print(f"\n=== Klaar! {total} buurten in DB, {with_score} met score ===")

    except Exception as e:
        session.rollback()
        print(f"\nFOUT: {e}")
        # Eerdere stappen zijn al gecommit en bewaard
        total = session.query(Buurt).count()
        if total > 0:
            print(f"  {total} buurten uit eerdere stappen zijn bewaard in DB")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
