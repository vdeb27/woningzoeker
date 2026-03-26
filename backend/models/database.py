"""Database configuration and session management."""

from pathlib import Path
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

# Database path
DATA_DIR = Path(__file__).parent.parent.parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DATABASE_URL = f"sqlite:///{DATA_DIR / 'woningzoeker.db'}"

# Create engine
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},  # Needed for SQLite
    echo=False,
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()


def get_db() -> Generator:
    """Dependency for FastAPI to get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _migrate_add_address_columns() -> None:
    """Add huisnummer/huisletter/toevoeging columns and backfill from adres."""
    from utils.address import parse_huisnummer

    with engine.connect() as conn:
        # Add columns if they don't exist yet
        for col in ["huisnummer INTEGER", "huisletter VARCHAR(5)", "toevoeging VARCHAR(10)"]:
            try:
                conn.execute(text(f"ALTER TABLE woningen ADD COLUMN {col}"))
            except Exception:
                pass  # Column already exists

        # Backfill: parse huisnummer from adres for existing records
        rows = conn.execute(
            text("SELECT id, adres FROM woningen WHERE huisnummer IS NULL AND adres IS NOT NULL")
        ).fetchall()
        for row in rows:
            huisnummer, huisletter = parse_huisnummer(row[1])
            if huisnummer:
                conn.execute(
                    text("UPDATE woningen SET huisnummer = :hn, huisletter = :hl WHERE id = :id"),
                    {"hn": huisnummer, "hl": huisletter, "id": row[0]},
                )
        conn.commit()


def init_db() -> None:
    """Create all tables and run migrations."""
    Base.metadata.create_all(bind=engine)
    _migrate_add_address_columns()
