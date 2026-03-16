"""Buurt (neighborhood) model."""

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, Integer, String, Float, DateTime, Text, JSON

from models.database import Base


class Buurt(Base):
    """Neighborhood with CBS statistics and scores."""

    __tablename__ = "buurten"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(20), unique=True, index=True, nullable=False)
    naam = Column(String(200), nullable=False)
    gemeente_code = Column(String(10), index=True)
    gemeente_naam = Column(String(100))
    wijk_code = Column(String(20))
    wijk_naam = Column(String(200))

    # CBS Statistics (core fields)
    inwoners = Column(Integer)
    huishoudens = Column(Integer)
    gemiddeld_inkomen = Column(Float)
    woz_waarde = Column(Float)
    gasverbruik = Column(Float)
    elektraverbruik = Column(Float)
    arbeidsparticipatie = Column(Float)

    # Extended indicators (JSON blob with all CBS + Leefbaarometer + RIVM data)
    indicatoren = Column(JSON)

    # Computed scores (0-1 normalized)
    score_totaal = Column(Float)
    score_inkomen = Column(Float)
    score_veiligheid = Column(Float)
    score_voorzieningen = Column(Float)
    score_woningen = Column(Float)
    score_coverage = Column(Float)  # How much data was available for scoring

    # Category scores
    score_bereikbaarheid = Column(Float)
    score_energie = Column(Float)
    score_demografie = Column(Float)
    score_leefbaarheid = Column(Float)

    # Leefbaarometer
    leefbaarometer_score = Column(Float)
    leefbaarometer_fysiek = Column(Float)
    leefbaarometer_voorzieningen = Column(Float)
    leefbaarometer_veiligheid = Column(Float)
    leefbaarometer_bevolking = Column(Float)
    leefbaarometer_woningen = Column(Float)

    # Price statistics (from Funda data)
    median_vraagprijs = Column(Integer)
    median_m2_prijs = Column(Float)
    aantal_te_koop = Column(Integer)

    # GeoJSON geometry (stored as JSON)
    geometrie = Column(JSON)

    # Metadata
    data_jaar = Column(Integer)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<Buurt {self.code}: {self.naam}>"
