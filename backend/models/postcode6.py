"""Postcode-6 area model."""

from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, JSON

from models.database import Base


class Postcode6(Base):
    """PC6 postcode area with polygon geometry."""

    __tablename__ = "postcode6"

    id = Column(Integer, primary_key=True, index=True)
    postcode = Column(String(6), unique=True, index=True, nullable=False)
    gemeente_naam = Column(String(100), index=True)
    aantal_adressen = Column(Integer)
    geometrie = Column(JSON)  # GeoJSON geometry (WGS84)
    updated_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<Postcode6 {self.postcode}>"
