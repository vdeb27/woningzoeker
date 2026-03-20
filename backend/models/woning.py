"""Woning (property) model."""

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Boolean, ForeignKey, JSON
from sqlalchemy.orm import relationship

from models.database import Base


class Woning(Base):
    """Property listing with BAG enrichment."""

    __tablename__ = "woningen"

    id = Column(Integer, primary_key=True, index=True)
    funda_id = Column(String(50), unique=True, index=True)
    url = Column(String(500))

    # Address
    adres = Column(String(300), nullable=False)
    postcode = Column(String(10), index=True)
    pc6 = Column(String(6), index=True)
    plaats = Column(String(100))
    buurt_code = Column(String(20), ForeignKey("buurten.code"), index=True)

    # Geocoordinates
    latitude = Column(Float)
    longitude = Column(Float)

    # Basic info
    vraagprijs = Column(Integer, index=True)
    woonoppervlakte = Column(Integer)
    perceeloppervlakte = Column(Integer)
    inhoud = Column(Integer)
    kamers = Column(Integer)
    slaapkamers = Column(Integer)
    badkamers = Column(Integer)
    woningtype = Column(String(50))
    bouwjaar = Column(Integer)

    # Energy
    energielabel = Column(String(10))
    isolatie = Column(String(200))
    verwarming = Column(String(100))

    # Status
    status = Column(String(20), default="active", index=True)  # active, sold, withdrawn
    datum_aangemeld = Column(DateTime)
    datum_verkocht = Column(DateTime)
    verkoopprijs = Column(Integer)

    # BAG enrichment
    bag_nummeraanduiding_id = Column(String(50))
    bag_verblijfsobject_id = Column(String(50))
    bag_pand_id = Column(String(50))
    bag_oppervlakte = Column(Integer)
    bag_bouwjaar = Column(Integer)
    bag_gebruiksdoel = Column(String(100))
    bag_status = Column(String(50))

    # Valuation
    geschatte_waarde_laag = Column(Integer)
    geschatte_waarde_hoog = Column(Integer)
    waarde_confidence = Column(Float)
    biedadvies = Column(String(50))  # onder_vraagprijs, vraagprijs, boven_vraagprijs

    # Raw data
    raw_data = Column(JSON)

    # Metadata
    scraped_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    enriched_at = Column(DateTime)

    # Relationships
    buurt = relationship("Buurt", backref="woningen")

    def __repr__(self) -> str:
        return f"<Woning {self.adres} - €{self.vraagprijs:,}>"

    @property
    def m2_prijs(self) -> Optional[float]:
        """Price per square meter."""
        if self.vraagprijs and self.woonoppervlakte and self.woonoppervlakte > 0:
            return self.vraagprijs / self.woonoppervlakte
        return None
