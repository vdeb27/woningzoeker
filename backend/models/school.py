"""School model for DUO school data."""

from datetime import datetime

from sqlalchemy import Column, Integer, String, Float, DateTime, UniqueConstraint

from models.database import Base


class School(Base):
    """School information from DUO open data."""

    __tablename__ = "scholen"

    id = Column(Integer, primary_key=True, index=True)

    # Identificatie
    brin = Column(String(10), nullable=False, index=True)
    vestigingsnummer = Column(String(10), nullable=False)
    naam = Column(String(300), nullable=False)

    # Type
    type = Column(String(20), nullable=False, index=True)  # "basisonderwijs" / "voortgezet"
    onderwijstype = Column(String(100))  # VO: "vmbo"/"havo"/"vwo"/combinaties

    # Adres
    straat = Column(String(200))
    postcode = Column(String(10), index=True)
    plaats = Column(String(100))
    gemeente = Column(String(100), index=True)

    # Kenmerken
    denominatie = Column(String(100))
    leerlingen = Column(Integer)

    # Locatie
    lat = Column(Float)
    lng = Column(Float)

    # Kwaliteit PO
    advies_havo_vwo_pct = Column(Float)  # % leerlingen met HAVO/VWO advies
    gem_eindtoets = Column(Float)  # Gemiddelde doorstroomtoets score

    # Kwaliteit VO
    slagingspercentage = Column(Float)
    gem_examencijfer = Column(Float)

    # Inspectie
    inspectie_oordeel = Column(String(50))  # "Voldoende" / "Onvoldoende" / "Zeer zwak"

    # Metadata
    data_jaar = Column(String(20))  # Schooljaar, bijv. "2024-2025"
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("brin", "vestigingsnummer", name="uq_school_brin_vestiging"),
    )

    def __repr__(self) -> str:
        return f"<School {self.naam} ({self.brin}{self.vestigingsnummer}) - {self.type}>"
