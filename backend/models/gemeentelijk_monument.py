"""Gemeentelijk monument model."""

from sqlalchemy import Column, Integer, String, Text

from models.database import Base


class GemeentelijkMonument(Base):
    """Municipal monument registration."""

    __tablename__ = "gemeentelijke_monumenten"

    id = Column(Integer, primary_key=True, index=True)
    postcode = Column(String(6), index=True, nullable=False)
    huisnummer = Column(Integer, nullable=False)
    huisletter = Column(String(2))
    toevoeging = Column(String(10))
    adres = Column(String(300))
    gemeente = Column(String(100), index=True)
    omschrijving = Column(Text)
    bron_url = Column(String(500))

    def __repr__(self) -> str:
        return f"<GemeentelijkMonument {self.postcode} {self.huisnummer} ({self.gemeente})>"
