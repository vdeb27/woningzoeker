"""Transaction model for historical property sales data."""

from datetime import datetime

from sqlalchemy import Column, Integer, String, Float, DateTime, Date, UniqueConstraint

from models.database import Base


class Transactie(Base):
    """Historical property transaction from external sources."""

    __tablename__ = "transacties"

    id = Column(Integer, primary_key=True, index=True)

    # Address
    postcode = Column(String(6), nullable=False, index=True)
    pc4 = Column(String(4), nullable=False, index=True)
    huisnummer = Column(Integer, nullable=False)
    straat = Column(String(200))
    woonplaats = Column(String(100), index=True)

    # Transaction
    transactie_datum = Column(Date, index=True)
    transactie_prijs = Column(Integer)
    koopjaar = Column(Integer, index=True)

    # Property characteristics (if available)
    woonoppervlakte = Column(Integer)
    perceeloppervlakte = Column(Integer)
    woningtype = Column(String(50))
    bouwjaar = Column(Integer)

    # Derived
    prijs_per_m2 = Column(Float)

    # Source tracking
    bron = Column(String(50), nullable=False, index=True)  # openkadaster, miljoenhuizen
    bron_url = Column(String(500))
    bron_id = Column(String(100))  # external ID if available

    # Metadata
    opgehaald_op = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "postcode", "huisnummer", "transactie_datum", "bron",
            name="uq_transactie_adres_datum_bron",
        ),
    )

    def __repr__(self) -> str:
        return f"<Transactie {self.straat} {self.huisnummer}, {self.postcode} - €{self.transactie_prijs:,}>"
