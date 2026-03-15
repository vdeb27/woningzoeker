"""Price history model for tracking price changes."""

from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from models.database import Base


class Prijshistorie(Base):
    """Historical price records for properties."""

    __tablename__ = "prijshistorie"

    id = Column(Integer, primary_key=True, index=True)
    woning_id = Column(Integer, ForeignKey("woningen.id"), nullable=False, index=True)

    prijs = Column(Integer, nullable=False)
    type = Column(String(20), nullable=False)  # vraagprijs, verkocht, verlaagd, verhoogd
    datum = Column(DateTime, default=datetime.utcnow, index=True)

    # Relationships
    woning = relationship("Woning", backref="prijshistorie")

    def __repr__(self) -> str:
        return f"<Prijshistorie {self.woning_id}: €{self.prijs:,} ({self.type})>"
