"""Watchlist model for tracking interesting properties."""

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, Boolean
from sqlalchemy.orm import relationship

from models.database import Base


class WatchlistItem(Base):
    """A property on the user's watchlist."""

    __tablename__ = "watchlist"

    id = Column(Integer, primary_key=True, index=True)
    woning_id = Column(Integer, ForeignKey("woningen.id"), nullable=False)

    # User notes
    notities = Column(Text)
    prioriteit = Column(Integer, default=0)  # Higher = more interested
    status = Column(String(50), default="interested")  # interested, viewed, bid, rejected

    # Bid tracking
    geboden_bedrag = Column(Integer)
    bod_datum = Column(DateTime)
    bod_status = Column(String(50))  # pending, accepted, rejected, outbid

    # Viewing
    bezichtiging_gepland = Column(DateTime)
    bezichtigd = Column(Boolean, default=False)

    # Timestamps
    added_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    woning = relationship("Woning", backref="watchlist_items")

    def __repr__(self) -> str:
        return f"<WatchlistItem {self.id}: woning {self.woning_id}>"
