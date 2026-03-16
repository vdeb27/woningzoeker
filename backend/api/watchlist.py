"""Watchlist API routes."""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from models import get_db, WatchlistItem, Woning

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


class WatchlistItemBase(BaseModel):
    """Base watchlist item schema."""
    woning_id: int
    notities: Optional[str] = None
    prioriteit: int = 0
    status: str = "interested"


class WatchlistItemCreate(WatchlistItemBase):
    """Schema for creating a watchlist item."""
    pass


class WatchlistItemUpdate(BaseModel):
    """Schema for updating a watchlist item."""
    notities: Optional[str] = None
    prioriteit: Optional[int] = None
    status: Optional[str] = None
    geboden_bedrag: Optional[int] = None
    bod_status: Optional[str] = None
    bezichtiging_gepland: Optional[datetime] = None
    bezichtigd: Optional[bool] = None


class WatchlistItemResponse(WatchlistItemBase):
    """Response schema for watchlist items."""
    id: int
    geboden_bedrag: Optional[int] = None
    bod_datum: Optional[datetime] = None
    bod_status: Optional[str] = None
    bezichtiging_gepland: Optional[datetime] = None
    bezichtigd: bool = False
    added_at: datetime
    updated_at: datetime

    # Embedded woning info
    woning_adres: Optional[str] = None
    woning_vraagprijs: Optional[int] = None
    woning_woonoppervlakte: Optional[int] = None
    woning_status: Optional[str] = None

    class Config:
        from_attributes = True


@router.get("/", response_model=List[WatchlistItemResponse])
def list_watchlist(
    status: Optional[str] = Query(None, description="Filter by status"),
    sort_by: str = Query("prioriteit", description="Sort field"),
    db: Session = Depends(get_db),
):
    """Get all watchlist items."""
    query = db.query(WatchlistItem)

    if status:
        query = query.filter(WatchlistItem.status == status)

    if sort_by == "prioriteit":
        query = query.order_by(WatchlistItem.prioriteit.desc())
    elif sort_by == "added_at":
        query = query.order_by(WatchlistItem.added_at.desc())

    items = query.all()

    # Enrich with woning data
    result = []
    for item in items:
        woning = db.query(Woning).filter(Woning.id == item.woning_id).first()
        response = WatchlistItemResponse(
            id=item.id,
            woning_id=item.woning_id,
            notities=item.notities,
            prioriteit=item.prioriteit,
            status=item.status,
            geboden_bedrag=item.geboden_bedrag,
            bod_datum=item.bod_datum,
            bod_status=item.bod_status,
            bezichtiging_gepland=item.bezichtiging_gepland,
            bezichtigd=item.bezichtigd,
            added_at=item.added_at,
            updated_at=item.updated_at,
            woning_adres=woning.adres if woning else None,
            woning_vraagprijs=woning.vraagprijs if woning else None,
            woning_woonoppervlakte=woning.woonoppervlakte if woning else None,
            woning_status=woning.status if woning else None,
        )
        result.append(response)

    return result


@router.get("/{item_id}", response_model=WatchlistItemResponse)
def get_watchlist_item(item_id: int, db: Session = Depends(get_db)):
    """Get a specific watchlist item."""
    item = db.query(WatchlistItem).filter(WatchlistItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Watchlist item niet gevonden")

    woning = db.query(Woning).filter(Woning.id == item.woning_id).first()

    return WatchlistItemResponse(
        id=item.id,
        woning_id=item.woning_id,
        notities=item.notities,
        prioriteit=item.prioriteit,
        status=item.status,
        geboden_bedrag=item.geboden_bedrag,
        bod_datum=item.bod_datum,
        bod_status=item.bod_status,
        bezichtiging_gepland=item.bezichtiging_gepland,
        bezichtigd=item.bezichtigd,
        added_at=item.added_at,
        updated_at=item.updated_at,
        woning_adres=woning.adres if woning else None,
        woning_vraagprijs=woning.vraagprijs if woning else None,
        woning_woonoppervlakte=woning.woonoppervlakte if woning else None,
        woning_status=woning.status if woning else None,
    )


@router.post("/", response_model=WatchlistItemResponse)
def create_watchlist_item(item: WatchlistItemCreate, db: Session = Depends(get_db)):
    """Add a property to the watchlist."""
    # Check if woning exists
    woning = db.query(Woning).filter(Woning.id == item.woning_id).first()
    if not woning:
        raise HTTPException(status_code=404, detail="Woning niet gevonden")

    # Check if already on watchlist
    existing = (
        db.query(WatchlistItem)
        .filter(WatchlistItem.woning_id == item.woning_id)
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=400,
            detail="Woning staat al op de watchlist"
        )

    db_item = WatchlistItem(
        woning_id=item.woning_id,
        notities=item.notities,
        prioriteit=item.prioriteit,
        status=item.status,
    )
    db.add(db_item)
    db.commit()
    db.refresh(db_item)

    return WatchlistItemResponse(
        id=db_item.id,
        woning_id=db_item.woning_id,
        notities=db_item.notities,
        prioriteit=db_item.prioriteit,
        status=db_item.status,
        geboden_bedrag=db_item.geboden_bedrag,
        bod_datum=db_item.bod_datum,
        bod_status=db_item.bod_status,
        bezichtiging_gepland=db_item.bezichtiging_gepland,
        bezichtigd=db_item.bezichtigd,
        added_at=db_item.added_at,
        updated_at=db_item.updated_at,
        woning_adres=woning.adres,
        woning_vraagprijs=woning.vraagprijs,
        woning_woonoppervlakte=woning.woonoppervlakte,
        woning_status=woning.status,
    )


@router.put("/{item_id}", response_model=WatchlistItemResponse)
def update_watchlist_item(
    item_id: int,
    update: WatchlistItemUpdate,
    db: Session = Depends(get_db),
):
    """Update a watchlist item."""
    item = db.query(WatchlistItem).filter(WatchlistItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Watchlist item niet gevonden")

    # Update fields
    update_data = update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(item, field, value)

    # If setting geboden_bedrag, also set bod_datum
    if "geboden_bedrag" in update_data and update_data["geboden_bedrag"]:
        item.bod_datum = datetime.utcnow()

    db.commit()
    db.refresh(item)

    woning = db.query(Woning).filter(Woning.id == item.woning_id).first()

    return WatchlistItemResponse(
        id=item.id,
        woning_id=item.woning_id,
        notities=item.notities,
        prioriteit=item.prioriteit,
        status=item.status,
        geboden_bedrag=item.geboden_bedrag,
        bod_datum=item.bod_datum,
        bod_status=item.bod_status,
        bezichtiging_gepland=item.bezichtiging_gepland,
        bezichtigd=item.bezichtigd,
        added_at=item.added_at,
        updated_at=item.updated_at,
        woning_adres=woning.adres if woning else None,
        woning_vraagprijs=woning.vraagprijs if woning else None,
        woning_woonoppervlakte=woning.woonoppervlakte if woning else None,
        woning_status=woning.status if woning else None,
    )


@router.delete("/{item_id}")
def delete_watchlist_item(item_id: int, db: Session = Depends(get_db)):
    """Remove a property from the watchlist."""
    item = db.query(WatchlistItem).filter(WatchlistItem.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Watchlist item niet gevonden")

    db.delete(item)
    db.commit()

    return {"message": "Watchlist item verwijderd"}
