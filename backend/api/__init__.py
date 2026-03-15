"""FastAPI routes for Woningzoeker."""

from api.buurten import router as buurten_router
from api.woningen import router as woningen_router
from api.watchlist import router as watchlist_router
from api.markt import router as markt_router

__all__ = [
    "buurten_router",
    "woningen_router",
    "watchlist_router",
    "markt_router",
]
