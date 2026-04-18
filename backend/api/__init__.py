"""FastAPI routes for Woningzoeker."""

from api.buurten import router as buurten_router
from api.woningen import router as woningen_router
from api.waardebepaling import router as waardebepaling_router
from api.watchlist import router as watchlist_router
from api.markt import router as markt_router
from api.scholen import router as scholen_router
from api.voorzieningen import router as voorzieningen_router
from api.postcode6 import router as postcode6_router
from api.bereikbaarheid import router as bereikbaarheid_router
from api.milieu import router as milieu_router
from api.bestemmingsplan import router as bestemmingsplan_router

__all__ = [
    "buurten_router",
    "woningen_router",
    "waardebepaling_router",
    "watchlist_router",
    "markt_router",
    "scholen_router",
    "voorzieningen_router",
    "postcode6_router",
    "bereikbaarheid_router",
    "milieu_router",
    "bestemmingsplan_router",
]
