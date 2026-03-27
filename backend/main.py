"""Woningzoeker FastAPI application."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Load .env file from backend directory
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

from api import buurten_router, woningen_router, waardebepaling_router, watchlist_router, markt_router, scholen_router, voorzieningen_router, postcode6_router, bereikbaarheid_router, milieu_router
from models.database import init_db
from models import Buurt, Woning, WatchlistItem, Prijshistorie, School, Postcode6  # noqa: F401 - ensure models are loaded


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database on startup."""
    init_db()

    # Migrate: add lat/lon columns if missing + remove sample data
    from sqlalchemy import text, inspect
    from models.database import engine

    with engine.connect() as conn:
        inspector = inspect(engine)
        columns = [c["name"] for c in inspector.get_columns("woningen")]
        if "latitude" not in columns:
            conn.execute(text("ALTER TABLE woningen ADD COLUMN latitude REAL"))
        if "longitude" not in columns:
            conn.execute(text("ALTER TABLE woningen ADD COLUMN longitude REAL"))
        # Remove hardcoded sample woningen
        conn.execute(text("DELETE FROM woningen WHERE funda_id LIKE 'sample_%'"))

        # Migrate: add score_milieu column if missing
        buurt_columns = [c["name"] for c in inspector.get_columns("buurten")]
        if "score_milieu" not in buurt_columns:
            conn.execute(text("ALTER TABLE buurten ADD COLUMN score_milieu REAL"))

        conn.commit()

    yield


app = FastAPI(
    title="Woningzoeker API",
    description="Data-gedreven tool voor huizenzoekers in de regio Den Haag",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite dev server
        "http://localhost:3000",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(buurten_router)
app.include_router(woningen_router)
app.include_router(waardebepaling_router)
app.include_router(watchlist_router)
app.include_router(markt_router)
app.include_router(scholen_router)
app.include_router(voorzieningen_router)
app.include_router(postcode6_router)
app.include_router(bereikbaarheid_router)
app.include_router(milieu_router)


@app.get("/")
def root():
    """Root endpoint with API info."""
    return {
        "name": "Woningzoeker API",
        "version": "0.1.0",
        "docs": "/docs",
        "endpoints": {
            "buurten": "/api/buurten",
            "woningen": "/api/woningen",
            "watchlist": "/api/watchlist",
            "markt": "/api/markt",
            "scholen": "/api/scholen",
            "voorzieningen": "/api/voorzieningen",
            "bereikbaarheid": "/api/woningen/{id}/bereikbaarheid",
            "reistijd": "/api/locatie/reistijd",
            "postcode6": "/api/postcode6",
        },
    }


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
