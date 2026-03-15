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

from api import buurten_router, woningen_router, watchlist_router, markt_router
from models.database import init_db
from models import Buurt, Woning, WatchlistItem, Prijshistorie  # noqa: F401 - ensure models are loaded


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database on startup."""
    init_db()
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
app.include_router(watchlist_router)
app.include_router(markt_router)


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
        },
    }


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
