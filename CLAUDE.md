# Woningzoeker - Project Notes

## Python Virtual Environment

Always use a virtual environment for Python projects. Never install packages globally.

### Creating and activating a virtual environment

```bash
# Create venv in the backend directory
cd /home/johan/Documents/Claude-Code/woningzoeker/backend
python3 -m venv venv

# Activate it (Linux/macOS)
source venv/bin/activate

# Install packages
pip install -r ../requirements.txt
```

### Running the backend

```bash
# Make sure venv is activated first
source venv/bin/activate

# Run via the run script (handles imports correctly)
python run.py
```

### Deactivating

```bash
deactivate
```

## Project Structure

- `backend/` - FastAPI Python backend
- `frontend/` - React TypeScript frontend
- `config/` - YAML configuration files
- `data/` - SQLite database and cache

## Key Commands

```bash
# Backend (from backend/ with venv activated)
uvicorn main:app --reload

# Frontend (from frontend/)
npm install
npm run dev
```

## Data Sources

- CBS StatLine API (cbsodata package)
- BAG API (requires API key in BAG_API_KEY env var)
- Funda (scraping with rate limits)
