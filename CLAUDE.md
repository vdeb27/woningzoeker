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
- WOZ Waardeloket (Kadaster LV-WOZ API)
- EP-Online (RVO energielabels)
- CBS Kerncijfers (buurt-niveau data)
- Miljoenhuizen.nl (historische vraagprijzen, scraping)

## Development Workflow

### Feature Development

1. **Check GitHub issues** voor openstaande taken:
   ```bash
   gh issue list
   gh issue view <nummer>
   ```

2. **Maak feature branch** gebaseerd op issue:
   ```bash
   git checkout -b feature/<korte-naam>
   ```

3. **Implementeer met tasks** voor complexe taken:
   - Gebruik TaskCreate om subtaken te tracken
   - Update status naar in_progress/completed

4. **Test de code** voordat je commit:
   ```bash
   cd backend && source venv/bin/activate
   python -c "from collectors import ...; ..."
   ```

### Git Workflow

**Commit style:**
```
feat: korte beschrijving

- Bullet points met details
- Wat is toegevoegd/gewijzigd

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
```

**Commit types:** `feat:`, `fix:`, `refactor:`, `docs:`, `test:`

**Pull Request maken:**
```bash
git add <bestanden>
git commit -m "..."
git push -u origin <branch>
gh pr create --base master --title "..." --body "..."
```

**PR koppelen aan issue:** Voeg `Closes #<nummer>` toe in PR body.

**Mergen:**
```bash
gh pr merge <nummer> --merge --delete-branch
```

### Collector Pattern

Nieuwe data collectors volgen dit patroon:

```python
# backend/collectors/<naam>_collector.py

@dataclass
class <Naam>Result:
    """Dataclass voor resultaat."""
    ...

    def to_dict(self) -> Dict[str, Any]: ...

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "<Naam>Result": ...

@dataclass
class <Naam>Collector:
    """Collector met rate limiting en caching."""

    min_delay: float = 2.0
    max_delay: float = 3.0
    cache_dir: Optional[Path] = None
    cache_days: int = 7
    session: Optional[requests.Session] = None

    def __post_init__(self): ...
    def _rate_limit(self): ...
    def _load_from_cache(self, key: str, max_age_days: int): ...
    def _save_to_cache(self, key: str, data: dict): ...

    # Publieke methodes
    def get_<data>(self, ...) -> <Naam>Result: ...

def create_<naam>_collector(cache_dir: Optional[Path] = None) -> <Naam>Collector:
    """Factory function met default cache directory."""
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "<naam>"
    return <Naam>Collector(cache_dir=cache_dir)
```

**Vergeet niet:**
- Exports toevoegen aan `backend/collectors/__init__.py`
- Integreren in `backend/api/woningen.py` indien relevant

### Scraping Best Practices

- **Rate limiting:** Minimaal 2 seconden tussen requests
- **Caching:** Korte cache (1 dag) voor actuele data, lange cache (30 dagen) voor historische data
- **User-Agent rotation:** Gebruik realistische browser headers
- **Graceful degradation:** Geen errors naar gebruiker bij scrape failures
- **Respecteer robots.txt:** Check vooraf wat toegestaan is

## Recent Completed

- Issue #22: Extra databronnen verkoopprijzen integreren
  - CBS Market Collector (gemiddelde prijzen, overbiedingspercentage)
  - CBS Buurt Collector (buurt-niveau WOZ, inkomen)
  - Miljoenhuizen Collector (historische vraagprijzen)
