# Woningzoeker

Data-gedreven tool voor huizenzoekers in de regio Den Haag, Leidschendam-Voorburg en Rijswijk.

## Features

- **Waardebepaling**: Schat de marktwaarde van een woning en krijg biedadvies
- **Buurtstatistieken**: CBS data over inkomen, veiligheid, voorzieningen per buurt
- **Woninglijst**: Automatisch verzameld aanbod met BAG verrijking
- **Watchlist**: Volg woningen en krijg updates over prijswijzigingen

## Quickstart

### Backend

```bash
cd backend
python -m venv venv
source venv/bin/activate
pip install -r ../requirements.txt
uvicorn main:app --reload
```

API beschikbaar op http://localhost:8000

### Frontend

```bash
cd frontend
npm install
npm run dev
```

Frontend beschikbaar op http://localhost:5173

## Projectstructuur

```
woningzoeker/
├── backend/
│   ├── api/              # FastAPI routes
│   ├── collectors/       # Data collectors (CBS, BAG, Funda)
│   ├── models/           # SQLAlchemy models
│   ├── services/         # Business logic (scoring, valuation)
│   └── main.py
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   ├── pages/
│   │   └── services/
│   └── package.json
├── data/
│   ├── woningzoeker.db   # SQLite database
│   └── cache/            # API response cache
├── config/
│   ├── areas.yaml
│   └── scoring.yaml
└── FUTURE_IDEAS.md       # Geparkeerde features
```

## Data bronnen

| Bron | Data | Update frequentie |
|------|------|-------------------|
| CBS StatLine | Demografie, inkomen, criminaliteit | Kwartaal |
| BAG/PDOK | Gebouwen, adressen, bouwjaar | Dagelijks |
| Funda | Woningaanbod | Dagelijks |
| EP-Online | Energielabels | Maandelijks |
| Leefbaarometer | Leefbaarheidsscores | Jaarlijks |

## API Endpoints

### Buurten
- `GET /api/buurten` - Alle buurten met scores
- `GET /api/buurten/{code}` - Detail met statistieken
- `GET /api/buurten/vergelijk` - Vergelijk tot 5 buurten

### Woningen
- `GET /api/woningen` - Zoeken met filters
- `GET /api/woningen/{id}` - Detail met BAG data
- `GET /api/woningen/{id}/waarde` - Waardebepaling

### Watchlist
- `GET /api/watchlist` - Alle gevolgde woningen
- `POST /api/watchlist` - Woning toevoegen
- `DELETE /api/watchlist/{id}` - Verwijderen

### Markt
- `GET /api/markt/trends` - Prijstrends per buurt
- `GET /api/markt/overbieden` - Overbiedingspercentages

## Doelregio

- Den Haag (gemeente 0518)
- Leidschendam-Voorburg (gemeente 1916)
- Rijswijk (gemeente 0603)
