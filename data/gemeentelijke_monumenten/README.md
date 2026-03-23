# Gemeentelijke Monumenten

Place CSV files in this directory with the following columns:

```
postcode,huisnummer,huisletter,toevoeging,adres,gemeente,omschrijving,bron_url
```

## Sources

- **Den Haag**: https://data.denhaag.nl/ — Gemeentelijke monumenten Den Haag
- **Leidschendam-Voorburg**: Gemeente website monumentenlijst
- **Rijswijk**: Gemeente website monumentenlijst (~55 monumenten)

## Import

```bash
cd backend && source venv/bin/activate
python import_gemeentelijke_monumenten.py
```
