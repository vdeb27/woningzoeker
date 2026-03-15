# Future Ideas - Woningzoeker

Dit document bevat geparkeerde features en ideeën die buiten de huidige MVP scope vallen.

---

## Recent Geïmplementeerd

### Waardebepaling Versterking (maart 2026)
- [x] WOZ-waarde integratie via WOZ Waardeloket
- [x] Energielabel automatisch ophalen via EP-Online (RVO)
- [x] Kadaster transactie collector (structuur, data bronnen in ontwikkeling)
- [x] Buurt m² prijzen uit database laden
- [x] Adres-gebaseerde waardebepaling met auto-fetch
- [x] Vergelijkbare verkopen API endpoints

---

## Fase 4: Polish & Uitbreiding

### Hypotheek-calculator koppeling
- Deep link naar externe hypotheek-calculator met vooringevulde waarden
- Parameters: koopsom, eigen inbreng, looptijd, inkomen

### Kaartweergave met buurten
- Leaflet/OpenStreetMap integratie
- Buurten inkleuren op basis van score
- PDOK topografische kaarten als ondergrond
- Woningen als markers met popup details

### Buurtenvergelijker
- Tot 5 buurten naast elkaar vergelijken
- Radar chart met scores per dimensie
- Tabel met alle indicatoren

### Email notificaties
- Nieuwe listings in watchlist-buurten
- Prijswijzigingen op gevolgde woningen
- Dagelijkse/wekelijkse digest optie

### Verfijning waardebepalingsmodel
- Machine learning model trainen op historische data
- Feature importance analyse
- Cross-validatie per buurt

---

## Fase 5: Doorontwikkeling

### Zon & Oriëntatie
- [ ] 3DBAG integratie voor gebouworiëntatie
- [ ] Tuinoriëntatie bepalen (N/Z/O/W)
- [ ] Zonuren berekenen per seizoen voor tuin
- [ ] Dakoriëntatie voor zonnepanelen geschiktheid
- [ ] AHN4 hoogtemodel voor schaduwberekening
- [ ] Gemeentelijke zonnekaart integratie

### OV & Bereikbaarheid
- [ ] OVapi integratie voor openbaar vervoer
- [ ] Reistijd naar werk/school configureerbaar
- [ ] Isochrone kaarten (alle locaties bereikbaar binnen X minuten)
- [ ] OV-bereikbaarheid score per woning/buurt

### Scholen in de buurt
- [ ] DUO data integratie (schoolprestaties)
- [ ] Afstand tot dichtstbijzijnde scholen
- [ ] Kwaliteitsscores primair/voortgezet onderwijs
- [ ] Doorstroomcijfers en citoscores

### Omgevingsdata (RIVM)
- [ ] Luchtkwaliteit per locatie (NO2, fijnstof)
- [ ] Geluidsbelasting kaart
- [ ] Externe veiligheid risico's
- [ ] Combineren tot omgevingskwaliteit score

### Historische prijstrends
- [ ] Grafieken met prijsontwikkeling per buurt
- [ ] Vergelijking met gemeente/regio gemiddelde
- [ ] Seizoenscorrecties toepassen
- [ ] Looptijd op markt analyseren

### Marktanalyse
- [ ] Markttemperatuur indicator (koud/warm/heet)
- [ ] Overbiedingspercentages per buurt/gemeente (NVM via CBS)
- [ ] Vraagprijs vs. verkoopprijs analyse (Funda verkocht)
- [ ] Voorspellingsmodel prijsontwikkeling

### PDF rapporten genereren
- Samenvatting per woning met alle data
- Buurtprofiel bijlage
- Exporteerbaar voor hypotheekadviseur

### Erfpacht analyse
- Detectie erfpacht in listing
- Berekening totale kosten over looptijd
- Afkoop mogelijkheden

### VvE analyse
- VvE bijdrage inschatting
- Onderhoudsstatus signalen
- Reserve fonds indicaties

### Bezichtiging tracker
- Agenda integratie
- Notities per bezichtiging
- Checklist voor te stellen vragen

### Biedhistorie tracker
- Uitgebrachte biedingen registreren
- Resultaat tracking (gewonnen/verloren/ingetrokken)
- Analyse overbiedingspatronen

### Monumentenstatus
- [ ] Rijksdienst voor Cultureel Erfgoed koppeling
- [ ] Monumentenstatus per pand
- [ ] Impact op verbouwmogelijkheden

---

## Geïnventariseerde Databronnen

### Categorie 1: Woningwaarde & Transacties
| Bron | Data | Toegang | Status |
|------|------|---------|--------|
| **Kadaster Transacties** | Historische verkoopprijzen (vanaf 1993) | OpenKadaster.com (gratis basis) | Collector aanwezig |
| **WOZ Waardeloket** | Belastingwaardes per adres | woz-waardeloket.nl (gratis) | Geïmplementeerd |
| **CBS Huizenprijzen** | Prijsindices, gem. verkoopprijzen | OData API (gratis) | Collector aanwezig |
| **Funda Verkocht** | Vraagprijs vs verkoopprijs, looptijd | Scraping (grijs gebied) | Gepland |

### Categorie 2: Gebouw & Perceel
| Bron | Data | Toegang | Status |
|------|------|---------|--------|
| **BAG** | Bouwjaar, oppervlakte, gebruiksdoel | PDOK WFS (gratis) | Geïmplementeerd |
| **3DBAG** | 3D model, dakoriëntatie, gebouwhoogte | TU Delft (gratis) | Gepland |
| **Kadaster BRK** | Perceelgrenzen, eigendom | PDOK (gratis) | Gepland |
| **EP-Online/RVO** | Energielabels | RVO API (gratis, registratie) | Geïmplementeerd |

### Categorie 3: Buurtstatistieken
| Bron | Data | Toegang | Status |
|------|------|---------|--------|
| **CBS Kerncijfers** | 1000+ indicatoren per buurt | CBS OData (gratis) | Geïmplementeerd |
| **Leefbaarometer** | Leefbaarheidsscores | Download (gratis) | Geïmplementeerd |
| **LV.incijfers.nl** | Lokale buurtdata LV | Download/Dashboard | Gepland |

### Categorie 4: Omgeving & Voorzieningen
| Bron | Data | Toegang | Status |
|------|------|---------|--------|
| **CBS Nabijheid** | Afstand tot scholen, winkels, zorg | CBS OData | Gepland |
| **DUO** | Schoolkwaliteit, prestaties | Open data | Gepland |
| **OVapi** | OV bereikbaarheid, reistijden | API | Gepland |
| **RIVM** | Luchtkwaliteit, geluidsbelasting | Open data | Gepland |

### Categorie 5: Zon & Oriëntatie
| Bron | Data | Toegang | Status |
|------|------|---------|--------|
| **3DBAG** | Gebouworiëntatie (azimuth) | Download/API | Gepland |
| **AHN4** | Hoogtemodel voor schaduwberekening | PDOK (gratis) | Gepland |
| **Zonnekaart** | Geschiktheid zonnepanelen | Gemeentelijk | Gepland |
| **SunCalc/Shadowmap** | Zonstand berekeningen | Web API | Gepland |

### Categorie 6: Marktinformatie
| Bron | Data | Toegang | Status |
|------|------|---------|--------|
| **NVM (via CBS)** | Overbiedingspercentages | Indirect via CBS | Gepland |
| **Funda** | Actueel aanbod, looptijd | Scraping | Geïmplementeerd |
| **Makelaarsdata** | Vergelijkbare verkopen | Niet publiek | - |

---

## Technische verbeteringen

### Caching strategie
- Redis voor API responses
- Invalidatie bij data refresh
- Warm-up script voor populaire queries
- [x] File-based cache voor collectors (WOZ, energielabel, kadaster)

### Performance optimalisatie
- Database indexen fine-tunen
- Query optimalisatie
- Frontend lazy loading

### Monitoring & logging
- Structurele logging met correlatie IDs
- Error tracking (Sentry)
- Performance metrics (response times)

### CI/CD pipeline
- Automated tests
- Docker containers
- Deployment automatisering

### Multi-tenancy
- Gescheiden zoekprofielen
- Meerdere gebruikers ondersteunen
- Privacy-first ontwerp

---

## Notities

- Focus op **waardebepaling** als kern feature
- Houd het simpel - SQLite is voldoende voor persoonlijk gebruik
- Respecteer rate limits van externe APIs
- Privacy: geen persoonlijke data van derden opslaan
- WOZ en energielabel collectors gebruiken file-based caching
- Kadaster transactie data vereist betaalde toegang voor volledige functionaliteit
