"""
PFAS Bodemkwaliteitskaart Collector.

Haalt de gemeentelijke bodemkwaliteitskaart PFAS op voor Den Haag.
Bepaalt per locatie in welke PFAS-kwaliteitszone een adres valt
(point-in-polygon) en retourneert de bodemkwaliteitsclassificatie.

De classificaties volgen het Besluit bodemkwaliteit:
- "Landbouw/natuur" = achtergrondwaarde (schoonst)
- "Wonen" = geschikt voor wonen (licht verhoogd)
- "Industrie" = geschikt voor industrie (verhoogd)
- "Nvt" / "Niet gezoneerd" = geen classificatie

Bron: Gemeente Den Haag via CKAN dataplatform.nl
Dataset: bodemkwaliteitskaart (incl. PFAS)
Formaat: GeoJSON in EPSG:28992 (RD)
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from utils.geo import rd_to_wgs84


BODEMKAART_URL = (
    "https://ckan.dataplatform.nl/dataset/"
    "b5b78ecd-2c3b-4fbf-b333-25c4942b8efe/resource/"
    "73277ec0-c6c5-488e-b50d-232b3466c22b/download/bodemkwaliteitskaart.json"
)

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "pfas_bodemkaart"
CACHE_DURATION_SECONDS = 90 * 24 * 60 * 60  # 90 dagen (data wijzigt zelden)

# Kwaliteitsklassen geordend van schoon naar vervuild
KWALITEIT_RANKING = {
    "Landbouw/natuur": 1,  # Achtergrondwaarde (schoonst)
    "Wonen": 2,            # Geschikt voor wonen
    "Industrie": 3,        # Geschikt voor industrie
    "Nvt": 0,              # Niet geclassificeerd
    "Water": 0,            # Watergebied
}


@dataclass
class BodemZone:
    """Eén zone uit de bodemkwaliteitskaart (WGS84 polygoon)."""

    objectid: int
    functie: str  # "Wonen", "Industrie", "Overig", "Water"
    zone_bg: str  # Zone naam bovengrond
    zone_pfas: str  # PFAS zone naam
    kwaliteit_bg: str  # Kwaliteitsklasse bovengrond (0-0.5m)
    kwaliteit_og: str  # Kwaliteitsklasse ondergrond (0.5-2m)
    toepassing_bg: str  # Toepassingseis bovengrond
    toepassing_og: str  # Toepassingseis ondergrond
    polygon_wgs84: Any = None  # shapely Polygon in WGS84

    def to_dict(self) -> Dict[str, Any]:
        return {
            "objectid": self.objectid,
            "functie": self.functie,
            "zone_bg": self.zone_bg,
            "zone_pfas": self.zone_pfas,
            "kwaliteit_bg": self.kwaliteit_bg,
            "kwaliteit_og": self.kwaliteit_og,
            "toepassing_bg": self.toepassing_bg,
            "toepassing_og": self.toepassing_og,
        }


@dataclass
class BodemkaartResult:
    """PFAS bodemkwaliteit voor een locatie."""

    in_den_haag: bool = False
    functie: Optional[str] = None
    zone_naam: Optional[str] = None
    kwaliteit_bovengrond: Optional[str] = None  # "Landbouw/natuur", "Wonen", "Industrie"
    kwaliteit_ondergrond: Optional[str] = None
    toepassing_bovengrond: Optional[str] = None
    toepassing_ondergrond: Optional[str] = None
    kwaliteit_ranking: int = 0  # 0=onbekend, 1=schoon, 2=wonen, 3=industrie

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None and v != 0}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BodemkaartResult":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class PFASBodemkaartCollector:
    """Collector voor gemeentelijke PFAS bodemkwaliteitskaart Den Haag."""

    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    _zones: List[BodemZone] = field(default_factory=list, init=False, repr=False)
    _loaded: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self) -> Path:
        return self.cache_dir / "bodemkaart_zones.json"

    def _load_from_cache(self) -> bool:
        cache_path = self._cache_path()
        if not cache_path.exists():
            return False
        try:
            with cache_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("timestamp", 0) + CACHE_DURATION_SECONDS < time.time():
                return False
            self._build_zones_from_cache(data.get("zones", []))
            self._loaded = True
            return True
        except (json.JSONDecodeError, IOError, TypeError):
            return False

    def _build_zones_from_cache(self, zone_dicts: List[Dict]) -> None:
        """Herbouw BodemZone objecten met shapely polygonen vanuit cache."""
        try:
            from shapely.geometry import shape
        except ImportError:
            print("  PFAS bodemkaart: shapely niet beschikbaar")
            return

        for zd in zone_dicts:
            geom_data = zd.pop("_geometry_wgs84", None)
            zone = BodemZone(**{k: v for k, v in zd.items() if k in BodemZone.__dataclass_fields__ and k != "polygon_wgs84"})
            if geom_data:
                try:
                    zone.polygon_wgs84 = shape(geom_data)
                except Exception:
                    pass
            self._zones.append(zone)

    def _save_to_cache(self) -> None:
        """Sla zones op inclusief WGS84 geometrie."""
        try:
            from shapely.geometry import mapping
        except ImportError:
            return

        zone_dicts = []
        for zone in self._zones:
            d = zone.to_dict()
            if zone.polygon_wgs84 is not None:
                d["_geometry_wgs84"] = mapping(zone.polygon_wgs84)
            zone_dicts.append(d)

        cache_data = {
            "timestamp": time.time(),
            "zones": zone_dicts,
        }
        try:
            with self._cache_path().open("w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False)
        except IOError:
            pass

    def _convert_polygon_rd_to_wgs84(self, coords: List) -> List:
        """Converteer polygon-coördinaten van RD naar WGS84 [lng, lat]."""
        result = []
        for ring in coords:
            wgs_ring = []
            for point in ring:
                if len(point) >= 2:
                    lat, lng = rd_to_wgs84(point[0], point[1])
                    wgs_ring.append([lng, lat])
            result.append(wgs_ring)
        return result

    def _fetch_bodemkaart(self) -> None:
        """Download de bodemkwaliteitskaart GeoJSON en converteer naar WGS84."""
        try:
            from shapely.geometry import shape, Polygon as ShapelyPolygon
        except ImportError:
            print("  PFAS bodemkaart: shapely niet geïnstalleerd, overslaan")
            self._loaded = True
            return

        try:
            print("  PFAS bodemkaart: downloaden van CKAN...")
            response = requests.get(BODEMKAART_URL, timeout=60)
            response.raise_for_status()
            geojson = response.json()
            features = geojson.get("features", [])
            print(f"  PFAS bodemkaart: {len(features)} zones ontvangen")
        except requests.RequestException as e:
            print(f"  PFAS bodemkaart download fout: {e}")
            self._loaded = True
            return

        for feature in features:
            props = feature.get("properties", {})
            geom = feature.get("geometry", {})

            if geom.get("type") != "Polygon":
                continue

            rd_coords = geom.get("coordinates", [])
            if not rd_coords:
                continue

            # Converteer RD → WGS84
            wgs_coords = self._convert_polygon_rd_to_wgs84(rd_coords)
            wgs_geom = {"type": "Polygon", "coordinates": wgs_coords}

            try:
                polygon = shape(wgs_geom)
                if not polygon.is_valid:
                    polygon = polygon.buffer(0)  # Fix ongeldige geometrie
            except Exception:
                continue

            kwaliteit_bg = str(props.get("KWALIT_BG", "")).strip()

            zone = BodemZone(
                objectid=props.get("OBJECTID", 0),
                functie=str(props.get("FUNCTIE", "")).strip(),
                zone_bg=str(props.get("ZONE_BG", "")).strip(),
                zone_pfas=str(props.get("ZONE_PFAS", "")).strip(),
                kwaliteit_bg=kwaliteit_bg,
                kwaliteit_og=str(props.get("KWALIT_OG", "")).strip(),
                toepassing_bg=str(props.get("TOEP_BG", "")).strip(),
                toepassing_og=str(props.get("TOEP_OG", "")).strip(),
                polygon_wgs84=polygon,
            )
            self._zones.append(zone)

        self._loaded = True
        self._save_to_cache()
        print(f"  PFAS bodemkaart: {len(self._zones)} zones verwerkt")

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        if not self._load_from_cache():
            self._fetch_bodemkaart()

    def get_for_location(self, lat: float, lng: float) -> BodemkaartResult:
        """Bepaal de PFAS bodemkwaliteitszone voor een locatie."""
        self._ensure_loaded()

        try:
            from shapely.geometry import Point
        except ImportError:
            return BodemkaartResult()

        point = Point(lng, lat)  # shapely: (x=lng, y=lat)

        for zone in self._zones:
            if zone.polygon_wgs84 is None:
                continue
            try:
                if zone.polygon_wgs84.contains(point):
                    ranking = KWALITEIT_RANKING.get(zone.kwaliteit_bg, 0)
                    return BodemkaartResult(
                        in_den_haag=True,
                        functie=zone.functie,
                        zone_naam=zone.zone_bg,
                        kwaliteit_bovengrond=zone.kwaliteit_bg,
                        kwaliteit_ondergrond=zone.kwaliteit_og,
                        toepassing_bovengrond=zone.toepassing_bg,
                        toepassing_ondergrond=zone.toepassing_og,
                        kwaliteit_ranking=ranking,
                    )
            except Exception:
                continue

        return BodemkaartResult()

    def get_all_zones(self) -> List[Dict[str, Any]]:
        """Retourneer alle zones (zonder geometrie)."""
        self._ensure_loaded()
        return [z.to_dict() for z in self._zones]


def create_pfas_bodemkaart_collector(
    cache_dir: Optional[Path] = None,
) -> PFASBodemkaartCollector:
    """Factory function met default cache directory."""
    if cache_dir is None:
        cache_dir = Path(__file__).parent.parent.parent / "data" / "cache" / "pfas_bodemkaart"
    return PFASBodemkaartCollector(cache_dir=cache_dir)
