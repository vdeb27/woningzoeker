"""OV (public transport) collector using OVapi.nl for nearby stops, lines, and frequency data."""

import hashlib
import json
import logging
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "ov"

# OVapi base URL
OVAPI_BASE = "http://v0.ovapi.nl"

# Transport type mapping from OVapi to Dutch labels
TRANSPORT_TYPE_MAP = {
    "BUS": "bus",
    "TRAM": "tram",
    "METRO": "metro",
    "TRAIN": "trein",
    "FERRY": "veerboot",
}

# Average speeds per transport type (km/h) for heuristic travel time
AVERAGE_SPEEDS = {
    "trein": 60,
    "metro": 35,
    "tram": 20,
    "bus": 25,
    "veerboot": 15,
}

# Score weights for OV-score calculation
SCORE_WEIGHTS = {
    "afstand_halte": 0.30,
    "type_vervoer": 0.25,
    "frequentie": 0.25,
    "verbinding_centrum": 0.20,
}

# Den Haag Centraal Station coordinates for "direct connection to center" scoring
DEN_HAAG_CS = {"lat": 52.0812, "lng": 4.3249, "naam": "Den Haag Centraal"}


def _haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate distance in meters between two coordinates using Haversine formula."""
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


@dataclass
class OVHalte:
    """A public transport stop with line information."""
    naam: str
    type: str  # "trein", "tram", "bus", "metro"
    lat: float
    lng: float
    afstand_m: int
    lijnen: List[str]  # e.g. ["Tram 1", "Bus 23"]
    stop_code: str = ""
    frequentie_spits: Optional[int] = None  # departures per hour in rush hour

    def to_dict(self) -> Dict[str, Any]:
        return {
            "naam": self.naam,
            "type": self.type,
            "lat": self.lat,
            "lng": self.lng,
            "afstand_m": self.afstand_m,
            "lijnen": self.lijnen,
            "stop_code": self.stop_code,
            "frequentie_spits": self.frequentie_spits,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OVHalte":
        return cls(
            naam=data["naam"],
            type=data["type"],
            lat=data["lat"],
            lng=data["lng"],
            afstand_m=data["afstand_m"],
            lijnen=data.get("lijnen", []),
            stop_code=data.get("stop_code", ""),
            frequentie_spits=data.get("frequentie_spits"),
        )


@dataclass
class OVReistijd:
    """Estimated OV travel time to a destination (heuristic)."""
    dest_naam: str
    dest_lat: float
    dest_lng: float
    reistijd_min: int
    overstappen: int
    route_beschrijving: str  # e.g. "Lopen 5 min → Tram 1 (12 min)"
    halte_naam: str = ""
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dest_naam": self.dest_naam,
            "dest_lat": self.dest_lat,
            "dest_lng": self.dest_lng,
            "reistijd_min": self.reistijd_min,
            "overstappen": self.overstappen,
            "route_beschrijving": self.route_beschrijving,
            "halte_naam": self.halte_naam,
            "error": self.error,
        }


@dataclass
class OVBereikbaarheid:
    """Complete OV accessibility result for a location."""
    ov_score: float  # 0.0 - 1.0
    dichtstbijzijnde_halte: Optional[OVHalte]
    haltes_nabij: List[OVHalte]
    reistijden: List[OVReistijd]
    score_breakdown: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ov_score": self.ov_score,
            "dichtstbijzijnde_halte": self.dichtstbijzijnde_halte.to_dict() if self.dichtstbijzijnde_halte else None,
            "haltes_nabij": [h.to_dict() for h in self.haltes_nabij],
            "reistijden": [r.to_dict() for r in self.reistijden],
            "score_breakdown": self.score_breakdown,
        }


@dataclass
class OVCollector:
    """Collector for OV data via OVapi.nl."""

    cache_dir: Optional[Path] = None
    cache_days: int = 7
    cache_days_frequency: int = 30
    min_delay: float = 1.0
    max_delay: float = 2.0
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, repr=False)
    _stops_cache: Optional[Dict] = field(default=None, repr=False)

    def __post_init__(self):
        if self.cache_dir is None:
            self.cache_dir = CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({
                "Accept": "application/json",
                "User-Agent": "Woningzoeker/1.0 (OV bereikbaarheid check)",
            })

    def _rate_limit(self):
        import random
        elapsed = time.time() - self._last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.time()

    def _cache_key(self, prefix: str, *args) -> str:
        raw = f"{prefix}_{'_'.join(str(a) for a in args)}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _load_from_cache(self, key: str, max_age_days: Optional[int] = None) -> Optional[Dict]:
        if max_age_days is None:
            max_age_days = self.cache_days
        cache_file = self.cache_dir / f"{key}.json"
        if not cache_file.exists():
            return None
        try:
            data = json.loads(cache_file.read_text())
            cached_time = data.get("_cached_at", 0)
            if time.time() - cached_time > max_age_days * 86400:
                return None
            return data
        except (json.JSONDecodeError, KeyError):
            return None

    def _save_to_cache(self, key: str, data: Dict):
        data["_cached_at"] = time.time()
        cache_file = self.cache_dir / f"{key}.json"
        cache_file.write_text(json.dumps(data, ensure_ascii=False))

    def _fetch_all_stops(self) -> Dict[str, Dict]:
        """Fetch all stop areas from OVapi. Cached regionally for efficiency."""
        if self._stops_cache is not None:
            return self._stops_cache

        cache_key = self._cache_key("all_stops")
        cached = self._load_from_cache(cache_key)
        if cached:
            stops = cached.get("stops", {})
            self._stops_cache = stops
            return stops

        self._rate_limit()
        try:
            resp = self.session.get(f"{OVAPI_BASE}/stopareacode/", timeout=30)
            resp.raise_for_status()
            stops = resp.json()
            self._save_to_cache(cache_key, {"stops": stops})
            self._stops_cache = stops
            return stops
        except requests.RequestException as exc:
            logger.warning("OVapi stopareacode fetch failed: %s", exc)
            return {}

    def _fetch_stop_details(self, stop_area_code: str) -> Optional[Dict]:
        """Fetch details for a stop area including lines."""
        cache_key = self._cache_key("stop", stop_area_code)
        cached = self._load_from_cache(cache_key)
        if cached:
            return cached

        self._rate_limit()
        try:
            resp = self.session.get(
                f"{OVAPI_BASE}/stopareacode/{stop_area_code}",
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            self._save_to_cache(cache_key, data)
            return data
        except requests.RequestException as exc:
            logger.warning("OVapi stop details fetch failed for %s: %s", stop_area_code, exc)
            return None

    def _extract_lines_from_stop(self, stop_data: Dict, stop_area_code: str) -> List[str]:
        """Extract line names (e.g. 'Tram 1', 'Bus 23') from stop detail data."""
        lines = set()
        # stop_data structure: {stop_area_code: {tpc_code: {Passes: {...}, ...}}}
        area_data = stop_data.get(stop_area_code, {})
        for tpc_code, tpc_data in area_data.items():
            if not isinstance(tpc_data, dict):
                continue
            passes = tpc_data.get("Passes", {})
            if not isinstance(passes, dict):
                continue
            for pass_id, pass_info in passes.items():
                if not isinstance(pass_info, dict):
                    continue
                transport_type = pass_info.get("TransportType", "").upper()
                line_number = pass_info.get("LinePublicNumber", "")
                if transport_type and line_number:
                    dutch_type = TRANSPORT_TYPE_MAP.get(transport_type, transport_type.lower())
                    lines.add(f"{dutch_type.capitalize()} {line_number}")
        return sorted(lines)

    def _count_departures_rush_hour(self, stop_data: Dict, stop_area_code: str) -> Optional[int]:
        """Count departures between 07:00-09:00 from stop data to estimate rush hour frequency."""
        departures = 0
        area_data = stop_data.get(stop_area_code, {})
        for tpc_code, tpc_data in area_data.items():
            if not isinstance(tpc_data, dict):
                continue
            passes = tpc_data.get("Passes", {})
            if not isinstance(passes, dict):
                continue
            for pass_id, pass_info in passes.items():
                if not isinstance(pass_info, dict):
                    continue
                expected = pass_info.get("ExpectedDepartureTime") or pass_info.get("TargetDepartureTime", "")
                if not expected:
                    continue
                # Format: "2024-01-15T07:30:00"
                try:
                    time_part = expected.split("T")[1] if "T" in expected else ""
                    hour = int(time_part.split(":")[0]) if time_part else -1
                    if 7 <= hour < 9:
                        departures += 1
                except (IndexError, ValueError):
                    continue

        if departures == 0:
            return None
        # Divide by 2 hours to get per-hour frequency
        return max(1, departures // 2)

    def _determine_stop_type(self, stop_info: Dict, lines: List[str]) -> str:
        """Determine the primary transport type at a stop."""
        # Check from line names first
        has_trein = any("Trein" in l or "Intercity" in l or "Sprinter" in l for l in lines)
        has_metro = any("Metro" in l for l in lines)
        has_tram = any("Tram" in l for l in lines)

        if has_trein:
            return "trein"
        if has_metro:
            return "metro"
        if has_tram:
            return "tram"
        return "bus"

    def get_nearby_stops(self, lat: float, lng: float, radius_m: int = 1000) -> List[OVHalte]:
        """Find all OV stops within radius of given coordinates.

        Args:
            lat, lng: Center coordinates
            radius_m: Search radius in meters (default 1000)

        Returns:
            List of OVHalte sorted by distance
        """
        # Check cache for this specific location query
        cache_key = self._cache_key("nearby", f"{lat:.4f}", f"{lng:.4f}", radius_m)
        cached = self._load_from_cache(cache_key)
        if cached:
            return [OVHalte.from_dict(h) for h in cached.get("haltes", [])]

        all_stops = self._fetch_all_stops()
        if not all_stops:
            return []

        nearby = []
        for code, stop_info in all_stops.items():
            if not isinstance(stop_info, dict):
                continue
            stop_lat = stop_info.get("Latitude") or stop_info.get("latitude")
            stop_lng = stop_info.get("Longitude") or stop_info.get("longitude")
            if not stop_lat or not stop_lng:
                continue
            try:
                stop_lat = float(stop_lat)
                stop_lng = float(stop_lng)
            except (ValueError, TypeError):
                continue

            distance = _haversine(lat, lng, stop_lat, stop_lng)
            if distance <= radius_m:
                name = (
                    stop_info.get("TimingPointName")
                    or stop_info.get("Name")
                    or stop_info.get("Description", code)
                )
                nearby.append({
                    "code": code,
                    "naam": name,
                    "lat": stop_lat,
                    "lng": stop_lng,
                    "afstand_m": round(distance),
                    "town": stop_info.get("TimingPointTown", ""),
                })

        # Sort by distance
        nearby.sort(key=lambda x: x["afstand_m"])

        # Fetch line details for nearby stops (limit to closest 10 to avoid too many API calls)
        haltes = []
        for stop in nearby[:15]:
            stop_detail = self._fetch_stop_details(stop["code"])
            lines = []
            freq = None
            if stop_detail:
                lines = self._extract_lines_from_stop(stop_detail, stop["code"])
                freq = self._count_departures_rush_hour(stop_detail, stop["code"])

            stop_type = self._determine_stop_type({}, lines) if lines else "bus"

            haltes.append(OVHalte(
                naam=stop["naam"],
                type=stop_type,
                lat=stop["lat"],
                lng=stop["lng"],
                afstand_m=stop["afstand_m"],
                lijnen=lines,
                stop_code=stop["code"],
                frequentie_spits=freq,
            ))

        # Cache the result
        self._save_to_cache(cache_key, {"haltes": [h.to_dict() for h in haltes]})
        return haltes

    def estimate_travel_time(
        self,
        origin_lat: float,
        origin_lng: float,
        dest_lat: float,
        dest_lng: float,
        dest_naam: str,
        nearby_stops: Optional[List[OVHalte]] = None,
    ) -> OVReistijd:
        """Estimate OV travel time using a heuristic approach.

        Heuristic:
        1. Walk to nearest suitable stop
        2. Wait (half the headway)
        3. In-vehicle time based on straight-line distance and average speed
        4. Transfer penalty if needed

        Args:
            origin_lat, origin_lng: Start coordinates
            dest_lat, dest_lng: Destination coordinates
            dest_naam: Human-readable destination name
            nearby_stops: Pre-fetched nearby stops (optional, will fetch if None)

        Returns:
            OVReistijd with estimated travel time
        """
        if nearby_stops is None:
            nearby_stops = self.get_nearby_stops(origin_lat, origin_lng, radius_m=1500)

        if not nearby_stops:
            return OVReistijd(
                dest_naam=dest_naam,
                dest_lat=dest_lat,
                dest_lng=dest_lng,
                reistijd_min=0,
                overstappen=0,
                route_beschrijving="",
                error="Geen OV-halte gevonden binnen 1,5 km",
            )

        # Find the best stop to use (prefer higher-capacity transport for longer distances)
        total_distance = _haversine(origin_lat, origin_lng, dest_lat, dest_lng)
        best_stop = self._pick_best_stop(nearby_stops, total_distance)

        # 1. Walking time to stop
        walk_min = round(best_stop.afstand_m / 83.3)  # 5 km/h walking speed

        # 2. Wait time (half headway)
        if best_stop.frequentie_spits and best_stop.frequentie_spits > 0:
            wait_min = round(60 / best_stop.frequentie_spits / 2)
        else:
            # Default wait times by type
            default_waits = {"trein": 8, "metro": 4, "tram": 5, "bus": 7}
            wait_min = default_waits.get(best_stop.type, 7)

        # 3. In-vehicle time based on distance from stop to destination
        stop_to_dest = _haversine(best_stop.lat, best_stop.lng, dest_lat, dest_lng)
        speed = AVERAGE_SPEEDS.get(best_stop.type, 25)
        # Add 30% to straight-line distance for route indirectness
        vehicle_km = (stop_to_dest / 1000) * 1.3
        vehicle_min = round((vehicle_km / speed) * 60)

        # 4. Estimate transfers
        overstappen = 0
        transfer_penalty = 0
        if total_distance > 10000 and best_stop.type in ("bus", "tram"):
            # Long distance on local transport likely needs a transfer
            overstappen = 1
            transfer_penalty = 5

        total_min = walk_min + wait_min + vehicle_min + transfer_penalty
        total_min = max(total_min, 1)

        # Build description
        parts = [f"Lopen {walk_min} min"]
        type_label = best_stop.type.capitalize()
        line_hint = best_stop.lijnen[0] if best_stop.lijnen else type_label
        parts.append(f"{line_hint} ({vehicle_min} min)")
        if overstappen > 0:
            parts.append(f"+{overstappen} overstap")

        return OVReistijd(
            dest_naam=dest_naam,
            dest_lat=dest_lat,
            dest_lng=dest_lng,
            reistijd_min=total_min,
            overstappen=overstappen,
            route_beschrijving=" → ".join(parts),
            halte_naam=best_stop.naam,
        )

    def _pick_best_stop(self, stops: List[OVHalte], total_distance: float) -> OVHalte:
        """Pick the best stop for a trip based on transport type and distance."""
        # For short trips (<3km), prefer closest stop
        if total_distance < 3000:
            return stops[0]

        # For longer trips, prefer higher-capacity transport if not too far away
        type_priority = {"trein": 4, "metro": 3, "tram": 2, "bus": 1}
        best = stops[0]
        best_score = 0

        for stop in stops:
            # Don't walk more than 15 min to a stop
            if stop.afstand_m > 1200:
                continue
            priority = type_priority.get(stop.type, 1)
            # Penalize distance to stop
            distance_penalty = stop.afstand_m / 1200  # 0-1
            score = priority * (1 - distance_penalty * 0.5)
            if score > best_score:
                best_score = score
                best = stop

        return best

    def calculate_ov_score(
        self,
        haltes: List[OVHalte],
        has_direct_cs_connection: bool = False,
    ) -> tuple[float, Dict[str, float]]:
        """Calculate OV accessibility score (0-1) from nearby stops.

        Score components:
        - afstand_halte (0.30): Distance to nearest stop
        - type_vervoer (0.25): Best transport type available
        - frequentie (0.25): Rush hour frequency at nearest stop
        - verbinding_centrum (0.20): Direct connection to Den Haag CS

        Returns:
            Tuple of (total_score, breakdown_dict)
        """
        breakdown = {
            "afstand_halte": 0.0,
            "type_vervoer": 0.0,
            "frequentie": 0.0,
            "verbinding_centrum": 0.0,
        }

        if not haltes:
            return 0.0, breakdown

        nearest = haltes[0]

        # 1. Distance to nearest stop
        if nearest.afstand_m < 300:
            breakdown["afstand_halte"] = 1.0
        elif nearest.afstand_m < 500:
            breakdown["afstand_halte"] = 0.7
        elif nearest.afstand_m < 800:
            breakdown["afstand_halte"] = 0.4
        elif nearest.afstand_m < 1000:
            breakdown["afstand_halte"] = 0.2
        else:
            breakdown["afstand_halte"] = 0.0

        # 2. Best transport type available (within 1km)
        types_nearby = {h.type for h in haltes if h.afstand_m <= 1000}
        if "trein" in types_nearby:
            breakdown["type_vervoer"] = 1.0
        elif "metro" in types_nearby:
            breakdown["type_vervoer"] = 0.8
        elif "tram" in types_nearby:
            breakdown["type_vervoer"] = 0.6
        elif "bus" in types_nearby:
            breakdown["type_vervoer"] = 0.3

        # 3. Frequency at nearest stop
        freq = nearest.frequentie_spits
        if freq is not None:
            if freq >= 12:
                breakdown["frequentie"] = 1.0
            elif freq >= 8:
                breakdown["frequentie"] = 0.8
            elif freq >= 4:
                breakdown["frequentie"] = 0.5
            elif freq >= 2:
                breakdown["frequentie"] = 0.2
        else:
            # No frequency data, give partial score based on type
            default_scores = {"trein": 0.6, "metro": 0.6, "tram": 0.5, "bus": 0.3}
            breakdown["frequentie"] = default_scores.get(nearest.type, 0.2)

        # 4. Direct connection to Den Haag CS
        if has_direct_cs_connection:
            breakdown["verbinding_centrum"] = 1.0
        else:
            # Check if any stop within 1km has lines that likely go to CS
            # Heuristic: tram/train stops in Den Haag usually connect to CS
            for h in haltes:
                if h.afstand_m > 1000:
                    break
                if h.type == "trein":
                    breakdown["verbinding_centrum"] = 0.8
                    break
                if h.type == "tram":
                    breakdown["verbinding_centrum"] = 0.5
                    break

        # Weighted sum
        total = sum(
            breakdown[k] * SCORE_WEIGHTS[k]
            for k in SCORE_WEIGHTS
        )
        return round(total, 3), breakdown

    def get_bereikbaarheid(
        self,
        lat: float,
        lng: float,
        werklocaties: Optional[List[Dict[str, Any]]] = None,
    ) -> OVBereikbaarheid:
        """Get complete OV accessibility data for a location.

        Args:
            lat, lng: Location coordinates
            werklocaties: List of dicts with 'naam', 'lat', 'lng'

        Returns:
            OVBereikbaarheid with score, stops, and travel times
        """
        haltes = self.get_nearby_stops(lat, lng, radius_m=1000)

        # Check for direct CS connection
        has_direct_cs = False
        cs_distance = _haversine(lat, lng, DEN_HAAG_CS["lat"], DEN_HAAG_CS["lng"])
        if cs_distance < 500:
            has_direct_cs = True
        elif haltes:
            # Check if any nearby train stop could be a direct connection
            for h in haltes:
                if h.type == "trein" and h.afstand_m < 1000:
                    has_direct_cs = True
                    break

        ov_score, breakdown = self.calculate_ov_score(haltes, has_direct_cs)

        # Estimate travel times to werklocaties
        reistijden = []
        if werklocaties:
            for wl in werklocaties:
                reistijd = self.estimate_travel_time(
                    origin_lat=lat,
                    origin_lng=lng,
                    dest_lat=wl["lat"],
                    dest_lng=wl["lng"],
                    dest_naam=wl["naam"],
                    nearby_stops=haltes,
                )
                reistijden.append(reistijd)

        return OVBereikbaarheid(
            ov_score=ov_score,
            dichtstbijzijnde_halte=haltes[0] if haltes else None,
            haltes_nabij=haltes,
            reistijden=reistijden,
            score_breakdown=breakdown,
        )


def create_ov_collector(cache_dir: Optional[Path] = None) -> OVCollector:
    """Factory function with default cache directory."""
    if cache_dir is None:
        cache_dir = CACHE_DIR
    return OVCollector(cache_dir=cache_dir)
