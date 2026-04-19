"""ORS Matrix Collector — multimodale afstandsberekening via OpenRouteService Matrix API.

Berekent routeafstanden en reistijden van één origin naar meerdere destinations.
Kiest automatisch de modaliteit (lopen, fietsen, auto) op basis van hemelsbreed afstand.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "ors_matrix"
ORS_BASE_URL = "https://api.openrouteservice.org/v2/matrix"

# ORS profile names
PROFILE_LOPEN = "foot-walking"
PROFILE_FIETSEN = "cycling-regular"
PROFILE_AUTO = "driving-car"

# Modaliteit labels (Nederlands)
MODALITEIT_MAP = {
    PROFILE_LOPEN: "lopen",
    PROFILE_FIETSEN: "fietsen",
    PROFILE_AUTO: "auto",
}

# Geschatte snelheden voor haversine fallback (m/s)
FALLBACK_SNELHEID = {
    PROFILE_LOPEN: 1.39,     # ~5 km/h
    PROFILE_FIETSEN: 4.17,   # ~15 km/h
    PROFILE_AUTO: 8.33,      # ~30 km/h (stadsverkeer)
}

# Promotie: als reistijd te hoog, stap over naar snellere modaliteit
PROMOTIE_MAP = {
    PROFILE_LOPEN: PROFILE_FIETSEN,
    PROFILE_FIETSEN: PROFILE_AUTO,
    # auto heeft geen promotie
}

# Default drempels als config niet geladen kan worden
DEFAULT_DREMPELS = {
    "lopen": {"max_hemelsbreed_m": 800},
    "fietsen": {"max_hemelsbreed_m": 5000},
}


def _haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Hemelsbreed afstand in meters."""
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _load_drempels(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """Laad ORS drempelwaarden uit scoring.yaml."""
    if config_path is None:
        config_path = Path(__file__).parent.parent.parent / "config" / "scoring.yaml"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        return config.get("ors_drempels", DEFAULT_DREMPELS)
    except (IOError, yaml.YAMLError):
        return DEFAULT_DREMPELS


@dataclass
class ORSMatrixResult:
    """Resultaat voor een enkel origin-destination paar."""
    dest_index: int
    dest_lat: float
    dest_lng: float
    afstand_m: int
    reistijd_sec: int
    modaliteit: str          # "lopen", "fietsen", "auto"
    is_fallback: bool        # True als haversine gebruikt werd

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dest_index": self.dest_index,
            "dest_lat": self.dest_lat,
            "dest_lng": self.dest_lng,
            "afstand_m": self.afstand_m,
            "reistijd_sec": self.reistijd_sec,
            "modaliteit": self.modaliteit,
            "is_fallback": self.is_fallback,
        }


@dataclass
class ORSMatrixCollector:
    """Collector voor multimodale afstanden via ORS Matrix API."""

    api_key: str = ""
    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)
    cache_days: int = 7
    min_delay: float = 1.5
    max_delay: float = 2.5
    session: Optional[requests.Session] = None
    drempels: Optional[Dict[str, Any]] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.api_key:
            self.api_key = os.environ.get("ORS_API_KEY", "")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({
                "Authorization": self.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            })
        if self.drempels is None:
            self.drempels = _load_drempels()

    def _rate_limit(self) -> None:
        elapsed = time.time() - self._last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.time()

    def _bepaal_profile(self, hemelsbreed_m: float) -> str:
        """Kies ORS profile op basis van hemelsbreed afstand."""
        lopen_max = self.drempels.get("lopen", {}).get("max_hemelsbreed_m", 800)
        fietsen_max = self.drempels.get("fietsen", {}).get("max_hemelsbreed_m", 5000)

        if hemelsbreed_m < lopen_max:
            return PROFILE_LOPEN
        elif hemelsbreed_m < fietsen_max:
            return PROFILE_FIETSEN
        else:
            return PROFILE_AUTO

    def _max_reistijd_sec(self, profile: str) -> Optional[int]:
        """Max reistijd in seconden voor een profile, of None als geen limiet."""
        profile_naar_key = {
            PROFILE_LOPEN: "lopen",
            PROFILE_FIETSEN: "fietsen",
        }
        key = profile_naar_key.get(profile)
        if not key:
            return None
        max_min = self.drempels.get(key, {}).get("max_reistijd_min")
        return int(max_min * 60) if max_min else None

    def _cache_key(self, profile: str, origin: Tuple[float, float],
                   destinations: List[Tuple[int, float, float]]) -> str:
        """Genereer cache key voor een matrix request."""
        o_lat = round(origin[0], 3)
        o_lng = round(origin[1], 3)
        dest_parts = sorted(f"{round(d[1], 4)},{round(d[2], 4)}" for d in destinations)
        raw = f"{profile}_{o_lat},{o_lng}_{'|'.join(dest_parts)}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _load_from_cache(self, key: str) -> Optional[Dict]:
        cache_file = self.cache_dir / f"{key}.json"
        if not cache_file.exists():
            return None
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            if data.get("_cached_at", 0) + (self.cache_days * 86400) < time.time():
                return None
            return data
        except (json.JSONDecodeError, IOError):
            return None

    def _save_to_cache(self, key: str, data: Dict) -> None:
        data["_cached_at"] = time.time()
        cache_file = self.cache_dir / f"{key}.json"
        try:
            cache_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        except IOError:
            pass

    def _fallback_result(self, dest_index: int, dest_lat: float, dest_lng: float,
                         origin_lat: float, origin_lng: float, profile: str) -> ORSMatrixResult:
        """Haversine fallback voor als ORS niet beschikbaar is."""
        hemelsbreed = _haversine(origin_lat, origin_lng, dest_lat, dest_lng)
        # Routeafstand is typisch 1.3x hemelsbreed
        afstand_m = int(hemelsbreed * 1.3)
        snelheid = FALLBACK_SNELHEID.get(profile, 1.39)
        reistijd_sec = int(afstand_m / snelheid)
        return ORSMatrixResult(
            dest_index=dest_index,
            dest_lat=dest_lat,
            dest_lng=dest_lng,
            afstand_m=afstand_m,
            reistijd_sec=reistijd_sec,
            modaliteit=MODALITEIT_MAP.get(profile, "lopen"),
            is_fallback=True,
        )

    def _matrix_request(
        self,
        profile: str,
        origin: Tuple[float, float],
        destinations: List[Tuple[int, float, float]],
    ) -> List[ORSMatrixResult]:
        """Doe een ORS Matrix API request voor één profile.

        Args:
            profile: ORS profile (foot-walking, cycling-regular, driving-car)
            origin: (lat, lng) van het startpunt
            destinations: lijst van (orig_index, lat, lng) tuples
        """
        if not destinations:
            return []

        cache_key = self._cache_key(profile, origin, destinations)
        cached = self._load_from_cache(cache_key)
        if cached and "results" in cached:
            results = []
            for r in cached["results"]:
                results.append(ORSMatrixResult(
                    dest_index=r["dest_index"],
                    dest_lat=r["dest_lat"],
                    dest_lng=r["dest_lng"],
                    afstand_m=r["afstand_m"],
                    reistijd_sec=r["reistijd_sec"],
                    modaliteit=r["modaliteit"],
                    is_fallback=r["is_fallback"],
                ))
            return results

        if not self.api_key:
            return [
                self._fallback_result(idx, lat, lng, origin[0], origin[1], profile)
                for idx, lat, lng in destinations
            ]

        self._rate_limit()

        # ORS verwacht [lng, lat] formaat
        locations = [[origin[1], origin[0]]]
        for _, lat, lng in destinations:
            locations.append([lng, lat])

        body = {
            "locations": locations,
            "sources": [0],
            "destinations": list(range(1, len(locations))),
            "metrics": ["distance", "duration"],
        }

        try:
            resp = self.session.post(
                f"{ORS_BASE_URL}/{profile}",
                json=body,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.warning("ORS Matrix request failed (%s): %s", profile, exc)
            return [
                self._fallback_result(idx, lat, lng, origin[0], origin[1], profile)
                for idx, lat, lng in destinations
            ]

        distances = data.get("distances", [[]])[0]
        durations = data.get("durations", [[]])[0]
        modaliteit = MODALITEIT_MAP.get(profile, "lopen")

        results: List[ORSMatrixResult] = []
        for i, (orig_idx, lat, lng) in enumerate(destinations):
            dist = distances[i] if i < len(distances) else None
            dur = durations[i] if i < len(durations) else None

            if dist is None or dur is None:
                results.append(self._fallback_result(orig_idx, lat, lng, origin[0], origin[1], profile))
            else:
                results.append(ORSMatrixResult(
                    dest_index=orig_idx,
                    dest_lat=lat,
                    dest_lng=lng,
                    afstand_m=int(dist),
                    reistijd_sec=int(dur),
                    modaliteit=modaliteit,
                    is_fallback=False,
                ))

        # Cache opslaan
        cache_data = {"results": [r.to_dict() for r in results]}
        self._save_to_cache(cache_key, cache_data)

        return results

    def get_afstanden(
        self,
        origin_lat: float,
        origin_lng: float,
        destinations: List[Tuple[float, float]],
    ) -> List[ORSMatrixResult]:
        """Bereken multimodale afstanden van origin naar meerdere destinations.

        Args:
            origin_lat, origin_lng: Startpunt coördinaten
            destinations: Lijst van (lat, lng) tuples

        Returns:
            Lijst van ORSMatrixResult, gesorteerd op dest_index (originele volgorde)
        """
        if not destinations:
            return []

        origin = (origin_lat, origin_lng)

        # Groepeer destinations per modaliteit
        groepen: Dict[str, List[Tuple[int, float, float]]] = {}
        for i, (lat, lng) in enumerate(destinations):
            hemelsbreed = _haversine(origin_lat, origin_lng, lat, lng)
            profile = self._bepaal_profile(hemelsbreed)
            groepen.setdefault(profile, []).append((i, lat, lng))

        # Per groep een matrix request
        alle_resultaten: Dict[int, ORSMatrixResult] = {}
        for profile, dests in groepen.items():
            # ORS Matrix API max 50 destinations per request
            for batch_start in range(0, len(dests), 49):
                batch = dests[batch_start:batch_start + 49]
                resultaten = self._matrix_request(profile, origin, batch)
                for r in resultaten:
                    alle_resultaten[r.dest_index] = r

        # Herclassificatie: promoveer naar snellere modaliteit als reistijd te hoog
        # Bijv. lopen > 10 min → herberekenen als fietsen
        # Werkt ook op fallback-resultaten zodat geschatte tijden correct gecategoriseerd worden
        te_promoveren: Dict[str, List[Tuple[int, float, float]]] = {}
        for idx, r in alle_resultaten.items():
            profile = next(
                (p for p, label in MODALITEIT_MAP.items() if label == r.modaliteit),
                None,
            )
            if not profile:
                continue
            max_sec = self._max_reistijd_sec(profile)
            next_profile = PROMOTIE_MAP.get(profile)
            if max_sec and next_profile and r.reistijd_sec > max_sec:
                te_promoveren.setdefault(next_profile, []).append(
                    (idx, r.dest_lat, r.dest_lng)
                )

        # Herberekening voor gepromoveerde POIs
        for profile, dests in te_promoveren.items():
            for batch_start in range(0, len(dests), 49):
                batch = dests[batch_start:batch_start + 49]
                resultaten = self._matrix_request(profile, origin, batch)
                for r in resultaten:
                    alle_resultaten[r.dest_index] = r

        # Sorteer op originele index
        return [alle_resultaten[i] for i in sorted(alle_resultaten.keys())]


def create_ors_matrix_collector(cache_dir: Optional[Path] = None) -> ORSMatrixCollector:
    """Factory function met default cache directory."""
    if cache_dir is None:
        cache_dir = CACHE_DIR
    return ORSMatrixCollector(cache_dir=cache_dir)
