"""
3DBAG collector for building height, roof data, and orientation.

Fetches 3D building attributes from api.3dbag.nl, including
roof height, ground level, roof type, building volume,
roof orientation (azimuth), and building footprint geometry.
Also supports spatial queries for surrounding buildings.
"""

from __future__ import annotations

import json
import logging
import math
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


@dataclass
class DrieDBagResult:
    """Result from 3DBAG API lookup."""

    pand_identificatie: str
    h_dak_max: Optional[float] = None
    h_dak_min: Optional[float] = None
    h_dak_50p: Optional[float] = None
    h_dak_70p: Optional[float] = None
    h_maaiveld: Optional[float] = None
    dak_type: Optional[str] = None
    bouwlagen: Optional[int] = None
    opp_grond: Optional[float] = None
    opp_dak_plat: Optional[float] = None
    opp_dak_schuin: Optional[float] = None
    volume_lod22: Optional[float] = None
    gebouwhoogte: Optional[float] = None
    dak_azimut: Optional[float] = None
    dak_hellingshoek: Optional[float] = None
    dak_delen: Optional[List[Dict[str, Any]]] = None
    footprint_rd: Optional[List[List[float]]] = None
    fetch_date: datetime = field(default_factory=datetime.now)
    source: str = "3dbag.nl"
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pand_identificatie": self.pand_identificatie,
            "h_dak_max": self.h_dak_max,
            "h_dak_min": self.h_dak_min,
            "h_dak_50p": self.h_dak_50p,
            "h_dak_70p": self.h_dak_70p,
            "h_maaiveld": self.h_maaiveld,
            "dak_type": self.dak_type,
            "bouwlagen": self.bouwlagen,
            "opp_grond": self.opp_grond,
            "opp_dak_plat": self.opp_dak_plat,
            "opp_dak_schuin": self.opp_dak_schuin,
            "volume_lod22": self.volume_lod22,
            "gebouwhoogte": self.gebouwhoogte,
            "dak_azimut": self.dak_azimut,
            "dak_hellingshoek": self.dak_hellingshoek,
            "dak_delen": self.dak_delen,
            "footprint_rd": self.footprint_rd,
            "fetch_date": self.fetch_date.isoformat(),
            "source": self.source,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DrieDBagResult":
        fetch_date = data.get("fetch_date")
        if isinstance(fetch_date, str):
            fetch_date = datetime.fromisoformat(fetch_date)
        elif fetch_date is None:
            fetch_date = datetime.now()

        return cls(
            pand_identificatie=data.get("pand_identificatie", ""),
            h_dak_max=data.get("h_dak_max"),
            h_dak_min=data.get("h_dak_min"),
            h_dak_50p=data.get("h_dak_50p"),
            h_dak_70p=data.get("h_dak_70p"),
            h_maaiveld=data.get("h_maaiveld"),
            dak_type=data.get("dak_type"),
            bouwlagen=data.get("bouwlagen"),
            opp_grond=data.get("opp_grond"),
            opp_dak_plat=data.get("opp_dak_plat"),
            opp_dak_schuin=data.get("opp_dak_schuin"),
            volume_lod22=data.get("volume_lod22"),
            gebouwhoogte=data.get("gebouwhoogte"),
            dak_azimut=data.get("dak_azimut"),
            dak_hellingshoek=data.get("dak_hellingshoek"),
            dak_delen=data.get("dak_delen"),
            footprint_rd=data.get("footprint_rd"),
            fetch_date=fetch_date,
            source=data.get("source", "3dbag.nl"),
            error=data.get("error"),
        )


@dataclass
class DrieDBagCollector:
    """
    Collector for 3D building data from 3DBAG.

    Fetches building height, roof type, and volume data from the
    3DBAG API (api.3dbag.nl). Buildings are looked up by BAG
    pand identificatie.

    Parameters
    ----------
    min_delay : float
        Minimum delay between requests in seconds (default: 1.0)
    max_delay : float
        Maximum delay between requests in seconds (default: 2.0)
    cache_dir : Path, optional
        Directory for caching results (default: data/cache/3dbag)
    cache_days : int
        Number of days to cache results (default: 365)
    """

    min_delay: float = 1.0
    max_delay: float = 2.0
    cache_dir: Optional[Path] = None
    cache_days: int = 365
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    BASE_URL = "https://api.3dbag.nl/collections/pand/items"

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/geo+json",
        }

    def _rate_limit(self) -> None:
        now = time.perf_counter()
        elapsed = now - self._last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.perf_counter()

    def _get_cache_key(self, pand_id: str) -> str:
        safe_id = pand_id.replace(".", "_")
        return f"3dbag_{safe_id}"

    def _load_from_cache(self, pand_id: str) -> Optional[DrieDBagResult]:
        if not self.cache_dir:
            return None

        cache_path = self.cache_dir / f"{self._get_cache_key(pand_id)}.json"
        if not cache_path.exists():
            return None

        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            result = DrieDBagResult.from_dict(data)
            cache_age = datetime.now() - result.fetch_date
            if cache_age > timedelta(days=self.cache_days):
                return None

            return result
        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    def _save_to_cache(self, result: DrieDBagResult) -> None:
        if not self.cache_dir:
            return

        cache_path = self.cache_dir / f"{self._get_cache_key(result.pand_identificatie)}.json"
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)
        except IOError:
            pass

    def _normalize_pand_id(self, pand_identificatie: str) -> str:
        """Ensure pand ID has the NL.IMBAG.Pand. prefix."""
        if pand_identificatie.startswith("NL.IMBAG.Pand."):
            return pand_identificatie
        return f"NL.IMBAG.Pand.{pand_identificatie}"

    def get_building_data(
        self,
        pand_identificatie: str,
        use_cache: bool = True,
    ) -> DrieDBagResult:
        """
        Fetch 3D building data for a BAG pand.

        Parameters
        ----------
        pand_identificatie : str
            BAG pand identificatie (e.g., "0518100000285158" or
            "NL.IMBAG.Pand.0518100000285158")
        use_cache : bool
            Whether to use cached results (default: True)

        Returns
        -------
        DrieDBagResult
            Building height and roof data
        """
        full_id = self._normalize_pand_id(pand_identificatie)
        raw_id = pand_identificatie.replace("NL.IMBAG.Pand.", "")

        if use_cache:
            cached = self._load_from_cache(raw_id)
            if cached:
                return cached

        result = DrieDBagResult(pand_identificatie=raw_id)

        try:
            self._rate_limit()

            url = f"{self.BASE_URL}/{full_id}"
            logger.info(f"Fetching 3DBAG data for {full_id}")
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            # CityJSON format: data is nested under feature -> CityObjects
            feature = data.get("feature", data)
            city_objects = feature.get("CityObjects", {})
            if not city_objects:
                result.error = "Geen gebouwdata gevonden in 3DBAG"
                if use_cache:
                    self._save_to_cache(result)
                return result

            # Get the parent city object (without suffix like -0)
            # The parent object contains the building-level attributes
            obj = None
            for key, val in city_objects.items():
                if not key.endswith(("-0", "-1", "-2")):
                    obj = val
                    break
            if obj is None:
                obj = next(iter(city_objects.values()))
            attrs = obj.get("attributes", {})

            result.h_dak_max = attrs.get("b3_h_dak_max")
            result.h_dak_min = attrs.get("b3_h_dak_min")
            result.h_dak_50p = attrs.get("b3_h_dak_50p")
            result.h_dak_70p = attrs.get("b3_h_dak_70p")
            result.h_maaiveld = attrs.get("b3_h_maaiveld")
            result.dak_type = attrs.get("b3_dak_type")
            result.bouwlagen = attrs.get("b3_bouwlagen")
            result.opp_grond = attrs.get("b3_opp_grond")
            result.opp_dak_plat = attrs.get("b3_opp_dak_plat")
            result.opp_dak_schuin = attrs.get("b3_opp_dak_schuin")
            result.volume_lod22 = attrs.get("b3_volume_lod22")

            # Compute building height
            if result.h_dak_max is not None and result.h_maaiveld is not None:
                result.gebouwhoogte = round(result.h_dak_max - result.h_maaiveld, 2)

            # Extract roof orientation from child objects' semantic surfaces
            result.dak_delen = self._extract_roof_parts(city_objects)
            if result.dak_delen:
                result.dak_azimut, result.dak_hellingshoek = (
                    self._compute_weighted_roof_orientation(result.dak_delen)
                )

            # Extract footprint polygon in RD coordinates
            metadata = data.get("metadata", {})
            transform = metadata.get("transform", {})
            if not transform:
                transform = metadata.get("metadata", {}).get("transform", {})
            vertices = feature.get("vertices", [])
            if obj and vertices and transform:
                result.footprint_rd = self._extract_footprint(
                    obj, vertices, transform
                )

            logger.info(
                f"3DBAG data for {raw_id}: hoogte={result.gebouwhoogte}m, "
                f"dak={result.dak_type}, bouwlagen={result.bouwlagen}, "
                f"azimut={result.dak_azimut}, footprint={'ja' if result.footprint_rd else 'nee'}"
            )

        except requests.RequestException as e:
            logger.error(f"Error fetching 3DBAG data: {e}")
            result.error = f"Fout bij ophalen 3DBAG data: {str(e)}"

        if use_cache:
            self._save_to_cache(result)

        return result


    @staticmethod
    def _extract_roof_parts(
        city_objects: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Extract roof part orientations from child object semantic surfaces."""
        roof_parts = []
        for key, val in city_objects.items():
            # Child objects have suffix like -0, -1, -2
            if not any(key.endswith(f"-{i}") for i in range(20)):
                continue
            for geo in val.get("geometry", []):
                if geo.get("lod") != "2.2":
                    continue
                surfaces = geo.get("semantics", {}).get("surfaces", [])
                for surface in surfaces:
                    if surface.get("type") != "RoofSurface":
                        continue
                    azimut = surface.get("b3_azimut")
                    hellingshoek = surface.get("b3_hellingshoek")
                    if azimut is not None:
                        h_max = surface.get("b3_h_dak_max")
                        h_min = surface.get("b3_h_dak_min")
                        roof_parts.append(
                            {
                                "azimut": azimut,
                                "hellingshoek": hellingshoek,
                                "h_dak_max": h_max,
                                "h_dak_min": h_min,
                            }
                        )
        return roof_parts

    @staticmethod
    def _compute_weighted_roof_orientation(
        roof_parts: List[Dict[str, Any]],
    ) -> tuple[Optional[float], Optional[float]]:
        """Compute area-weighted average roof azimuth and slope.

        Uses circular mean for azimuth to handle the 0/360 wraparound.
        Only considers sloped surfaces (hellingshoek > 5 degrees).
        """
        sin_sum = 0.0
        cos_sum = 0.0
        slope_sum = 0.0
        count = 0

        for part in roof_parts:
            azimut = part.get("azimut")
            hellingshoek = part.get("hellingshoek")
            if azimut is None:
                continue
            # Only use meaningfully sloped surfaces for orientation
            if hellingshoek is not None and hellingshoek > 5.0:
                rad = math.radians(azimut)
                sin_sum += math.sin(rad)
                cos_sum += math.cos(rad)
                slope_sum += hellingshoek
                count += 1

        if count == 0:
            # No sloped surfaces; return first available azimuth as fallback
            for part in roof_parts:
                if part.get("azimut") is not None:
                    avg_slope = part.get("hellingshoek")
                    return round(part["azimut"], 1), (
                        round(avg_slope, 1) if avg_slope else None
                    )
            return None, None

        avg_azimut = math.degrees(math.atan2(sin_sum, cos_sum)) % 360
        avg_slope = slope_sum / count
        return round(avg_azimut, 1), round(avg_slope, 1)

    @staticmethod
    def _extract_footprint(
        parent_obj: Dict[str, Any],
        vertices: List[List[int]],
        transform: Dict[str, Any],
    ) -> Optional[List[List[float]]]:
        """Convert CityJSON LoD 0 footprint vertices to RD coordinates."""
        scale = transform.get("scale", [1, 1, 1])
        translate = transform.get("translate", [0, 0, 0])

        # Find LoD 0 geometry (footprint)
        for geo in parent_obj.get("geometry", []):
            if geo.get("lod") == "0":
                boundaries = geo.get("boundaries", [])
                if not boundaries or not boundaries[0]:
                    continue
                ring_indices = boundaries[0][0]
                coords = []
                for idx in ring_indices:
                    if idx < len(vertices):
                        v = vertices[idx]
                        x = v[0] * scale[0] + translate[0]
                        y = v[1] * scale[1] + translate[1]
                        coords.append([round(x, 3), round(y, 3)])
                if coords:
                    return coords
        return None

    def get_surrounding_buildings(
        self,
        rd_x: float,
        rd_y: float,
        radius: float = 75.0,
        exclude_pand_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch surrounding buildings within a bounding box.

        Parameters
        ----------
        rd_x, rd_y : float
            Center point in RD coordinates (EPSG:28992)
        radius : float
            Half-width of bounding box in meters (default: 75m)
        exclude_pand_id : str, optional
            Pand ID to exclude (the building itself)

        Returns
        -------
        list of dict
            Each dict has: pand_id, hoogte, footprint_rd
        """
        # Round bbox for cache key stability
        bbox_key = (
            f"{int(rd_x - radius)}_{int(rd_y - radius)}_"
            f"{int(rd_x + radius)}_{int(rd_y + radius)}"
        )

        # Check cache
        if self.cache_dir:
            cache_path = self.cache_dir / f"3dbag_bbox_{bbox_key}.json"
            if cache_path.exists():
                try:
                    with open(cache_path, "r", encoding="utf-8") as f:
                        cached = json.load(f)
                    cache_date = datetime.fromisoformat(cached.get("fetch_date", ""))
                    if datetime.now() - cache_date < timedelta(days=30):
                        buildings = cached.get("buildings", [])
                        if exclude_pand_id:
                            buildings = [
                                b
                                for b in buildings
                                if b.get("pand_id") != exclude_pand_id
                            ]
                        return buildings
                except (json.JSONDecodeError, ValueError):
                    pass

        bbox = f"{rd_x - radius},{rd_y - radius},{rd_x + radius},{rd_y + radius}"
        buildings = []

        try:
            self._rate_limit()
            url = f"{self.BASE_URL}?bbox={bbox}&limit=100"
            logger.info(f"Fetching surrounding buildings: bbox={bbox}")
            response = self.session.get(
                url,
                headers={"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            metadata = data.get("metadata", {})
            transform = metadata.get("transform", {})
            if not transform:
                transform = metadata.get("metadata", {}).get("transform", {})

            for feature in data.get("features", []):
                feat_vertices = feature.get("vertices", [])
                city_objects = feature.get("CityObjects", {})

                for key, val in city_objects.items():
                    # Skip child objects
                    if any(key.endswith(f"-{i}") for i in range(20)):
                        continue
                    attrs = val.get("attributes", {})
                    pand_id = attrs.get("identificatie", key).replace(
                        "NL.IMBAG.Pand.", ""
                    )
                    h_max = attrs.get("b3_h_dak_max")
                    h_mv = attrs.get("b3_h_maaiveld")
                    hoogte = round(h_max - h_mv, 2) if h_max and h_mv else None

                    footprint = None
                    if feat_vertices and transform:
                        footprint = self._extract_footprint(
                            val, feat_vertices, transform
                        )

                    if hoogte and hoogte > 1.0:
                        buildings.append(
                            {
                                "pand_id": pand_id,
                                "hoogte": hoogte,
                                "footprint_rd": footprint,
                            }
                        )

        except requests.RequestException as e:
            logger.error(f"Error fetching surrounding buildings: {e}")

        # Save to cache
        if self.cache_dir:
            cache_path = self.cache_dir / f"3dbag_bbox_{bbox_key}.json"
            try:
                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(
                        {"fetch_date": datetime.now().isoformat(), "buildings": buildings},
                        f,
                        ensure_ascii=False,
                    )
            except IOError:
                pass

        if exclude_pand_id:
            buildings = [
                b for b in buildings if b.get("pand_id") != exclude_pand_id
            ]

        return buildings


    def get_building_by_location(
        self,
        rd_x: float,
        rd_y: float,
        use_cache: bool = True,
    ) -> Optional[DrieDBagResult]:
        """Fetch building data using a spatial query instead of pand ID.

        Useful when the pand_id maps to wrong coordinates in 3DBAG
        (e.g. due to municipal mergers).

        Parameters
        ----------
        rd_x, rd_y : float
            Building location in RD coordinates (EPSG:28992)

        Returns
        -------
        DrieDBagResult or None
        """
        cache_key = f"3dbag_loc_{int(rd_x)}_{int(rd_y)}"

        if use_cache and self.cache_dir:
            cache_path = self.cache_dir / f"{cache_key}.json"
            if cache_path.exists():
                try:
                    with open(cache_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    result = DrieDBagResult.from_dict(data)
                    if datetime.now() - result.fetch_date < timedelta(days=self.cache_days):
                        return result
                except (json.JSONDecodeError, KeyError, ValueError):
                    pass

        # Small bbox around point (15m)
        radius = 15
        bbox = f"{rd_x - radius},{rd_y - radius},{rd_x + radius},{rd_y + radius}"

        try:
            self._rate_limit()
            url = f"{self.BASE_URL}?bbox={bbox}&limit=5"
            logger.info(f"Fetching 3DBAG by location: bbox={bbox}")
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            metadata = data.get("metadata", {})
            transform = metadata.get("transform", {})
            if not transform:
                transform = metadata.get("metadata", {}).get("transform", {})

            # Find the building closest to our point
            from shapely.geometry import Point
            target = Point(rd_x, rd_y)
            best_result = None
            best_dist = float("inf")

            for feature in data.get("features", []):
                feat_verts = feature.get("vertices", [])
                city_objects = feature.get("CityObjects", {})

                for key, val in city_objects.items():
                    if any(key.endswith(f"-{i}") for i in range(20)):
                        continue

                    attrs = val.get("attributes", {})
                    pand_id = attrs.get("identificatie", key).replace(
                        "NL.IMBAG.Pand.", ""
                    )

                    footprint = self._extract_footprint(val, feat_verts, transform) if feat_verts and transform else None
                    if not footprint:
                        continue

                    from shapely.geometry import Polygon as ShapelyPoly
                    fp_poly = ShapelyPoly(footprint)
                    dist = target.distance(fp_poly)

                    if dist < best_dist:
                        best_dist = dist

                        result = DrieDBagResult(pand_identificatie=pand_id)
                        result.h_dak_max = attrs.get("b3_h_dak_max")
                        result.h_dak_min = attrs.get("b3_h_dak_min")
                        result.h_dak_50p = attrs.get("b3_h_dak_50p")
                        result.h_dak_70p = attrs.get("b3_h_dak_70p")
                        result.h_maaiveld = attrs.get("b3_h_maaiveld")
                        result.dak_type = attrs.get("b3_dak_type")
                        result.bouwlagen = attrs.get("b3_bouwlagen")
                        result.opp_grond = attrs.get("b3_opp_grond")
                        result.opp_dak_plat = attrs.get("b3_opp_dak_plat")
                        result.opp_dak_schuin = attrs.get("b3_opp_dak_schuin")
                        result.volume_lod22 = attrs.get("b3_volume_lod22")
                        result.footprint_rd = footprint

                        if result.h_dak_max is not None and result.h_maaiveld is not None:
                            result.gebouwhoogte = round(result.h_dak_max - result.h_maaiveld, 2)

                        # Roof orientation from child objects
                        result.dak_delen = self._extract_roof_parts(city_objects)
                        if result.dak_delen:
                            result.dak_azimut, result.dak_hellingshoek = (
                                self._compute_weighted_roof_orientation(result.dak_delen)
                            )

                        best_result = result

            if best_result:
                # Cache the result
                if self.cache_dir:
                    cache_path = self.cache_dir / f"{cache_key}.json"
                    try:
                        with open(cache_path, "w", encoding="utf-8") as f:
                            json.dump(best_result.to_dict(), f, ensure_ascii=False, indent=2)
                    except IOError:
                        pass
                return best_result

        except Exception as e:
            logger.error(f"Error fetching 3DBAG by location: {e}")

        return None


def create_driedbag_collector(cache_dir: Optional[Path] = None) -> DrieDBagCollector:
    """
    Factory function to create a 3DBAG collector with default cache directory.

    Parameters
    ----------
    cache_dir : Path, optional
        Cache directory. If None, uses data/cache/3dbag.

    Returns
    -------
    DrieDBagCollector
        Configured collector instance
    """
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "3dbag"

    return DrieDBagCollector(cache_dir=cache_dir)
