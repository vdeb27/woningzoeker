"""
BGT boom collector with AHN height data.

Fetches tree positions from the BGT (Basisregistratie Grootschalige
Topografie) and determines tree heights using the AHN (Actueel
Hoogtebestand Nederland) DSM/DTM difference.
"""

from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

BGT_URL = "https://api.pdok.nl/lv/bgt/ogc/v1/collections/vegetatieobject_punt/items"
AHN_WCS_URL = "https://service.pdok.nl/rws/ahn/wcs/v1_0"


@dataclass
class BgtBoomCollector:
    """Collector for tree positions (BGT) and heights (AHN).

    Parameters
    ----------
    min_delay : float
        Minimum delay between requests in seconds
    max_delay : float
        Maximum delay between requests in seconds
    cache_dir : Path, optional
        Directory for caching results
    cache_days : int
        Number of days to cache results (default: 365)
    """

    min_delay: float = 1.0
    max_delay: float = 2.0
    cache_dir: Optional[Path] = None
    cache_days: int = 365
    session: Optional[requests.Session] = None
    _last_request: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _rate_limit(self) -> None:
        now = time.perf_counter()
        elapsed = now - self._last_request
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request = time.perf_counter()

    def get_trees(
        self,
        rd_x: float,
        rd_y: float,
        radius: float = 75.0,
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """Fetch trees near a location with heights.

        Parameters
        ----------
        rd_x, rd_y : float
            Center point in RD coordinates (EPSG:28992)
        radius : float
            Search radius in meters (default: 75m)
        use_cache : bool
            Whether to use cached results

        Returns
        -------
        list of dict
            Each dict has: rd_x, rd_y, hoogte (meters above ground)
        """
        cache_key = f"bomen_{int(rd_x)}_{int(rd_y)}_{int(radius)}"

        if use_cache and self.cache_dir:
            cache_path = self.cache_dir / f"{cache_key}.json"
            if cache_path.exists():
                try:
                    with open(cache_path, "r", encoding="utf-8") as f:
                        cached = json.load(f)
                    cache_date = datetime.fromisoformat(cached.get("fetch_date", ""))
                    if datetime.now() - cache_date < timedelta(days=self.cache_days):
                        return cached.get("trees", [])
                except (json.JSONDecodeError, ValueError):
                    pass

        # Convert RD to WGS84 for BGT OGC API bbox
        tree_positions = self._fetch_tree_positions(rd_x, rd_y, radius)

        if not tree_positions:
            self._save_cache(cache_key, [])
            return []

        # Get heights from AHN for all tree positions
        trees = self._enrich_with_heights(tree_positions)

        self._save_cache(cache_key, trees)
        return trees

    def _fetch_tree_positions(
        self, rd_x: float, rd_y: float, radius: float
    ) -> List[Dict[str, float]]:
        """Fetch tree positions from BGT OGC API."""
        try:
            from pyproj import Transformer

            transformer = Transformer.from_crs(
                "EPSG:28992", "EPSG:4326", always_xy=True
            )
            lon_min, lat_min = transformer.transform(
                rd_x - radius, rd_y - radius
            )
            lon_max, lat_max = transformer.transform(
                rd_x + radius, rd_y + radius
            )
        except ImportError:
            logger.error("pyproj not available for coordinate transformation")
            return []

        self._rate_limit()

        try:
            params = {
                "bbox": f"{lon_min},{lat_min},{lon_max},{lat_max}",
                "limit": 200,
            }
            response = self.session.get(
                BGT_URL,
                params=params,
                headers={"User-Agent": random.choice(USER_AGENTS)},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            transformer_to_rd = Transformer.from_crs(
                "EPSG:4326", "EPSG:28992", always_xy=True
            )

            positions = []
            for feat in data.get("features", []):
                props = feat.get("properties", {})
                # Only include actual trees that still exist
                plus_type = props.get("plus_type", "")
                status = props.get("status", "")
                if plus_type != "boom" or status != "bestaand":
                    continue
                # Skip trees with an end date (removed)
                if props.get("termination_date"):
                    continue

                geom = feat.get("geometry", {})
                coords = geom.get("coordinates", [])
                if len(coords) >= 2:
                    tree_rd_x, tree_rd_y = transformer_to_rd.transform(
                        coords[0], coords[1]
                    )
                    positions.append(
                        {"rd_x": round(tree_rd_x, 1), "rd_y": round(tree_rd_y, 1)}
                    )

            logger.info(f"BGT: {len(positions)} bomen gevonden in straal {radius}m")
            return positions

        except requests.RequestException as e:
            logger.error(f"Error fetching BGT tree data: {e}")
            return []

    def _enrich_with_heights(
        self, positions: List[Dict[str, float]]
    ) -> List[Dict[str, Any]]:
        """Get tree heights by querying AHN DSM and DTM."""
        if not positions:
            return []

        # Batch: get a single AHN tile covering all trees
        all_x = [p["rd_x"] for p in positions]
        all_y = [p["rd_y"] for p in positions]
        x_min, x_max = min(all_x) - 2, max(all_x) + 2
        y_min, y_max = min(all_y) - 2, max(all_y) + 2

        dsm_values = self._fetch_ahn_raster("dsm_05m", x_min, y_min, x_max, y_max)
        dtm_values = self._fetch_ahn_raster("dtm_05m", x_min, y_min, x_max, y_max)

        if dsm_values is None or dtm_values is None:
            # Without AHN, estimate tree height as 8m (typical Dutch street tree)
            logger.warning("AHN niet beschikbaar, schatting boomhoogte 8m")
            return [
                {"rd_x": p["rd_x"], "rd_y": p["rd_y"], "hoogte": 8.0}
                for p in positions
            ]

        trees = []
        ds_transform, ds_data = dsm_values
        dt_transform, dt_data = dtm_values

        for pos in positions:
            dsm_h = self._sample_raster(ds_transform, ds_data, pos["rd_x"], pos["rd_y"])
            dtm_h = self._sample_raster(dt_transform, dt_data, pos["rd_x"], pos["rd_y"])

            if dsm_h is not None and dtm_h is not None:
                height = dsm_h - dtm_h
                if height > 1.0:  # Minimum tree height
                    trees.append(
                        {
                            "rd_x": pos["rd_x"],
                            "rd_y": pos["rd_y"],
                            "hoogte": round(height, 1),
                        }
                    )
            elif dsm_h is not None:
                # DTM nodata (under building?), estimate ground from nearby
                trees.append(
                    {
                        "rd_x": pos["rd_x"],
                        "rd_y": pos["rd_y"],
                        "hoogte": 8.0,  # fallback estimate
                    }
                )

        logger.info(f"AHN: {len(trees)} bomen met hoogte bepaald")
        return trees

    def _fetch_ahn_raster(
        self, coverage: str, x_min: float, y_min: float, x_max: float, y_max: float
    ):
        """Fetch AHN raster tile via WCS."""
        try:
            import numpy as np
            import rasterio
        except ImportError:
            logger.error("rasterio/numpy not available for AHN parsing")
            return None

        self._rate_limit()

        try:
            url = (
                f"{AHN_WCS_URL}?service=WCS&version=2.0.1"
                f"&request=GetCoverage&CoverageId={coverage}"
                f"&format=image/tiff"
                f"&subset=x({x_min},{x_max})"
                f"&subset=y({y_min},{y_max})"
            )
            response = self.session.get(
                url,
                headers={"User-Agent": random.choice(USER_AGENTS)},
                timeout=30,
            )
            response.raise_for_status()

            if "tiff" not in response.headers.get("content-type", ""):
                return None

            with rasterio.open(BytesIO(response.content)) as ds:
                data = ds.read(1)
                # Replace nodata with NaN
                nodata_mask = data > 1e10
                data = data.astype(np.float64)
                data[nodata_mask] = np.nan
                return (ds.transform, data)

        except Exception as e:
            logger.error(f"Error fetching AHN {coverage}: {e}")
            return None

    @staticmethod
    def _sample_raster(transform, data, x: float, y: float) -> Optional[float]:
        """Sample a raster value at a given RD coordinate."""
        import numpy as np

        try:
            import rasterio

            row, col = rasterio.transform.rowcol(transform, x, y)
            if 0 <= row < data.shape[0] and 0 <= col < data.shape[1]:
                val = data[row, col]
                if not np.isnan(val):
                    return float(val)
        except Exception:
            pass
        return None

    def _save_cache(self, cache_key: str, trees: List[Dict[str, Any]]) -> None:
        if not self.cache_dir:
            return
        cache_path = self.cache_dir / f"{cache_key}.json"
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(
                    {"fetch_date": datetime.now().isoformat(), "trees": trees},
                    f,
                    ensure_ascii=False,
                )
        except IOError:
            pass


def create_bgt_boom_collector(
    cache_dir: Optional[Path] = None,
) -> BgtBoomCollector:
    """Factory function to create a BGT boom collector."""
    if cache_dir is None:
        project_root = Path(__file__).parent.parent.parent
        cache_dir = project_root / "data" / "cache" / "bgt_bomen"
    return BgtBoomCollector(cache_dir=cache_dir)
