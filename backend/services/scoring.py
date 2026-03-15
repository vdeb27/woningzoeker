"""Neighborhood scoring service."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yaml


class ScoringService:
    """
    Calculate composite scores for neighborhoods based on CBS indicators.

    Uses min-max normalization and weighted averaging.
    """

    def __init__(self, config_path: Optional[Path] = None):
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "scoring.yaml"
        self.config = self._load_config(config_path)
        self.indicators = self.config.get("indicators", {})

    def _load_config(self, path: Path) -> Dict[str, Any]:
        """Load scoring configuration from YAML."""
        if not path.exists():
            return {"indicators": {}}
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def normalize_series(
        self,
        series: pd.Series,
        higher_is_better: bool = True,
    ) -> pd.Series:
        """
        Normalize a series to 0-1 range using min-max scaling.

        Parameters
        ----------
        series : pd.Series
            Raw values to normalize
        higher_is_better : bool
            If True, higher values get higher scores.
            If False, lower values get higher scores.

        Returns
        -------
        pd.Series
            Normalized values between 0 and 1
        """
        values = series.astype(float)
        mask = values.notna()

        if mask.sum() == 0:
            return pd.Series(pd.NA, index=series.index)

        min_val = values[mask].min()
        max_val = values[mask].max()

        if min_val == max_val:
            norm = pd.Series(0.5, index=series.index)
        else:
            norm = (values - min_val) / (max_val - min_val)
            if not higher_is_better:
                norm = 1 - norm

        norm[~mask] = pd.NA
        return norm

    def calculate_indicator(
        self,
        df: pd.DataFrame,
        indicator_id: str,
        spec: Dict[str, Any],
    ) -> Tuple[pd.Series, pd.Series]:
        """
        Calculate a single indicator's raw and normalized values.

        Returns
        -------
        tuple of (raw_values, normalized_values)
        """
        if "column" in spec:
            column = spec["column"]
            if column not in df.columns:
                return pd.Series(dtype=float), pd.Series(dtype=float)
            raw = df[column]
        elif "numerator" in spec and "denominator" in spec:
            num = spec["numerator"]
            denom = spec["denominator"]
            if num not in df.columns or denom not in df.columns:
                return pd.Series(dtype=float), pd.Series(dtype=float)
            raw = df[num] / df[denom].replace(0, pd.NA)
        else:
            return pd.Series(dtype=float), pd.Series(dtype=float)

        normalized = self.normalize_series(
            raw,
            higher_is_better=spec.get("higher_is_better", True),
        )

        return raw, normalized

    def calculate_scores(
        self,
        df: pd.DataFrame,
        id_column: str = "__selection",
    ) -> pd.DataFrame:
        """
        Calculate composite scores for all rows in a DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with CBS indicator columns
        id_column : str
            Column to use as identifier

        Returns
        -------
        pd.DataFrame
            Original data with added score columns
        """
        result = df.copy()
        weight_map: Dict[str, float] = {}
        total_weight = 0.0

        for indicator_id, spec in self.indicators.items():
            raw, normalized = self.calculate_indicator(df, indicator_id, spec)

            if not normalized.empty:
                result[f"{indicator_id}_raw"] = raw
                result[f"{indicator_id}_norm"] = normalized

                weight = float(spec.get("weight", 0.0))
                weight_map[indicator_id] = weight
                total_weight += weight

        # Calculate weighted score
        scores = []
        coverages = []

        for idx in range(len(result)):
            numer = 0.0
            denom = 0.0

            for indicator_id, weight in weight_map.items():
                norm_col = f"{indicator_id}_norm"
                if norm_col in result.columns:
                    value = result.iloc[idx][norm_col]
                    if pd.notna(value):
                        numer += value * weight
                        denom += weight

            if denom > 0:
                scores.append(numer / denom)
                coverages.append(denom / total_weight if total_weight else 0.0)
            else:
                scores.append(pd.NA)
                coverages.append(0.0)

        result["score"] = scores
        result["score_coverage"] = coverages

        return result.sort_values("score", ascending=False)

    def get_indicator_descriptions(self) -> Dict[str, str]:
        """Get descriptions for all indicators."""
        return {
            ind_id: spec.get("description", "")
            for ind_id, spec in self.indicators.items()
        }

    def get_weights(self) -> Dict[str, float]:
        """Get weights for all indicators."""
        return {
            ind_id: spec.get("weight", 0.0)
            for ind_id, spec in self.indicators.items()
        }
