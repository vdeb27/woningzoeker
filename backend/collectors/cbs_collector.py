"""
CBS Open Data collector for neighborhood statistics.

Downloads data from CBS StatLine via direct HTTP requests.
Target region: Den Haag, Leidschendam-Voorburg, Rijswijk.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import yaml


# CBS OData API base URL
CBS_API_BASE = "https://opendata.cbs.nl/ODataApi/odata"

# Default dataset: Kerncijfers wijken en buurten 2024
DEFAULT_DATASET = "85618NED"

# Target municipalities for Woningzoeker
TARGET_MUNICIPALITIES = {
    "0518": "Den Haag",
    "1916": "Leidschendam-Voorburg",
    "0603": "Rijswijk",
}

# Map logical column roles to CBS column names (handles 2023/2024 variations)
COLUMN_ALIASES = {
    "code": ["RegioS", "Codering_3"],
    "name": ["WijkenEnBuurten", "RegioS", "Wijken"],
    "type": ["SoortRegio_2"],
    "municipality_code": ["Gemeentecode_1", "Gemeentecode_4"],
    "municipality_name": ["Gemeentenaam_1"],
    "province_code": ["Provinciecode_1", "Provinciecode_2"],
    "province_name": ["Provincienaam_1"],
}

CORE_ROLES = [
    "code",
    "name",
    "type",
    "municipality_code",
    "municipality_name",
    "province_code",
    "province_name",
]


def download_cbs_dataset(
    dataset_id: str = DEFAULT_DATASET,
    municipality_codes: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Download a CBS Open Data dataset as a DataFrame.

    Uses the CBS OData API directly via HTTP requests.
    Applies server-side filtering for municipality codes to avoid large downloads.
    """
    base_url = f"{CBS_API_BASE}/{dataset_id}/TypedDataSet"
    records = []

    # Build OData filter for municipalities
    params = {}
    if municipality_codes:
        # CBS uses different column names, try common ones
        # Filter format: $filter=substring(Gemeentecode_4,2,4) eq '0518' or ...
        code_filters = []
        for code in municipality_codes:
            # Try different filter patterns that CBS uses
            code_filters.append(f"startswith(Codering_3,'GM{code}')")
            code_filters.append(f"startswith(Codering_3,'WK{code}')")
            code_filters.append(f"startswith(Codering_3,'BU{code}')")
        params["$filter"] = " or ".join(code_filters)

    url = base_url
    first_request = True

    while url:
        try:
            if first_request and params:
                response = requests.get(url, params=params, timeout=120)
                first_request = False
            else:
                response = requests.get(url, timeout=120)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:
            raise RuntimeError(
                f"Failed to download CBS dataset {dataset_id}: {exc}"
            ) from exc

        records.extend(data.get("value", []))

        # Handle pagination (OData uses @odata.nextLink)
        url = data.get("odata.nextLink") or data.get("@odata.nextLink")

    if not records:
        raise ValueError(f"No records returned for dataset {dataset_id}")

    return pd.DataFrame.from_records(records)


def get_dataset_info(dataset_id: str = DEFAULT_DATASET) -> Dict[str, Any]:
    """Get metadata about a CBS dataset."""
    url = f"{CBS_API_BASE}/{dataset_id}/TableInfos"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("value", [{}])[0]
    except requests.RequestException:
        return {}


def get_column_for_role(df: pd.DataFrame, role: str) -> Optional[str]:
    """Find the actual column name for a logical role."""
    for candidate in COLUMN_ALIASES.get(role, []):
        if candidate in df.columns:
            return candidate
    return None


def filter_for_region(
    df: pd.DataFrame,
    municipality_codes: Optional[List[str]] = None,
    include_types: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Filter a CBS dataset for the target region.

    Parameters
    ----------
    df : DataFrame
        Raw CBS dataset
    municipality_codes : list of str, optional
        Municipality codes to include. Defaults to TARGET_MUNICIPALITIES.
    include_types : list of str, optional
        Area types to include (e.g., ["Buurt", "Wijk"]). Defaults to all.

    Returns
    -------
    DataFrame
        Filtered dataset with only the target region.
    """
    if municipality_codes is None:
        municipality_codes = list(TARGET_MUNICIPALITIES.keys())

    muni_col = get_column_for_role(df, "municipality_code")
    type_col = get_column_for_role(df, "type")

    mask = pd.Series(True, index=df.index)

    if muni_col:
        # Municipality codes are stored with leading "GM" in some datasets
        codes_upper = {code.upper() for code in municipality_codes}
        codes_with_prefix = codes_upper | {f"GM{code}" for code in codes_upper}
        muni_series = df[muni_col].astype(str).str.strip().str.upper()
        mask &= muni_series.isin(codes_with_prefix) | muni_series.str.extract(r"(\d+)", expand=False).isin(codes_upper)

    if include_types and type_col:
        type_series = df[type_col].astype(str).str.lower()
        include_lower = {t.lower() for t in include_types}
        type_mask = pd.Series(False, index=df.index)
        for t in include_lower:
            type_mask |= type_series.str.contains(t, na=False)
        mask &= type_mask

    return df[mask].copy()


def get_core_columns(df: pd.DataFrame) -> List[str]:
    """Get the list of core identification columns present in the dataset."""
    columns = []
    for role in CORE_ROLES:
        col = get_column_for_role(df, role)
        if col and col not in columns:
            columns.append(col)
    return columns


def load_areas_config(path: Path) -> List[Dict[str, Any]]:
    """Load area definitions from a YAML config file."""
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    areas = data.get("areas", [])
    if isinstance(areas, list):
        return [a for a in areas if isinstance(a, dict)]
    return []


def filter_for_areas(
    df: pd.DataFrame,
    areas: List[Dict[str, Any]],
) -> pd.DataFrame:
    """
    Filter a dataset for specific areas defined in config.

    Each area dict can have:
    - code/codes: Exact region codes to match
    - name/names: Substring match on region names
    - gemeente: Municipality name filter
    - label: Label for the selection
    """
    selections = []
    code_col = get_column_for_role(df, "code")
    name_col = get_column_for_role(df, "name")
    muni_col = get_column_for_role(df, "municipality_name")

    for area in areas:
        mask = pd.Series(True, index=df.index)

        codes = area.get("code") or area.get("codes")
        if codes:
            if isinstance(codes, str):
                codes = [codes]
            codes_upper = {c.upper() for c in codes}
            if code_col:
                mask &= df[code_col].astype(str).str.upper().isin(codes_upper)

        names = area.get("name") or area.get("names")
        if names:
            if isinstance(names, str):
                names = [names]
            if name_col:
                name_mask = pd.Series(False, index=df.index)
                for n in names:
                    name_mask |= df[name_col].astype(str).str.contains(n, case=False, na=False)
                mask &= name_mask

        gemeente = area.get("gemeente")
        if gemeente and muni_col:
            mask &= df[muni_col].astype(str).str.contains(gemeente, case=False, na=False)

        subset = df[mask].copy()
        if not subset.empty:
            label = area.get("label") or area.get("naam") or str(codes or names)
            subset.insert(0, "__selection", label)
            selections.append(subset)

    if selections:
        return pd.concat(selections, ignore_index=True)
    return df.iloc[0:0]  # Empty DataFrame with same columns


def download_and_filter(
    dataset_id: str = DEFAULT_DATASET,
    municipality_codes: Optional[List[str]] = None,
    include_types: Optional[List[str]] = None,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Download a CBS dataset and filter for the target region.

    Convenience function combining download and filter steps.
    """
    df = download_cbs_dataset(dataset_id)
    df = filter_for_region(df, municipality_codes, include_types)

    if columns:
        core = get_core_columns(df)
        all_cols = core + [c for c in columns if c not in core and c in df.columns]
        df = df[all_cols]

    return df
