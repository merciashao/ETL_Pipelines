from typing import Union, Optional, Dict, Any

import geopandas as gpd
import numpy as np
import pandas as pd

from yaml_pipeline.action_registry import register_action


@register_action("convert_crs")
def convert_crs(
        gdf: gpd.GeoDataFrame, 
        parameters: dict
) -> Optional[gpd.GeoDataFrame]:
    """
    Convert GeoDataFrame to the specified CRS.

    Parameters
    ----------
    gdf: gpd.GeoDataFrame
        Input GeoDataFrame to convert.
    to_crs: str
        Target CRS expected to convert in EPSG format, e.g., "EPSG:3826".

    Returns
    -------
    gpd.GeoDataFrame or None
        A new GeoDataFrame sucessfully convert to the target CRS, or None if conversion fails.
    """

    try:
        to_crs = parameters.get("to_crs")
        if not to_crs:
            print("[WARN] No target CRS specified")
            return gdf
        
        # If already in target CRS, return unchanged
        if gdf.crs == to_crs or gdf.crs.to_string() == to_crs:
            print(f"[INFO] CRS already in {to_crs}")
            return gdf
        
        # Perform conversion
        converted = gdf.to_crs(to_crs)
        print(f"[SUCCESS] Converted CRS to {to_crs}")
        return converted
    
    except Exception as e:
        print(f"[ERROR] Failed to convert CRS: {e}")
        return None

@register_action("rename_columns")
def rename_columns(
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        parameters: Optional[dict[str,Any]]
) -> Optional[Union[pd.DataFrame, gpd.GeoDataFrame]]:
    """
    Rename columns of a DataFrame or GeoDataFrame based on mappings in YAML.

    Parameters
    ----------
    df: Union[pd.DataFrame, gpd.GeoDataFrame]
        Input DataFrame/GeoDataFrame whose columns need renaming.
    parameters: dict[str,Any]
        - mapping: dict
            To standardized field names for ensuring consistency with the expected schema.

    Returns
    -------
    Optional[Union[pd.DataFrame, gpd.GeoDataFrame]]
        DataFrame or GeoDataFrame with renamed columns, or None if no mappings are provided.
    """

    parameters = parameters or {}
    mappings = parameters.get('mappings', {})
    if not mappings:
        return None
    
    return df.rename(columns=mappings)

@register_action("strip_whitespace")
def strip_whitespace(
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        parameters: dict
) -> Optional[Union[pd.DataFrame, gpd.GeoDataFrame]]:
    """
    Remove leading/trailing whitespace and newline (/r/n) from string columns.

    Parameters:
    -----------
    df: pd.DataFrame | gpd.GeoDataFrame
        The input DataFrame to clean.
    parameters: dict
        - dtype: str, default: 'object'
        - remove_newlines: bool, default: 'True'

    Returns:
    --------
    pd.DataFrame | gpd.GeoDataFrame
        DataFrame with cleaned string columns.
    """

    dtype = parameters.get('dtype', 'object')
    remove_newlines = parameters.get("remove_newlines", True)

    def _clean_series(series: pd.Series) -> pd.Series:
        if str(series.dtype) == dtype:
            # First strip whitespace
            series = series.str.strip()
            if remove_newlines:
                series = series.str.replace(r"[\r\n]+", " ", regex=True)
        return series
    
    return df.apply(_clean_series)

@register_action("typo_mapping")
def typo_mapping(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    parameters: dict
) -> Optional[Union[pd.DataFrame, gpd.GeoDataFrame]]:
    """
    Correct common typos in specific columns, using exact matches or regex replacements
    defined in the YAML config.

    Parameters
    ----------
    df: pd.DataFrame | gpd.GeoDataFrame
        The input DataFrame to clean.
    parameters: dict
        - exact: dict of {old_value: new_value}
        - regex: dict of {pattern: replacement}
        - backup: bool
            If True, create a backup column with '_raw' suffix.
    
    Returns
    -------
    pd.DataFrame | gpd.GeoDataFrame
        DataFrame with corrected values.
    """

    for col, rules in parameters.items():
        if col not in df.columns:
            continue
    
    # Backup column if requested
        if rules.get("backup", False):
            df[f"{col}_raw"] = df[col]
    
    # Apply exact mappings
        if "exact" in rules:
            df[col] = df[col].replace(rules["exact"])  # for exact, whole-value matches.

    # Apply regex mappings
        if "regex" in rules:
            for pattern, repl in rules["regex"].items():
                df[col] = df[col].str.replace(pattern, repl, regex=True)  # for substring replacements.

    return df

@register_action("convert_datetime")
def convert_datetime(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    parameters: dict
) -> Optional[Union[pd.DataFrame, gpd.GeoDataFrame]]:
    """
    Convert calendar dates with local year offsets into Gregorian dates and cast as datetime.

    Parameters
    ----------
    df: pd.DataFrame | gpd.GeoDataFrame
        Input DataFrame to be cleaned.
    parameters: dict
        - columns: list[str]
        - backup: bool
        - regex: str, default: '^(\d{3})'
        - shift: int, default: 1911 for Minguo → Gregorian
        - format: str, default: '%Y/%m/%d'
        - cast_to: str | None, default: overwrite original
        - errors: {'raise', 'coerce', 'ignore'}, default: 'coerce'
    
    Returns
    -------
    pd.DataFrame | gpd.GeoDataFrame
        DataFrame with converted datetime columns.
    """

    columns = parameters.get('columns', [])
    backup = parameters.get('backup', False)
    regex_pattern = parameters.get('regex', r"^(\d{3})")
    year_shift = parameters.get('shift', 1911)
    date_format = parameters.get('format', '%Y/%m/%d')
    cast_to = parameters.get('cast_to', None)
    errors = parameters.get('errors', 'coerce')

    for col in columns:
        if col not in df.columns:
            continue

        # Backup column if requested
        if backup:
            df[f"{col}_raw"] = df[col]

        # Replace the local year with Gregorian year
        df[col] = (
            df[col].astype(str)
            .str.replace(
                regex_pattern,
                lambda m: str(int(m.group(1)) + year_shift),
                regext=True
            )
        )

        # Convert to datetime
        target_col = cast_to if cast_to else col
        df[target_col] = pd.to_datetime(df[col], format=date_format, errors=errors)

    return df


@register_action("dropna")
def dropna(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    parameters: dict
) -> Optional[Union[pd.DataFrame, gpd.GeoDataFrame]]:
    """
    Drop rows where all values are null.

    Parameters
    ----------
    df: pd.DataFrame | gpd.GeoDataFrame
        The input DataFrame to clean.
    parameters: dict
        - how: {'any', 'all'}, default: 'any'
            Determine if row is removed when having at least one NA ('any') or all NA ('all').
        - subset: list[str] | None
            Labels along the row axis to consider. If None, apply to all columns.

    Returns
    -------
    pd.DataFrame | gpd.GeoDataFrame
        DataFrame with rows dropped according to the null condition.
    """

    how = parameters.get('how', 'any')
    subset = parameters.get('subset', None)

    # Handle YAML 'null' keyword → Python None
    if subset in ('null', None):
        subset = None
    
    df = df.dropna(how=how, subset=subset)
    return df

@register_action("exclude_pair_rows")
def exclude_pair_rows(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    parameters: dict
) -> Optional[Union[pd.DataFrame, gpd.GeoDataFrame]]:
    """
    Exclude rows matching spcific (prj_idx, county, comm_nm) pairs.

    Parameters
    ----------
    df: pd.DataFrame | gpd.GeoDataFrame
        The input DataFrame to clean.
    parameters: dict
        - index_name: str
            Column to use as index (e.g., "prj_idx").
        - column: list[str]
            Columns to check when matching pairs.
        - not_in: list[Tuple[int, str, str]]
            List of pairs of values to exclude.

    Returns
    -------
    pd.DataFrame | gpd.GeoDataFrame
        DataFrame with rows dropped according to the prj_idx and row pairs.
    """

    index_name = parameters.get("index_name", 'prj_idx')
    columns = parameters.get("columns", [])
    pairs_to_exclude = parameters.get("not_in", [])

    # Assign the intended name to the DataFrame’s index
    if index_name in df.columns:
        df = df.set_index(index_name, drop=False)

    # Build the condion for exclusion
    mask = pd.Series(False, index=df.index)

    for pair in pairs_to_exclude:
        condition = True  # all rows considered valid
        for col, value in zip(columns, pair):
            condition &= (df[col] == value)
        mask |= condition
    
    # Exclude rows
    df = df[~mask]

    return df