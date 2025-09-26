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
        - mode: list[str]
            Which rule types to apply ["exact", "regex"].
        - column: dict
            - exact: dict of {old_value: new_value}
            - regex: dict of {pattern: replacement}
            - backup: bool
                If True, create a backup column with '_raw' suffix.
    
    Returns
    -------
    pd.DataFrame | gpd.GeoDataFrame
        DataFrame with corrected values.
    """

    def _apply_exact(series: pd.Series, exact_map: dict) -> pd.Series:
        """Apply exact replacements to a column."""
        return series.replace(exact_map)

    def _apply_regex(series: pd.Series, regex_map: dict) -> pd.Series:
        """Apply regex replacements to a column."""
        for pattern, replacement in regex_map.items():
            series = series.astype(str).str.replace(
                pattern,
                str(replacement) if replacement is not None else "",
                regex=True
            )
        return series
    
    modes = parameters.get("mode", ["exact", "regex"])
    col_rules = parameters.get("columns", {})

    for col, rules in col_rules.items():
        if col not in df.columns:
            continue

        if rules.get("backup", False):
            df[f"{col}_raw"] = df[col]
        
        if "exact" in modes and "exact" in rules:
            df[col] = _apply_exact(df[col], rules["exact"])

        if "regex" in modes and "regex" in rules:
            df[col] = _apply_regex(df[col], rules["regex"])
        
        df[col] = df[col].replace({None: np.nan})
    
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
    regex_pattern = parameters.get('regex', r"^(\d{3})")
    year_shift = parameters.get('shift', 1911)
    date_format = parameters.get('format', '%Y/%m/%d')
    errors = parameters.get('errors', 'coerce')

    for col in columns:
        if col not in df.columns:
            continue

        # Replace the local year with Gregorian year
        df[col] = (
            df[col].astype(str)
            .str.replace(
                regex_pattern,
                lambda m: str(int(m.group(1)) + year_shift),
                regex=True
            )
        )

        # Convert to datetime
        df[col] = pd.to_datetime(df[col], format=date_format, errors=errors)

    return df


@register_action("droprows")
def droprows(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    parameters: dict
) -> Optional[Union[pd.DataFrame, gpd.GeoDataFrame]]:
    """
    Drop specific rows by index.

    Parameters
    ----------
    df: pd.DataFrame | gpd.GeoDataFrame
        The input DataFrame to clean.
    parameters: dict
        - index: int | list[int]
            Row index or a list of row indices to drop.

    Returns
    -------
    pd.DataFrame | gpd.GeoDataFrame
        DataFrame with the specific rows dropped.
    """

    index = parameters.get("index")

    df = df.drop(index=index, errors="ignore")
    return df

@register_action("drop_by_pairs")
def drop_by_pairs(
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
    df[index_name] = df.index

    # Build the condion for exclusion
    mask = pd.Series(False, index=df.index)

    for pair in pairs_to_exclude:
        condition = pd.Series(True, index=df.index)
        for col, value in zip(columns, pair):
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found in DataFrame")
            condition &= (df[col] == value)
        mask |= condition

    return df[~mask]

@register_action("explode_village")
def explode_village(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    parameters: dict
) -> Optional[Union[pd.DataFrame, gpd.GeoDataFrame]]:
    """
    Split a string column into multiple rows based on a delimiter, while preserving
    the original values in a duplicate column.

    Parameters
    ----------
    df: pd.DataFrame | gpd.GeoDataFrame
        Input dataframe containing the source column.
    parameters: dict
        - source_column: str
            Column to split. Default is "village_nm".
        - delimiter: str 
            Delimiter used for splitting. Default is "、".
        - keep_original_as: str 
            Name of the column to preserve original values. Default is "villg_raw".
        - sort_by: str 
            Column to sort by. Default is "prj_idx".
        - reset_index: bool
            Whether to reset the index after transformation. Default is True.
        - new_index: str
            Name of the new index column to assign after exploding rows.

    Returns
    -------
    pd.DataFrame | gpd.GeoDataFrame
        A dataframe where each split value is expanded into its own row.
    """

    # Extract parameters with defaults
    source_column = parameters.get("source_column", 'village_nm')
    delimiter = parameters.get("delimiter", '、')
    original_column = parameters.get("keep_original_as", 'villg_raw')
    sort_by = parameters.get("sort_by", 'prj_idx')
    reset_index = parameters.get("reset_index", True)
    new_index = parameters.get("new_index", 'loc_idx')

    df = (
        df
        .assign(**{original_column: lambda d: d[source_column]})
        .assign(**{source_column: lambda d: d[source_column].astype(str).str.split(delimiter)})
        .explode(source_column)
        .sort_values(by=sort_by)
    )

    if reset_index:
        df = df.reset_index(drop=True).set_index(
            pd.RangeIndex(start=0, stop=len(df), name=new_index)
        )

    return df

@register_action("split_duplicates")
def split_duplicates(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    parameters: dict
) -> dict[str, Union[pd.DataFrame, gpd.GeoDataFrame]]:
    """
    Identify duplicate and unique rows based on a subset of columns.

    Parameters
    ----------
    df: DataFrame
        Input dataframe.
    parameters: dict
        - subset: list[str]
            Columns to check duplicates.
        - output_aliases: dict
            Names for duplicate and unique outputs.
        - sort_duplicates_by: str, optional
            Column to sort duplicates.

    Returns
    -------
    
    """

    subsset = parameters.get("subset", [])
    aliases = parameters.get("output_aliases", {"duplicates": "duplicates", "unique": "unique"})
    sort_by = parameters.get("sort_duplicates_by")

    dup_mask = df.duplicated(subset=subsset, keep=False)
    dupes = df.loc[dup_mask].copy()
    unique = df.loc[~dup_mask].copy()

    if sort_by and sort_by in dupes.columns:
        dupes = dupes.sort_values(by=sort_by)
    
    return {
        aliases.get("duplicates", "duplicates"): dupes,
        aliases.get("unique", "unique"): unique
    }

@register_action("dissolve_villages")
def dissolve_villages(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    parameters: dict
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Dissolve duplicates of a target community into a single row.

    Parameters
    ----------
    df: DataFrame
        Input dataframe (usually duplicates).
    parameters: dict
        - where: str, optional
            Query filter to select target rows.
        - by: str
            Column to dissolve by.
        - aggregation_rules: dict
            Rules for aggregating columns. 
            Keys: 'sum', 'first', 'exclude_from_first', 'preserve_index_as', 'output_alias'.

    Returns
    -------

    """
    where = parameters.get("where")
    by = parameters.get("by", "village_nm")
    rules = parameters.get("aggregation_rules", {})
    output_alias = rules.get("output_alias", "dissolved")

    # Filter dataset
    df_filtered = df.query(where) if where else df.copy()

    # Preserve original index if requested
    preserve_index_as = rules.get("preserve_index_as")
    if preserve_index_as:
        df_filtered[preserve_index_as] = df_filtered.index
    
    # Build aggregation dictionary
    agg_rules = {}

    # Explicit sum rules
    for col in rules.get("sum", []):
        if col in df_filtered.columns:
            agg_rules[col] = "sum"

    # Apply 'First' to rules except excluded if ["*"]
    exclude = set(rules.get("exclude_from_first", []))
    if rules.get("first"):
        if rules["first"] == ["*"]:
            for col in df_filtered.columns:
                if col not in agg_rules and col not in exclude and col != by:
                    agg_rules[col] = "first"
        else:
            for col in rules["first"]:
                if col not in agg_rules and col in df_filtered.columns and col not in exclude:
                    agg_rules[preserve_index_as] = "first"
    
    # Dissolve/groupby
    df_dissolved = (
        df_filtered
        .dissolve(by=by, aggfunc=agg_rules, as_index=False)
        .set_index(preserve_index_as)
    )
    df_dissolved.attrs['alias'] = output_alias

    return df_dissolved

@register_action("concat_finalize")
def concat_finalize(
    inputs: list[Union[pd.DataFrame, gpd.GeoDataFrame]],
    parameters: dict
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    """

    sort_by = parameters.get("sort_by")
    reset_index = parameters.get("reset_index", True)
    index_name = parameters.get("index_name")
    ignore_index = parameters.get("ignore_index", False)
    output_alias = parameters.get("output_alias", "final")

    df = pd.concat(inputs, ignore_index=ignore_index)

    if sort_by and sort_by in df.columns:
        df = df.sort_values(by=sort_by)
    
    if reset_index:
        df = df.reset_index(drop=True)
        if index_name:
            df.index.name = index_name
    
    df.attrs["alias"] = output_alias
    return df