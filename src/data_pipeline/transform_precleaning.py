from pathlib import Path
from typing import Union, Optional, Dict, Any

import geopandas as gpd
import numpy as np
import pandas as pd

from yaml_pipeline.action_registry import register_action

@register_action("convert_crs")
def convert_crs(
        src: gpd.GeoDataFrame, 
        to_crs: str
) -> Optional[gpd.GeoDataFrame]:
    """
    Convert GeoDataFrame to the specified CRS.

    Parameters:
    -----------
    src: gpd.GeoDataFrame
        Input GeoDataFrame to convert.
    to_crs: str
        Target CRS expected to convert in EPSG format, e.g., "EPSG:3826".

    Returns:
    --------
    gpd.GeoDataFrame or None
        A new GeoDataFrame sucessfully convert to the target CRS, or None if conversion fails.
    """
    try:
        # If already in target CRS, return unchanged
        if src.crs == to_crs or src.crs.to_string() == to_crs:
            print(f"[INFO] CRS already in {to_crs}")
            return src
        
        # Perform conversion
        converted = src.to_crs(to_crs)
        print(f"[SUCCESS] Converted CRS to {to_crs}")
        return converted
    
    except Exception as e:
        print(f"[ERROR] Failed to convert CRS: {e}")
        return None

@register_action("rename_columns")
def rename_columns(
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        parameters: Dict[str, Any]
) -> Optional[Union[pd.DataFrame, gpd.GeoDataFrame]]:
    """
    Rename columns of a DataFrame or GeoDataFrame based on mappings in YAML.

    Parameters:
    -----------
    df: Union[pd.DataFrame, gpd.GeoDataFrame]
        Input DataFrame/GeoDataFrame whose columns need renaming.
    parameters: dict
        Parameters from YAML. Expected key: 'mappings' (dict of old:new column names).

    Returns:
    --------
    Optional[Union[pd.DataFrame, gpd.GeoDataFrame]]
        DataFrame or GeoDataFrame with renamed columns, or None if no mappings are provided.
    """
    mappings = parameters.get('mappings')
    if not mappings:
        return None
    
    return df.rename(columns=mappings, inplace=True)
