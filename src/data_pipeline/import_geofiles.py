from pathlib import Path
from typing import Union, Optional

import geopandas as gpd
import pandas as pd

from data_pipeline.transform_encoding import check_encoding


# Imort data using geopandas
def geopandas_loadfile(
        path: Union[str,Path],
        encoding: Optional[str] = None,
        **kwargs
) -> Optional[gpd.GeoDataFrame]:
    """
    Load a shapefile or geospatial dataset using Geopanas with correct encoding.

    Parameters:
    -----------
    path: str
        Path to the geospatial file.
    encoding: Optional[str]
        Encoding to use when reading the file.
    **kwargs: dict
        Additional keyword arguments passed to pandas' read functions.

    Returns:
    --------
    Optional[gpd.GeoDataFrame]
        The loaded GeoDataFrame, or None if reading failed.
    """
    path = Path(path).expanduser()

    # Read a file with specified or detected encoding
    try:
        if encoding is None:
            detection = check_encoding(path)
            encoding = detection['encoding']
            confidence = detection['confidence']
            print(
                f"The file was successfully read using {encoding} "
                f"(confidence {confidence:.2f})."
            )
        else:
            print(f"The file was read using user-defined encoding '{encoding}'.")
    
        gdf = gpd.read_file(path, encoding=encoding, **kwargs)
        return gdf
    
    except FileNotFoundError as e:
        print(f"File not found or cannot be accessed: {e}")
        return None
    
    except UnicodeDecodeError as e:
        print(f"Encoding error while reading the file: {e}")
        return None
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

# Import data using pandas
def pandas_loadfile(
        path: Union[str,Path],
        encoding: Optional[str] = None,
        **kwargs
) -> Optional[pd.DataFrame]:
    """
    Load a CSV, TXT, EXCEL, JSON file using pandas, with optional encoding and others.

    Parameters:
    -----------
    path: str or Path
        Path to the input file.
    encoding: Optional[str]
        Encoding to use. If None, pandas will use the default.
    **kwargs: dict
        Additional keyword arguments passed to pandas' read functions.    
    
    Returns:
    --------
    Optional[pd.DataFrame]
        A pandas DataFrame, or None if loading fails.
    """
    path = Path(path).expanduser()
    suffix = path.suffix.lower()
    df = None

    if not path.exists():
        print(f"File does not exist: {path}")
        return None
    
    if encoding is None:
        encoding = 'utf-8'

    try:
        if suffix in [".csv", ".txt"]:
            df = pd.read_csv(path, encoding=encoding, **kwargs)
        elif suffix in [".xls", ".xlsx"]:
            df = pd.read_excel(path, encoding=encoding, **kwargs)
        elif suffix == '.json':
            df = pd.read_json(path, encoding=encoding, **kwargs)
        else:
            print("Unsupported file type: {suffix}")
            return None
        
        print(f"File '{path.name}' has been imported as DataFrame with shape {df.shape}")
        return df
    
    except Exception as e:
        print(f"Failed to read file '{path.name}': {e}")
        return None