import logging
from pathlib import Path
from typing import Union, Optional

import geopandas as gpd
import pandas as pd

from data_pipeline.transform_encoding import check_encoding


logger = logging.getLogger(__name__)

# Import data using geopandas
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
        if not path.exists():
            logger.error(f"File not found: {path}")
            return None
        
        if encoding is None:
            detection = check_encoding(path)
            encoding = detection['encoding']
            confidence = detection.get('confidence', 0)
            logger.info(
                f"Detected encoding: {encoding} (confidence={confidence:.2f})"
            )
        else:
            logger.info(f"Using user-defined encoding: {encoding}")
    
        gdf = gpd.read_file(path, encoding=encoding, **kwargs)
        logger.info(f"✅ GeoDataFrame loaded successfully with shape {gdf.shape}")
        return gdf
     
    except UnicodeDecodeError as e:
        logger.error(f"Encoding error while reading '{path.name}': {e}")

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")

    except Exception as e:
        logger.exception(f"Unexpected error while reading '{path.name}': {e}")

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

    if not path.exists():
        logger.error(f"File does not exist: {path}")
        return None
    
    encoding = encoding or "utf-8"
    suffix = path.suffix.lower()

    try:
        if suffix in [".csv", ".txt"]:
            df = pd.read_csv(path, encoding=encoding, **kwargs)
        elif suffix in [".xls", ".xlsx"]:
            df = pd.read_excel(path, **kwargs)
        elif suffix == '.json':
            df = pd.read_json(path, **kwargs)
        else:
            logger.warning("Unsupported file type: {suffix}")
            return None
        
        logger.info(f"File '{path.name}' loaded as DataFrame with shape {df.shape}")
        return df
    
    except Exception as e:
        logger.exception(f"Failed to read file '{path.name}': {e}")
        return None