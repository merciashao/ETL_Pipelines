import pandas as pd
import geopandas as gpd
import os
from pathlib import Path
import chardet
from ETL_Pipelines.etl.transform_encoding import check_encoding


# Load the data
def geopandas_loadfile(path, encoding=None):
    path = Path(path).expanduser()

    # Read a file with specified or detected encoding
    try:
        if encoding is None:
            detection = check_encoding(path)
            encoding = detection['encoding']
            confidence = detection['confidence']
        gdf = gpd.read_file(path, encoding=encoding)
        print(
            f"File has been successfully read with encoding {encoding} "
            f"with confidence: {confidence:.2f}."
        )
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