import os
from pathlib import Path
from typing import Union, Optional

import chardet
import geopandas as gpd
import pandas as pd


# Identify and fix ecoding issues
def check_encoding(src_file: Union[str, Path]) -> Optional[dict]:
    """
    Detect the encoding of .dbf file associated with a shapefile.

    Parameters:
    -----------
    src_file: str or pathlib.Path
        Path to the shapefile. The function will automatically look for 
        the corresponding `.dbf` file.

    Returns:
    --------
    dict or None
        A dictionary containing the detected encoding information with keys 
        like `encoding` and `confidence`, or None if detection fails.
    """
    path = Path(src_file).expanduser()
    dbf_path = path.with_suffix('.dbf')
    confidence = None

    try:
        with open(dbf_path, 'rb') as f:
            raw = f.read(100_000)  # read 100,000 bytes from the file
            detection = chardet.detect(raw)
            return detection
        
    except FileNotFoundError as e:
        print(f"File not found or cannot be accessed: {e}")
        return None
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
    
# Convert encoding to 'utf-8'
def convert_encoding(
        src_file: Union[str, Path],
        *,
        output_dir: Union[str, Path],
        dst_encoding: str = 'utf-8'
) -> Optional[Path]:
    """
    Convert the DBF encoding of a shapefile to a specified encoding (default UTF-8)
    and write a new `.cpg` file.
    
    Parameters:
    -----------
    src_file: str or pathlib.Path
        Path to the input shapefile (.shp). The function automatically 
        handles the associated DBF file.
    output_dir: str or pathlib.Path
        Destination folder where the re-encoded shapefile will be saved.
    dst_encoding: str, optional
        Target encoding for the output shapefile. Default is `'utf-8'`.
    
    Returns:
    --------
    pathlib.Path or None
        Path to the re-encoded shapefile if successful, or None if the 
        conversion fails.
    """
    try:
        shp_path = Path(src_file).expanduser()
        output_dir = Path(output_dir).expanduser()
        os.makedirs(output_dir, exist_ok=True)
        output_path = output_dir / (shp_path.stem + "_FIX.shp")
        
        src_encoding = check_encoding(src_file)['encoding']
        gdf = gpd.read_file(shp_path, encoding=src_encoding)    
        gdf.to_file(output_path, encoding=dst_encoding)    
        cpg_path = output_path.with_suffix('.cpg')

        with open(cpg_path, 'w', encoding=dst_encoding) as f:
            f.write(dst_encoding)
    
        print(f"[SUCCESS] Converted DBF encoding to {dst_encoding}")
        return output_path
    
    except Exception as e:
        print(f"[ERROR] Failed to convert shapefile: {e}")
        return None