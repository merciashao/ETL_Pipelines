import pandas as pd
import geopandas as gpd
import chardet
import os
from pathlib import Path
import chardet


# Identify and fix ecoding issues
def check_encoding(src_file):
    path = Path(src_file).expanduser()
    dbf_path = path.with_suffix('.dbf')
    confidence = None

    try:
        with open(dbf_path, 'rb') as f:
            raw = f.read(100_000)
            detection = chardet.detect(raw)
            encoding = detection['encoding']
            confidence = detection['confidence']
            return detection

    except FileNotFoundError as e:
        print(f"File not found or cannot be accessed: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
    
# Convert encoding to 'utf-8'
def convert_encoding(*, src_file, output_dir, dst_encoding='utf-8'):
    """
    Convert the DBF encoding of a shapefile to UTF-8 and write a new .cpg file.
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