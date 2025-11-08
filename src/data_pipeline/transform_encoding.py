import logging
import os
from pathlib import Path
from typing import Any, Optional, Union

import chardet
import geopandas as gpd


logger = logging.getLogger(__name__)

# Identify and fix ecoding issues
def check_encoding(src_file: Union[str, Path]) -> Optional[dict[str, Any]]:
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

    try:
        with open(dbf_path, 'rb') as f:
            raw = f.read(100_000)  # sample first 100 KB
        detection = chardet.detect(raw)
        enc = detection.get("encoding")
    
        if not enc:
            logger.warning(f"Encoding detection returned no result for: {dbf_path.name}")
            return None
        
        logger.info(
            "Detected DBF encoding %s (confidence=%.2f) for %s",
            enc,
            detection.get("confidence", 0.0),
            dbf_path.name,
        )
        return detection

    except FileNotFoundError as e:
        logger.error("DBF file not found or inaccessible: %s", e)
        return None
    except Exception as e:
        logger.exception("Unexpected error while detecting encoding: %s", e)
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
    shp_path = Path(src_file).expanduser()
    output_dir = Path(output_dir).expanduser()
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        detection = check_encoding(shp_path)
        if not detection or not detection.get("encoding"):
            logger.warning(
                "Falling back to UTF-8 read (detection failed) for %s", shp_path.name
            )
            src_encoding = "utf-8"
        else:
            src_encoding = detection["encoding"]
        
        # Read using the source encoding
        gdf = gpd.read_file(shp_path, encoding=src_encoding) 

        # Write out as new shapefile with target encoding
        output_path = output_dir / f"{shp_path.stem}_FIX.shp"  
        gdf.to_file(output_path, encoding=dst_encoding)

        # Ensure .cpg exists with the declared target encoding
        cpg_path = output_path.with_suffix('.cpg')
        with open(cpg_path, 'w', encoding=dst_encoding) as f:
            f.write(dst_encoding)
    
        logger.info("✅ Converted DBF encoding to %s → %s", dst_encoding, output_path)
        return output_path
    
    except UnicodeError as e:
        logger.error("Encoding error while converting %s: %s", shp_path.name, e)
        return None
    except FileNotFoundError as e:
        logger.error("Source shapefile not found: %s", e)
        return None
    except Exception as e:
        logger.exception("❌ Failed to convert shapefile: %s", e)
        return None