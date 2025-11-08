import logging
import os
import tarfile
import zipfile
from pathlib import Path
from typing import Optional, Union
from tqdm import tqdm

import requests
import rarfile


logger = logging.getLogger(__name__)

def download_file(
        *,
        url: str,
        folder: Union[Path, str],
        filename: Optional[str] = None,
        timeout: int = 15,
        chunk_size: int = 8192
) -> Optional[Path]:
    """
    Download a shapefile from a given URL and save it to a specified folder.
    If no filename is provided, it uses the filename from the URL.

    Parameters:
        url: str
            URL to the `.shp` file.
        folder: str or pathlib.Path
            Destination folder where the file will be saved (supports '~').
        filename: str, optional
            File name to use. If None, it's derived from the URL.
    
    Returns:
    --------
    pathlib.Path or None
        The path to the downloaded file if successful, or None if the 
        download fails.
    """
    folder = Path(folder).expanduser()  # Expand "~" to full home directory path
    os.makedirs(folder, exist_ok=True)  # Ensure folder exists
    
    if filename is None:
        filename = url.split('/')[-1]

    file_path = folder / filename

    try:
        logger.info(f"Starting download: {url}")
        # Start downloading
        response = requests.get(url, stream=True, timeout=timeout)
        # Check for successful response (status code 200)
        response.raise_for_status()
        
        total_size = int(response.headers.get('Content-Length', 0))
    
        # Save a file and set up progress bar
        with open(file_path, "wb") as f, tqdm(
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            desc=f"Downloading {filename}"
        ) as progress:
            for chunk in response.iter_content(chunk_size=chunk_size):  # Stream the response data in small chunks (8kb)
                if chunk:  # filter out keep-alive chunks
                    f.write(chunk)
                    progress.update(len(chunk))

        logger.info(f"✅ File downloaded successfully: {file_path}")
        return file_path

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error: {e}")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error: {e}")
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout after {timeout}s: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error downloading file: {e}")
    
    return None


def extract_archive(
        arch_path: Union[str, Path],
        extract_to: Optional[Union[str, Path]] = None
) -> Optional[Path]:
    """
    Extract an archive file to a target folder.
    Support various archive formats including `.zip`, `.tar.gz`, `.tar`, `.rar`.

    Parameters:
    -----------
    arch_path: str or pathlib.Path
        Path to the archive file.
    extract_to: str or pathlib.Path, optional
        Destination folder where contents will be extracted. 
        If None, extraction occurs in the same directory as the archive.

    Return:
    -------
    None
        This function does not return anything. Files are extracted to disk.
    """
    arch_path = Path(arch_path)
    if extract_to is None:
        extract_to = arch_path.parent / arch_path.stem  # the base path without the extension
    
    os.makedirs(extract_to, exist_ok=True)

    try:
        if zipfile.is_zipfile(arch_path):
            with zipfile.ZipFile(arch_path, 'r') as zf:
                zf.extractall(extract_to)
            logger.info(f"✅ Extracted ZIP file to: {extract_to}")

        elif tarfile.is_tarfile(arch_path):
            with tarfile.open(arch_path, 'r:*') as tf:  # try all known supported compressions
                tf.extractall(extract_to)
            logger.info(f"✅ Extracted TAR file to: {extract_to}")

        elif str(arch_path).endswith('.rar'):
            with rarfile.RarFile(arch_path) as rf:
                rf.extractall(extract_to)
            logger.info(f"Extracted RAR file to: {extract_to}")
    
        else:
            raise ValueError(f"Unsupported archive format: {arch_path}")
        
        return extract_to
    
    except Exception as e:
        logger.exception(f"❌ Failed to extract {arch_path}: {e}")
        return None