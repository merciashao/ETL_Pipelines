import os
from pathlib import Path
import tarfile
from tqdm import tqdm
from typing import Union, Optional
from urllib.parse import urlparse
import zipfile

import requests
import rarfile


def download_file(
        *,
        url: str,
        folder: Union[Path, str],
        filename: str = None
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
        # Start downloading
        response = requests.get(url, stream=True, timeout=15)

        # Check for successful response (status code 200)
        response.raise_for_status()
        
        total_size = int(response.headers.get('Content-Length', 0))
        
        chunk_size = 8192
    
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

        print(f"File successfully downloaded to: {file_path}")
        return file_path

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")

    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error: {timeout_err}")

    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")

def extract_archive(
        arch_path: Union[str, Path],
        extract_to: Optional[Union[str, Path]] = None
) -> None:
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
    if extract_to is None:
        extract_to = Path(arch_path).with_suffix('')  # the base path without the extension
    
    os.makedirs(extract_to, exist_ok=True)

    if zipfile.is_zipfile(arch_path):
        with zipfile.ZipFile(arch_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"ZIP file extracted to: {extract_to}")
        return extract_to

    elif tarfile.is_tarfile(arch_path):
        with tarfile.open(arch_path, 'r:*') as tar_ref:  # try all known supported compressions
            tar_ref.extractall(extract_to)
        print(f"TAR file extracted to: {extract_to}")
        return extract_to
    
    elif arch_path.endswith('.rar'):
        with rarfile.RarFile(arch_path) as rar_ref:
            rar_ref.extractall(extract_to)
        print(f"RAR file extracted to: {extract_to}")
        return extract_to
    
    else:
        raise ValueError(f"Unsupported archive format for file: {arch_path}")