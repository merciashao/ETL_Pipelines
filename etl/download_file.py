import os
import requests
from tqdm import tqdm
from urllib.parse import urlparse


def download_file(url, folder, filename=None):
    """
    Download a shapefile from a given URL and save it to a specified folder.
    If no filename is provided, it uses the filename from the URL.

    Parameters:
        url (str): The URL to the .shp file.
        folder (str): Folder path where the file will be saved (supports '~').
        filename (str, optional): File name to use. If None, it's derived from the URL.
    """
    
    folder = os.path.expanduser(folder)  # Expand "~" to full home directory path
    os.makedirs(folder, exist_ok=True)  # Ensure folder exists
    file_path = os.path.join(folder, filename)

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

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")

    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error: {timeout_err}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")