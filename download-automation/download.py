import os
import yaml
import requests
from zipfile import ZipFile
from tqdm import tqdm
import sys

def download_file(url, save_path):
    try:
        print(f"Connecting to {url}...")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_size_in_bytes = int(r.headers.get('content-length', 0))
            block_size = 1024
            progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True, desc=os.path.basename(save_path))
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=block_size):
                    progress_bar.update(len(chunk))
                    f.write(chunk)
            progress_bar.close()
            if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
                print("ERROR, something went wrong during download.")
                return False
            print(f"Successfully downloaded: {save_path}")
            return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return False

def unzip_file(zip_path, extract_dir):
    try:
        with ZipFile(zip_path, 'r') as zip_ref:
            print(f"Unzipping {os.path.basename(zip_path)} to {extract_dir}...")
            zip_ref.extractall(extract_dir)
            print("Unzipping complete.")
    except Exception as e:
        print(f"Error unzipping file: {e}")

def main(config_path="config.yaml"):
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_path}' not found.")
        sys.exit(1)

    download_dir = config.get("download_dir", "data/raw")
    files_to_download = config.get("files", [])
    os.makedirs(download_dir, exist_ok=True)

    for item in files_to_download:
        name = item.get("name")
        url = item.get("url")
        if not name or not url:
            print(f"Skipping invalid entry in config: {item}. Please provide a valid name and URL.")
            continue

        zip_save_path = os.path.join(download_dir, f"{name}.zip")
        print("-" * 50)
        print(f"Processing '{name}'...")
        if download_file(url, zip_save_path):
            unzip_file(zip_save_path, download_dir)

if __name__ == "__main__":
    main()
