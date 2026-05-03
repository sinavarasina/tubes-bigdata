#!/usr/bin/env python3
import os
import urllib.request
import zipfile
import pandas as pd

# Directory setup
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

# URL and paths
ZIP_URL = "https://archive.ics.uci.edu/static/public/697/predict+students+dropout+and+academic+success.zip"
ZIP_PATH = os.path.join(DATA_DIR, "dataset.zip")
RAW_CSV_PATH = os.path.join(DATA_DIR, "data.csv")
CLEAN_CSV_PATH = os.path.join(DATA_DIR, "dataset_clean.csv")


def download_and_prepare():
    if os.path.exists(CLEAN_CSV_PATH):
        print(f"[INFO] Dataset is already prepared at: {CLEAN_CSV_PATH}")
        return

    print("==================================================")
    print("DOWNLOADING DATASET FROM UCI ML REPOSITORY")
    print("==================================================")
    print(f"[1/4] Downloading zip file from {ZIP_URL}...")
    try:
        urllib.request.urlretrieve(ZIP_URL, ZIP_PATH)
    except Exception as e:
        print(f"[ERROR] Failed to download: {e}")
        return

    print("[2/4] Extracting zip file...")
    try:
        with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
            zip_ref.extractall(DATA_DIR)
    except Exception as e:
        print(f"[ERROR] Failed to extract: {e}")
        return

    if not os.path.exists(RAW_CSV_PATH):
        print(f"[ERROR] File 'data.csv' not found after extraction!")
        return

    print("[3/4] Standardizing CSV format (converting ';' to ',')...")
    try:
        df = pd.read_csv(RAW_CSV_PATH, sep=';')
        df.to_csv(CLEAN_CSV_PATH, index=False)
    except Exception as e:
        print(f"[ERROR] Failed to process CSV: {e}")
        return

    print("[4/4] Cleaning temporary files...")
    os.remove(ZIP_PATH)
    os.remove(RAW_CSV_PATH)

    print("==================================================")
    print(f"[SUCCESS] Dataset ready: {CLEAN_CSV_PATH}")
    print(f"          Total Records: {len(df):,}")
    print(f"          Total Columns: {len(df.columns)}")
    print("==================================================")


if __name__ == "__main__":
    download_and_prepare()
