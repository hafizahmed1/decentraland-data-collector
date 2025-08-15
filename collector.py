import pandas as pd
import requests
from datetime import datetime
import os
import time
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# Load service account credentials from environment variable
try:
    creds_json = os.environ.get("GDRIVE_CREDENTIALS")
    with open("creds.json", "w") as f:
        f.write(creds_json)
except Exception as e:
    print(f"Error loading credentials: {e}")
    exit(1)

# Google Drive authentication
try:
    gauth = GoogleAuth()
    gauth.ServiceAuth()
    drive = GoogleDrive(gauth)
except Exception as e:
    print(f"Google Drive authentication failed: {e}")
    exit(1)

# --- Define the file naming and time logic ---
now = datetime.utcnow()
filename = f"decentraland_data_{now.strftime('%Y-%m-%d_%H')}.csv"
folder_id = "1R82h4Xt86nsOTPVFtuxgqxIF8cN9sgaS"  # REPLACE THIS WITH YOUR FOLDER'S ID

# Find the file on Google Drive or create it
try:
    file_list = drive.ListFile({
        'q': f"title='{filename}' and '{folder_id}' in parents and trashed=false"
    }).GetList()

    if file_list:
        file = file_list[0]
        file.GetContentFile(filename)
        df_existing = pd.read_csv(filename)
        print(f"✅ Found existing file: {filename}. Appending data.")
    else:
        df_existing = pd.DataFrame()
        print(f"➕ Creating new file: {filename}.")
except Exception as e:
    print(f"Error checking for existing file: {e}")
    exit(1)

# --- Decentraland Data Collection Logic (10-second interval for 1 minute) ---
collected_data = []
start_time = time.time()
fetch_interval = 10  # seconds

while (time.time() - start_time) < 60:
    try:
        response = requests.get("https://archipelago-stats.decentraland.org/comms/peers")
        if response.status_code == 200:
            data = response.json()
            peers = data.get("peers", [])
            timestamp = datetime.utcnow().isoformat()

            for peer in peers:
                collected_data.append({
                    "timestamp_crawl": timestamp,
                    "peer_id": peer.get("id"),
                    "peer_address": peer.get("address"),
                    "position_x": peer.get("position", [None, None, None])[0],
                    "position_y": peer.get("position", [None, None, None])[1],
                    "position_z": peer.get("position", [None, None, None])[2],
                    "parcel_x": peer.get("parcel", [None, None])[0],
                    "parcel_y": peer.get("parcel", [None, None])[1],
                    "last_ping": peer.get("lastPing")
                })
            print(f"[{timestamp}] ✅ Fetched {len(peers)} peers.")
        else:
            print(f"❌ Request failed: {response.status_code}")
    except Exception as e:
        print(f"⚠️ Error during fetch: {e}")

    time.sleep(fetch_interval)

# --- Combine and Upload Data ---
if collected_data:
    df_new = pd.DataFrame(collected_data)
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)

    # Save combined data to the local file
    df_combined.to_csv(filename, index=False)

    # Upload the file to Google Drive
    if 'file' in locals() and file_list:
        file.SetContentFile(filename)
        file.Upload()
        print(f"✅ Updated existing file '{filename}' on Google Drive.")
    else:
        file_drive = drive.CreateFile({"title": filename, "parents": [{"id": folder_id}]})
        
        file_drive.SetContentFile(filename)
        file_drive.Upload()
        print(f"✅ Created and uploaded new file '{filename}' to Google Drive.")
else:
    print("No data collected to upload.")
