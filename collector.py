import pandas as pd
import requests
from datetime import datetime
import os
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
    gauth.LoadServiceConfigFile("creds.json")
    gauth.ServiceAuth()
    drive = GoogleDrive(gauth)
except Exception as e:
    print(f"Google Drive authentication failed: {e}")
    exit(1)

# --- Decentraland Data Collection Logic ---
BASE_URL = "https://archipelago-stats.decentraland.org"
ENDPOINT = "/comms/peers"
collected_data = []

try:
    response = requests.get(BASE_URL + ENDPOINT)
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
        print(f"Fetched {len(peers)} peers.")
    else:
        print(f"Request failed: {response.status_code}")
except Exception as e:
    print(f"Error during fetch: {e}")

# --- Save to CSV and Upload to Google Drive ---
if collected_data:
    df = pd.DataFrame(collected_data)
    filename = f"decentraland_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)

    folder_id = "1R82h4Xt86nsOTPVFtuxgqxIF8cN9sgaS"  
    file_drive = drive.CreateFile({"title": filename, "parents": [{"id": folder_id}]})
    file_drive.SetContentFile(filename)
    file_drive.Upload()

    print(f"Uploaded {filename} to Google Drive")
