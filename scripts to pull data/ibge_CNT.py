import requests
import pandas as pd
from io import BytesIO
import json

# --- Configuration ---

# IBGE URL for the XLSX data
IBGE_URL = 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela1620.xlsx&terr=N&rank=-&query=t/1620/n1/all/v/all/p/all/c11255/all/d/v583%202/l/,v%2Bc11255,t%2Bp'

# Firebase Realtime Database URL
FIREBASE_URL = 'https://peixo-28d2d-default-rtdb.firebaseio.com/ibge_cnt.json'
# The data will be stored under a node named 'ibge_cnt'

# --- Data Fetching, Processing, and Upload ---

def fetch_and_upload_ibge_data():
    """
    Fetches the XLSX data, cleans it based on inspection, and uploads it to Firebase.
    """
    print("1. Fetching data from IBGE...")
    try:
        # 1. Fetch the binary content of the XLSX file
        response = requests.get(IBGE_URL)
        response.raise_for_status() 

        # 2. Read the binary content directly into a pandas DataFrame
        data = BytesIO(response.content)
        
        # KEY CHANGE: skiprows=3 to skip metadata, header=1 to use the row with sector names
        df = pd.read_excel(data, sheet_name='Tabela', skiprows=3, header=1)

        print(f"Data fetched successfully. Initial size: {df.shape}")

        # --- Cleaning and Structuring Data ---
        
        # 3. Rename the time column and drop the redundant 'Brasil' column
        df.rename(columns={'Unnamed: 1': 'Trimestre'}, inplace=True)
        df.drop(columns=['Unnamed: 0'], inplace=True)
        
        # 4. Handle non-numeric values (like '..', '...') that IBGE uses for not available/not applicable
        # We must replace these string values with NaN before conversion
        df = df.replace(['..', '...'], pd.NA)

        # 5. Convert all data columns (from index 1 onwards) to numeric (float)
        # The 'Trimestre' column is at index 0 after dropping 'Unnamed: 0'
        for col in df.columns[1:]:
             df[col] = pd.to_numeric(df[col], errors='coerce')

        # 6. Drop any row where the 'Trimestre' is NaN (removes any junk rows at the end)
        df.dropna(subset=['Trimestre'], inplace=True)

        # 7. Convert DataFrame to a list of dictionaries (JSON format for Firebase)
        data_to_upload = df.to_dict(orient='records')
        
        print(f"Data ready for upload: {len(data_to_upload)} records.")
        
        # --- Upload to Firebase ---

        print(f"\n2. Uploading data to Firebase at: {FIREBASE_URL}")
        
        # Use PUT to replace the data at the 'ibge_cnt' node with the full, clean dataset
        firebase_response = requests.put(FIREBASE_URL, json=data_to_upload)
        
        firebase_response.raise_for_status()

        if firebase_response.status_code == 200:
            print(f"✅ Success! {len(data_to_upload)} records uploaded to Firebase.")
        else:
            print(f"❌ Upload failed with status code: {firebase_response.status_code}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error during request or Firebase upload: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    fetch_and_upload_ibge_data()