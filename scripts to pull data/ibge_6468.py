"""
Script to fetch IBGE Table 6468: Taxa de desocupação (PNAD Contínua)
Uploads the processed data to Firebase under the PNADCT category.
"""
from ibge_base import fetch_ibge_data, clean_and_structure_data, upload_table_data

# --- Configuration ---

# IBGE URL for the XLSX data (Table 6468)
IBGE_URL = (
    "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6468.xlsx&terr=N&rank=-"
    "&query=t/6468/n1/all/v/4099/p/all/d/v4099%201/l/v,,t%2Bp"
)

# Table configuration
TABLE_NUMBER = 6468
TABLE_NAME = "Taxa de desocupação (PNAD Contínua) (nº6468)"
CATEGORY = "pnadct"


# --- Data Fetching, Processing, and Upload ---

def fetch_and_upload_ibge_data():
    """
    Fetches the XLSX data, cleans it, and uploads it to Firebase.
    """
    print("1. Fetching data from IBGE (Table 6468)...")
    try:
        # Fetch and process data
        df_raw, sector_names = fetch_ibge_data(IBGE_URL)
        print(f"Data fetched successfully. Initial size: {df_raw.shape}")
        print(f"Number of sectors: {len(sector_names)}")

        # Clean and structure data
        df = clean_and_structure_data(df_raw, sector_names)
        print(f"Data ready for upload: {len(df)} records.")

        # Build period range string dynamically
        period_range = None
        if not df.empty and "Trimestre" in df.columns:
            start_period = df["Trimestre"].iloc[0]
            end_period = df["Trimestre"].iloc[-1]
            period_range = f"{start_period} a {end_period}"
            print(f"Detected period range: {period_range}")

        # Upload to Firebase with nested structure
        success = upload_table_data(
            df,
            TABLE_NUMBER,
            TABLE_NAME,
            period_range=period_range,
            category=CATEGORY,
        )

        if not success:
            print("[ERROR] Failed to upload data to Firebase.")

    except Exception as exc:
        print(f"[ERROR] An unexpected error occurred: {exc}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    fetch_and_upload_ibge_data()


