"""
Script to fetch IBGE Table 6613: Valores encadeados a preços de 1995 com ajuste sazonal
"""
from ibge_base import fetch_ibge_data, clean_and_structure_data, upload_table_data

# --- Configuration ---

# IBGE URL for the XLSX data (Table 6613)
IBGE_URL = 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6613.xlsx&terr=N&rank=-&query=t/6613/n1/all/v/all/p/all/c11255/all/d/v9319%202/l/v,c11255,t%2Bp'

# Table configuration
TABLE_NUMBER = 6613
TABLE_NAME = 'Valores encadeados a preços de 1995 com ajuste sazonal (nº6613)'
PERIOD_RANGE = '1º trimestre 1996 a 2º trimestre 2025'

# --- Data Fetching, Processing, and Upload ---

def fetch_and_upload_ibge_data():
    """
    Fetches the XLSX data, cleans it, and uploads it to Firebase.
    """
    print("1. Fetching data from IBGE (Table 6613)...")
    try:
        # Fetch and process data
        df, sector_names = fetch_ibge_data(IBGE_URL)
        print(f"Data fetched successfully. Initial size: {df.shape}")
        print(f"Number of sectors: {len(sector_names)}")
        
        # Clean and structure data
        df = clean_and_structure_data(df, sector_names)
        print(f"Data ready for upload: {len(df)} records.")
        
        # Upload to Firebase with nested structure
        success = upload_table_data(df, TABLE_NUMBER, TABLE_NAME, PERIOD_RANGE)
        
        if not success:
            print("[ERROR] Failed to upload data to Firebase.")
            
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_upload_ibge_data()

