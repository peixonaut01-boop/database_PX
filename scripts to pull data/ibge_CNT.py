"""
Script to fetch IBGE Table 1620: Série encadeada do índice de volume trimestral (Base: média 1995 = 100)
"""
from ibge_base import fetch_ibge_data, clean_and_structure_data, upload_table_data_keyed_by_date

# --- Configuration ---

# IBGE URL for the XLSX data (Table 1620)
IBGE_URL = 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela1620.xlsx&terr=N&rank=-&query=t/1620/n1/all/v/all/p/all/c11255/all/d/v583%202/l/,v%2Bc11255,t%2Bp'

# Table configuration
TABLE_NUMBER = 1620
TABLE_NAME = 'Série encadeada do índice de volume trimestral (Base: média 1995 = 100) (nº1620)'
PERIOD_RANGE = '1º trimestre 1996 a 2º trimestre 2025'

# --- Data Fetching, Processing, and Upload ---

def fetch_and_upload_ibge_data():
    """
    Fetches the XLSX data, cleans it, and uploads it to Firebase.
    """
    print("1. Fetching data from IBGE (Table 1620)...")
    try:
        # Fetch and process data
        df, sector_names = fetch_ibge_data(IBGE_URL)
        print(f"Data fetched successfully. Initial size: {df.shape}")
        print(f"Number of sectors: {len(sector_names)}")
        
        # Clean and structure data
        df = clean_and_structure_data(df, sector_names)
        print(f"Data ready for upload: {len(df)} records.")
        
        # Upload to Firebase with date keys
        success = upload_table_data_keyed_by_date(
            df,
            date_column='Trimestre',
            table_number=TABLE_NUMBER,
            table_name=TABLE_NAME,
            period_range=PERIOD_RANGE
        )
        
        if not success:
            print("[ERROR] Failed to upload data to Firebase.")
            
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_upload_ibge_data()