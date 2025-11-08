"""
Script to fetch IBGE Table 5932: Taxa de variação do índice de volume trimestral
This table has 4 sheets - each sheet will be uploaded as a separate Firebase node
"""
from ibge_base import (
    fetch_all_sheets, 
    clean_and_structure_data, 
    upload_multiple_sheets_to_firebase
)

# --- Configuration ---

# IBGE URL for the XLSX data (Table 5932)
IBGE_URL = 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5932.xlsx&terr=N&rank=-&query=t/5932/n1/all/v/all/p/all/c11255/all/d/v6561%201,v6562%201,v6563%201,v6564%201/l/v,c11255,t%2Bp'

# Table configuration
TABLE_NUMBER = 5932
TABLE_NAME = 'Taxa de variação do índice de volume trimestral (nº5932)'
PERIOD_RANGE = '1º trimestre 1996 a 2º trimestre 2025'

# --- Data Fetching, Processing, and Upload ---

def fetch_and_upload_ibge_data():
    """
    Fetches all sheets from the XLSX file, cleans them, and uploads each to Firebase.
    """
    print("1. Fetching data from IBGE (Table 5932)...")
    try:
        # Fetch all sheets
        sheets_data = fetch_all_sheets(IBGE_URL)
        print(f"Found {len(sheets_data)} sheets in the Excel file")
        
        # Process each sheet
        processed_sheets = {}
        
        for sheet_name, (df, sector_names) in sheets_data.items():
            print(f"\nProcessing sheet: {sheet_name}")
            print(f"  Initial size: {df.shape}")
            print(f"  Number of sectors: {len(sector_names)}")
            
            try:
                # Clean and structure data
                df_clean = clean_and_structure_data(df, sector_names)
                print(f"  Cleaned data: {len(df_clean)} records")
                processed_sheets[sheet_name] = df_clean
            except Exception as e:
                print(f"  [ERROR] Failed to process sheet '{sheet_name}': {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # Upload all processed sheets to Firebase with nested structure
        print(f"\nUploading {len(processed_sheets)} sheets to Firebase...")
        results = upload_multiple_sheets_to_firebase(processed_sheets, TABLE_NUMBER, TABLE_NAME, PERIOD_RANGE)
        
        # Print summary
        successful = sum(1 for v in results.values() if v)
        failed = len(results) - successful
        
        print(f"\n{'='*60}")
        print("UPLOAD SUMMARY")
        print(f"{'='*60}")
        for sheet_name, success in results.items():
            status = "[SUCCESS]" if success else "[FAILED]"
            print(f"{status} {sheet_name}")
        print(f"\nTotal: {successful} successful, {failed} failed out of {len(results)} sheets")
        
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fetch_and_upload_ibge_data()

