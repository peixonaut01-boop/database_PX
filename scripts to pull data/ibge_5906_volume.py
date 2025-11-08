"""
Script to fetch IBGE Table 5906 - Subbranch 2: Volume
Pesquisa Mensal de Serviços (PMS) - Monthly Services Survey
This table has 6 sheets - each sheet will be uploaded as a separate Firebase node

Note: PMS data structure is different from CNT - it's monthly data with simpler structure
"""
from ibge_base import (
    upload_to_firebase_path,
    clean_firebase_key
)
import pandas as pd
import requests
from io import BytesIO

# --- Configuration ---

# IBGE URL for the XLSX data (Table 5906 - Volume)
IBGE_URL = 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5906.xlsx&terr=N&rank=-&query=t/5906/n1/all/v/all/p/all/c11046/56726/d/v7167%205,v7168%205,v11623%201,v11624%201,v11625%201,v11626%201/l/v,c11046,t%2Bp'

# Table configuration
TABLE_NUMBER = 5906
TABLE_NAME = 'PMS - Índice e variação da receita nominal e do volume de serviços (2022 = 100) - Volume (nº5906)'
PERIOD_RANGE = 'janeiro 2011 a agosto 2025'

# --- Data Fetching, Processing, and Upload ---

def clean_pms_data(df, sheet_name):
    """
    Cleans PMS data which has a simpler structure than CNT.
    Structure: Column 0 = 'Brasil' (first row only), Column 1 = Period, Column 2 = Data value
    """
    # Drop the 'Brasil' column (column 0)
    if df.shape[1] > 0:
        df = df.drop(columns=[0])
    
    # Rename column 1 to 'Periodo' (Period) and column 2 to 'Valor' (Value)
    if df.shape[1] >= 2:
        df.rename(columns={1: 'Periodo'}, inplace=True)
        if df.shape[1] > 1:
            df.rename(columns={2: 'Valor'}, inplace=True)
    
    # Handle non-numeric values
    df = df.replace(['..', '...'], pd.NA)
    
    # Convert data column to numeric
    for col in df.columns:
        if col != 'Periodo':
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Drop any row where 'Periodo' is NaN
    df.dropna(subset=['Periodo'], inplace=True)
    
    return df

def fetch_and_upload_ibge_data():
    """
    Fetches all sheets from the XLSX file, cleans them, and uploads each to Firebase.
    """
    print("1. Fetching data from IBGE (Table 5906 - Volume)...")
    try:
        # Fetch the Excel file
        response = requests.get(IBGE_URL)
        response.raise_for_status()
        data = BytesIO(response.content)
        
        # Get all sheet names
        excel_file = pd.ExcelFile(data)
        sheet_names = excel_file.sheet_names
        print(f"Found {len(sheet_names)} sheets in the Excel file")
        
        # Process each sheet
        processed_sheets = {}
        
        for sheet_name in sheet_names:
            # Skip "Notas" sheets
            if sheet_name.lower() in ['notas', 'notes', 'metadata']:
                continue
                
            print(f"\nProcessing sheet: {sheet_name}")
            
            try:
                # Reset and read the sheet
                data.seek(0)
                df = pd.read_excel(data, sheet_name=sheet_name, skiprows=4, header=None)
                print(f"  Initial size: {df.shape}")
                
                # Clean and structure data (PMS has simpler structure)
                df_clean = clean_pms_data(df, sheet_name)
                print(f"  Cleaned data: {len(df_clean)} records")
                processed_sheets[sheet_name] = df_clean
            except Exception as e:
                print(f"  [ERROR] Failed to process sheet '{sheet_name}': {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # Upload all processed sheets to Firebase with nested structure
        # Store under ibge_data/pms branch
        firebase_base_path = f'ibge_data/pms/table_{TABLE_NUMBER}/volume'
        print(f"\nUploading {len(processed_sheets)} sheets to Firebase...")
        
        results = {}
        uploaded_sheets = []
        
        for sheet_name, df in processed_sheets.items():
            # Clean sheet name for Firebase
            clean_sheet_name = clean_firebase_key(sheet_name)
            
            # Upload sheet data to nested path
            sheet_path = f'{firebase_base_path}/sheets/{clean_sheet_name}/data'
            
            try:
                success = upload_to_firebase_path(df, sheet_path)
                results[sheet_name] = success
                if success:
                    uploaded_sheets.append({
                        'sheet_name': sheet_name,
                        'clean_name': clean_sheet_name,
                        'record_count': len(df)
                    })
            except Exception as e:
                print(f"[ERROR] Failed to upload sheet '{sheet_name}': {e}")
                results[sheet_name] = False
        
        # Upload metadata with sheet information
        metadata = {
            'table_number': TABLE_NUMBER,
            'table_name': TABLE_NAME,
            'subbranch': 'volume',
            'sheet_count': len(uploaded_sheets),
            'sheets': uploaded_sheets,
            'period_range': PERIOD_RANGE
        }
        
        metadata_path = f'{firebase_base_path}/metadata'
        metadata_success = upload_to_firebase_path(metadata, metadata_path)
        
        if not metadata_success:
            print("[WARNING] Failed to upload metadata")
        
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

