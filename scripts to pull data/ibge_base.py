"""
Base module for IBGE data fetching and Firebase upload.
This module provides reusable functions for fetching IBGE data and uploading to Firebase.
"""
import requests
import pandas as pd
from io import BytesIO
import traceback
from urllib.parse import quote

# Firebase Realtime Database configuration
FIREBASE_BASE_URL = 'https://peixo-28d2d-default-rtdb.firebaseio.com'


def get_category_base_path(category: str) -> str:
    """
    Return the base Firebase path for a given data category.
    """
    if not category:
        category = 'cnt'
    category_clean = category.strip('/').lower()
    return f'ibge_data/{category_clean}'


def fetch_ibge_data(ibge_url, skiprows=4, header_row=3, data_start_row=4):
    """
    Fetches IBGE data from the given URL and returns a processed DataFrame.
    
    Args:
        ibge_url: URL to fetch the Excel file from IBGE
        skiprows: Number of rows to skip when reading the data (default: 4)
        header_row: Row index (0-based) containing sector/column names (default: 3)
        data_start_row: Row index (0-based) where data starts (default: 4)
    
    Returns:
        tuple: (df, sector_names) - DataFrame with data and list of sector names
    """
    try:
        # Fetch the binary content of the XLSX file
        response = requests.get(ibge_url)
        response.raise_for_status()
        
        # Read the binary content directly into a pandas DataFrame
        data = BytesIO(response.content)
        
        # Read header row to get sector names
        df_sectors = pd.read_excel(data, sheet_name='Tabela', header=None, nrows=header_row+1)
        sector_names = df_sectors.iloc[header_row, 2:].tolist()  # Sector names start from column 2
        
        # Reset the BytesIO object to read again
        data.seek(0)
        
        # Read the data starting from data_start_row
        df = pd.read_excel(data, sheet_name='Tabela', skiprows=data_start_row, header=None)
        
        return df, sector_names
        
    except Exception as e:
        print(f"[ERROR] Error fetching IBGE data: {e}")
        raise


def clean_column_name(col_name):
    """
    Cleans a column name to be Firebase-compliant.
    Firebase keys cannot contain: . $ # [ ] / and some other characters.
    """
    import re
    if pd.isna(col_name):
        return None
    
    col_str = str(col_name).strip()
    # Replace invalid characters with underscores
    cleaned = re.sub(r'[.$#\[\]/\\]', '_', col_str)
    # Replace multiple spaces/underscores with single underscore
    cleaned = re.sub(r'[_\s]+', '_', cleaned)
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    # Ensure it's not empty
    if not cleaned:
        cleaned = 'unnamed_column'
    return cleaned


def clean_and_structure_data(df, sector_names):
    """
    Cleans and structures the DataFrame with proper column names.
    
    Args:
        df: Raw DataFrame from IBGE
        sector_names: List of sector/column names
    
    Returns:
        DataFrame: Cleaned and structured DataFrame
    """
    # Drop the 'Brasil' column (first column) if it exists
    if df.shape[1] > 0:
        df = df.drop(columns=[0])
    
    # Rename the second column (now first after drop) to 'Trimestre'
    if df.shape[1] > 0:
        df.rename(columns={1: 'Trimestre'}, inplace=True)
    
    # Rename sector columns and clean them for Firebase compliance
    for i, sector_name in enumerate(sector_names):
        col_idx = i + 2  # Sector columns start from index 2
        if col_idx in df.columns:
            # Clean sector name and use it as column name, or use a default name
            if pd.notna(sector_name):
                clean_name = clean_column_name(sector_name)
                if clean_name:
                    df.rename(columns={col_idx: clean_name}, inplace=True)
                else:
                    df.rename(columns={col_idx: f'Sector_{i+1}'}, inplace=True)
            else:
                df.rename(columns={col_idx: f'Sector_{i+1}'}, inplace=True)
    
    # Clean all remaining column names (including 'Trimestre' if it has invalid chars)
    df.columns = [clean_column_name(col) if col else f'col_{i}' for i, col in enumerate(df.columns)]
    
    # Handle non-numeric values (like '..', '...') that IBGE uses for not available/not applicable
    df = df.replace(['..', '...'], pd.NA)
    
    # Convert all data columns (except 'Trimestre') to numeric (float)
    for col in df.columns:
        if col != 'Trimestre':
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Drop any row where the 'Trimestre' is NaN (removes any junk rows at the end)
    df.dropna(subset=['Trimestre'], inplace=True)
    
    return df


def fetch_all_sheets(ibge_url, skiprows=4, header_row=3, data_start_row=4):
    """
    Fetches all sheets from an IBGE Excel file and returns a dictionary of sheet data.
    
    Args:
        ibge_url: URL to fetch the Excel file from IBGE
        skiprows: Number of rows to skip when reading the data (default: 4)
        header_row: Row index (0-based) containing sector/column names (default: 3)
        data_start_row: Row index (0-based) where data starts (default: 4)
    
    Returns:
        dict: Dictionary with sheet names as keys and (df, sector_names) tuples as values
    """
    try:
        # Fetch the binary content of the XLSX file
        response = requests.get(ibge_url)
        response.raise_for_status()
        
        # Read the binary content
        data = BytesIO(response.content)
        
        # Get all sheet names
        excel_file = pd.ExcelFile(data)
        sheet_names = excel_file.sheet_names
        
        sheets_data = {}
        
        for sheet_name in sheet_names:
            try:
                # Reset the BytesIO object
                data.seek(0)
                
                # Read header row to get sector names
                df_sectors = pd.read_excel(data, sheet_name=sheet_name, header=None, nrows=header_row+1)
                sector_names = df_sectors.iloc[header_row, 2:].tolist() if header_row < len(df_sectors) else []
                
                # Reset and read data
                data.seek(0)
                df = pd.read_excel(data, sheet_name=sheet_name, skiprows=data_start_row, header=None)
                
                sheets_data[sheet_name] = (df, sector_names)
            except Exception as e:
                print(f"[WARNING] Error reading sheet '{sheet_name}': {e}")
                continue
        
        return sheets_data
        
    except Exception as e:
        print(f"[ERROR] Error fetching IBGE data: {e}")
        raise


def clean_data_for_json(data):
    """
    Cleans data to make it JSON-compliant (handles NaN, Infinity, etc.).
    
    Args:
        data: List of dictionaries or DataFrame
    
    Returns:
        List of dictionaries with NaN values replaced with None
    """
    import json
    import math
    
    if isinstance(data, pd.DataFrame):
        data_dict = data.to_dict(orient='records')
        data_type = 'dataframe'
    elif isinstance(data, dict):
        data_dict = data
        data_type = 'dict'
    else:
        data_dict = data
        data_type = 'list'
    
    # Replace NaN, Infinity, and -Infinity with None
    def clean_value(v):
        if isinstance(v, dict):
            return {k: clean_value(val) for k, val in v.items()}
        if isinstance(v, list):
            return [clean_value(item) for item in v]
        try:
            if isinstance(v, float):
                if pd.isna(v) or math.isnan(v):
                    return None
                if math.isinf(v):
                    return None
            else:
                if pd.isna(v):
                    return None
        except TypeError:
            # pd.isna raises TypeError for some non-numeric types; leave value as-is
            pass
        return v
    
    if data_type == 'dict':
        return {k: clean_value(v) for k, v in data_dict.items()}
    
    cleaned_data = []
    for record in data_dict:
        if isinstance(record, dict):
            cleaned_record = {k: clean_value(v) for k, v in record.items()}
        else:
            cleaned_record = clean_value(record)
        cleaned_data.append(cleaned_record)
    
    return cleaned_data


def upload_to_firebase_path(data, firebase_path):
    """
    Uploads data to a specific Firebase path.
    
    Args:
        data: List of dictionaries or DataFrame to upload
        firebase_path: Firebase path (e.g., 'ibge_data/cnt/table_1620/data')
    
    Returns:
        bool: True if upload was successful, False otherwise
    """
    try:
        # Clean data for JSON compliance (handle NaN values)
        data_to_upload = clean_data_for_json(data)
        
        # URL-encode each path segment
        path_parts = firebase_path.split('/')
        encoded_parts = [quote(part, safe="") for part in path_parts]
        firebase_path_encoded = '/'.join(encoded_parts)
        
        firebase_url = f'{FIREBASE_BASE_URL}/{firebase_path_encoded}.json'
        
        # Use PUT to replace the data at the path
        firebase_response = requests.put(firebase_url, json=data_to_upload)
        firebase_response.raise_for_status()
        
        if firebase_response.status_code == 200:
            print(f"[SUCCESS] {len(data_to_upload)} records uploaded to: {firebase_path}")
            return True
        else:
            print(f"[ERROR] Upload failed with status code: {firebase_response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Error during Firebase upload: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text[:500]}")
        return False
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred during upload: {e}")
        traceback.print_exc()
        return False


def upload_metadata(table_number, table_name, period_range=None, sheet_count=None, category='cnt'):
    """
    Uploads metadata for a table to Firebase.
    
    Args:
        table_number: Table number (e.g., 1620)
        table_name: Full descriptive name of the table
        period_range: Optional period range string
        sheet_count: Optional number of sheets (for multi-sheet tables)
    
    Returns:
        bool: True if upload was successful, False otherwise
    """
    metadata = {
        'table_number': table_number,
        'table_name': table_name,
    }
    
    if period_range:
        metadata['period_range'] = period_range
    
    if sheet_count:
        metadata['sheet_count'] = sheet_count
    
    # Upload metadata directly (it's already a dict, not a list)
    try:
        base_path = get_category_base_path(category)
        path_parts = f'{base_path}/table_{table_number}/metadata'.split('/')
        encoded_parts = [quote(part, safe="") for part in path_parts]
        firebase_path_encoded = '/'.join(encoded_parts)
        firebase_url = f'{FIREBASE_BASE_URL}/{firebase_path_encoded}.json'
        
        firebase_response = requests.put(firebase_url, json=metadata)
        firebase_response.raise_for_status()
        
        if firebase_response.status_code == 200:
            print(f"[SUCCESS] Metadata uploaded to: {base_path}/table_{table_number}/metadata")
            return True
        else:
            print(f"[ERROR] Metadata upload failed with status code: {firebase_response.status_code}")
            return False
    except Exception as e:
        print(f"[ERROR] Failed to upload metadata: {e}")
        return False


def upload_table_data(data, table_number, table_name, period_range=None, category='cnt'):
    """
    Uploads a single-sheet table to Firebase with nested structure.
    
    Args:
        data: DataFrame or list of dictionaries to upload
        table_number: Table number (e.g., 1620)
        table_name: Full descriptive name of the table
        period_range: Optional period range string
    
    Returns:
        bool: True if upload was successful, False otherwise
    """
    print(f"\n2. Uploading table {table_number} to Firebase")
    print(f"   Table: {table_name}")
    
    # Upload data
    base_path = get_category_base_path(category)
    data_path = f'{base_path}/table_{table_number}/data'
    data_success = upload_to_firebase_path(data, data_path)
    
    # Upload metadata
    metadata_success = upload_metadata(table_number, table_name, period_range, category=category)
    
    return data_success and metadata_success


def clean_firebase_key(key):
    """
    Cleans a key to be Firebase-compliant.
    Firebase keys cannot contain: . $ # [ ] / and some other characters.
    """
    import re
    # Replace invalid characters with underscores
    # Keep only alphanumeric, spaces, hyphens, and underscores
    cleaned = re.sub(r'[.$#\[\]/\\]', '_', key)
    # Replace multiple spaces/underscores with single underscore
    cleaned = re.sub(r'[_\s]+', '_', cleaned)
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    return cleaned


def upload_multiple_sheets_to_firebase(sheets_data, table_number, table_name, period_range=None, category='cnt'):
    """
    Uploads multiple sheets to Firebase with nested structure.
    
    Args:
        sheets_data: Dictionary with sheet names as keys and DataFrames as values
        table_number: Table number (e.g., 2072)
        table_name: Full descriptive name of the table
        period_range: Optional period range string
    
    Returns:
        dict: Dictionary with sheet names as keys and success status (bool) as values
    """
    print(f"\n2. Uploading table {table_number} (multi-sheet) to Firebase")
    print(f"   Table: {table_name}")
    print(f"   Sheets: {len([s for s in sheets_data.keys() if s.lower() not in ['notas', 'notes', 'metadata']])} sheets")
    
    base_path = get_category_base_path(category)

    results = {}
    uploaded_sheets = []
    
    for sheet_name, df in sheets_data.items():
        # Skip "Notas" sheets or other metadata sheets
        if sheet_name.lower() in ['notas', 'notes', 'metadata']:
            print(f"[SKIP] Skipping metadata sheet: {sheet_name}")
            continue
        
        # Clean sheet name for Firebase - create a safe key
        clean_sheet_name = clean_firebase_key(sheet_name)
        
        # Upload sheet data to nested path
        sheet_path = f'{base_path}/table_{table_number}/sheets/{clean_sheet_name}/data'
        
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
        'table_number': table_number,
        'table_name': table_name,
        'sheet_count': len(uploaded_sheets),
        'sheets': uploaded_sheets
    }
    
    if period_range:
        metadata['period_range'] = period_range
    
    try:
        path_parts = f'{base_path}/table_{table_number}/metadata'.split('/')
        encoded_parts = [quote(part, safe="") for part in path_parts]
        firebase_path_encoded = '/'.join(encoded_parts)
        firebase_url = f'{FIREBASE_BASE_URL}/{firebase_path_encoded}.json'
        
        firebase_response = requests.put(firebase_url, json=metadata)
        firebase_response.raise_for_status()
        
        if firebase_response.status_code == 200:
            print(f"[SUCCESS] Metadata uploaded to: {base_path}/table_{table_number}/metadata")
        else:
            print(f"[WARNING] Metadata upload failed with status code: {firebase_response.status_code}")
    except Exception as e:
        print(f"[WARNING] Failed to upload metadata: {e}")
    
    return results

