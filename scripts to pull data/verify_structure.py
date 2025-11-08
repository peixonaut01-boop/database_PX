"""
Script to verify the new nested Firebase structure.
"""
import requests
from urllib.parse import quote

FIREBASE_BASE_URL = 'https://peixo-28d2d-default-rtdb.firebaseio.com'

def get_firebase_data(path):
    """Get data from Firebase path."""
    try:
        encoded_parts = [quote(part, safe="") for part in path.split('/')]
        firebase_path = '/'.join(encoded_parts)
        url = f'{FIREBASE_BASE_URL}/{firebase_path}.json'
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        print(f"Error fetching {path}: {e}")
        return None

print("="*60)
print("Verifying New Firebase Structure")
print("="*60)

# Check single-sheet table
print("\n1. Single-sheet table (1620):")
metadata_1620 = get_firebase_data('ibge_data/table_1620/metadata')
if metadata_1620:
    print(f"   [OK] Metadata found: {metadata_1620.get('table_name', 'N/A')[:50]}...")
    print(f"   [OK] Table number: {metadata_1620.get('table_number')}")
    print(f"   [OK] Period: {metadata_1620.get('period_range', 'N/A')}")
else:
    print("   [ERROR] Metadata not found")

data_1620 = get_firebase_data('ibge_data/table_1620/data')
if data_1620:
    count = len(data_1620) if isinstance(data_1620, list) else 'N/A'
    print(f"   [OK] Data found: {count} records")
else:
    print("   [ERROR] Data not found")

# Check multi-sheet table (2072)
print("\n2. Multi-sheet table (2072):")
metadata_2072 = get_firebase_data('ibge_data/table_2072/metadata')
if metadata_2072:
    print(f"   [OK] Metadata found: {metadata_2072.get('table_name', 'N/A')[:50]}...")
    print(f"   [OK] Table number: {metadata_2072.get('table_number')}")
    print(f"   [OK] Sheet count: {metadata_2072.get('sheet_count')}")
    if 'sheets' in metadata_2072:
        print(f"   [OK] Sheets list: {len(metadata_2072['sheets'])} sheets")
else:
    print("   [ERROR] Metadata not found")

# Check one sheet from 2072
sheet_2072 = get_firebase_data('ibge_data/table_2072/sheets/Produto_Interno_Bruto_(Milh/data')
if sheet_2072:
    count = len(sheet_2072) if isinstance(sheet_2072, list) else 'N/A'
    print(f"   [OK] Sample sheet data: {count} records")
else:
    print("   [ERROR] Sample sheet not found")

# Check multi-sheet table (5932)
print("\n3. Multi-sheet table (5932):")
metadata_5932 = get_firebase_data('ibge_data/table_5932/metadata')
if metadata_5932:
    print(f"   [OK] Metadata found: {metadata_5932.get('table_name', 'N/A')[:50]}...")
    print(f"   [OK] Table number: {metadata_5932.get('table_number')}")
    print(f"   [OK] Sheet count: {metadata_5932.get('sheet_count')}")
else:
    print("   [ERROR] Metadata not found")

# Check all tables exist
print("\n4. Checking all tables:")
tables = [1620, 1621, 1846, 2072, 5932, 6612, 6613, 6726, 6727]
for table_num in tables:
    metadata = get_firebase_data(f'ibge_data/table_{table_num}/metadata')
    if metadata:
        print(f"   [OK] Table {table_num}: OK")
    else:
        print(f"   [ERROR] Table {table_num}: Missing")

print("\n" + "="*60)
print("Verification Complete!")
print("="*60)

