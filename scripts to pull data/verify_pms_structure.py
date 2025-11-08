import requests
from urllib.parse import quote

FIREBASE_BASE_URL = 'https://peixo-28d2d-default-rtdb.firebaseio.com'


def get_firebase_data(path):
    """Retrieve JSON data from a Firebase path."""
    encoded_parts = [quote(part, safe="") for part in path.split('/')]
    firebase_path = '/'.join(encoded_parts)
    url = f"{FIREBASE_BASE_URL}/{firebase_path}.json"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None


def main():
    print("=" * 60)
    print("Verifying PMS Firebase structure")
    print("=" * 60)

    branches = {
        'Receita metadata': 'pms_data/table_5906/receita/metadata',
        'Receita sheets': 'pms_data/table_5906/receita/sheets',
        'Volume metadata': 'pms_data/table_5906/volume/metadata',
        'Volume sheets': 'pms_data/table_5906/volume/sheets',
    }

    for name, path in branches.items():
        data = get_firebase_data(path)
        if data:
            detail = (
                f"{len(data)} entries" if isinstance(data, list) else
                f"keys: {', '.join(list(data.keys())[:5])}" if isinstance(data, dict) else
                "value retrieved"
            )
            print(f"[OK] {name}: {detail}")
        else:
            print(f"[ERROR] {name}: missing or empty")

    print("\n" + "=" * 60)
    print("Verification done")
    print("=" * 60)


if __name__ == '__main__':
    main()
