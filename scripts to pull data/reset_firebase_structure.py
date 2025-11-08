"""
Utility script to reset the IBGE-related nodes in Firebase before a full re-upload.
Removes `ibge_data` and legacy `pms_data` roots so that scripts can rebuild the
structure under ibge_data/cnt and ibge_data/pms.
"""
import requests

FIREBASE_BASE_URL = 'https://peixo-28d2d-default-rtdb.firebaseio.com'
TARGET_NODES = ['ibge_data', 'pms_data']


def delete_node(node):
    url = f"{FIREBASE_BASE_URL}/{node}.json"
    try:
        response = requests.delete(url)
        if response.status_code == 200:
            print(f"[SUCCESS] Deleted node: {node}")
        elif response.status_code == 404:
            print(f"[SKIP] Node not found (already removed): {node}")
        else:
            print(f"[ERROR] Failed to delete {node} - status {response.status_code}")
            print(f"        Response: {response.text[:200]}")
    except Exception as exc:
        print(f"[ERROR] Exception deleting {node}: {exc}")


def main():
    print("=" * 60)
    print("Resetting Firebase nodes for IBGE data")
    print("=" * 60)
    for node in TARGET_NODES:
        delete_node(node)
    print("=" * 60)
    print("Reset complete. Re-run the data ingestion scripts to repopulate.")
    print("=" * 60)


if __name__ == '__main__':
    main()
