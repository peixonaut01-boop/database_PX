"""
Script to delete all root-level folders in Firebase except 'ibge_data'.
"""
import requests
from urllib.parse import quote

FIREBASE_BASE_URL = 'https://peixo-28d2d-default-rtdb.firebaseio.com'

def get_root_nodes():
    """Get all root-level nodes from Firebase."""
    try:
        url = f'{FIREBASE_BASE_URL}/.json?shallow=true'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data:
                return list(data.keys())
            return []
        return []
    except Exception as e:
        print(f"[ERROR] Failed to get root nodes: {e}")
        return []

def delete_firebase_node(node_name):
    """Delete a node from Firebase root."""
    try:
        encoded_name = quote(node_name, safe="")
        firebase_url = f'{FIREBASE_BASE_URL}/{encoded_name}.json'
        
        # Use DELETE method to remove the node
        response = requests.delete(firebase_url)
        
        if response.status_code == 200:
            print(f"[SUCCESS] Deleted: {node_name}")
            return True
        elif response.status_code == 404:
            print(f"[SKIP] Not found (already deleted): {node_name}")
            return True
        else:
            print(f"[ERROR] Failed to delete {node_name}. Status: {response.status_code}")
            print(f"        Response: {response.text[:200]}")
            return False
    except Exception as e:
        print(f"[ERROR] Exception deleting {node_name}: {e}")
        return False

def main():
    """Delete all root-level nodes except 'ibge_data'."""
    print("="*60)
    print("Cleaning up root-level folders (keeping only 'ibge_data')")
    print("="*60)
    
    # Get all root nodes
    print("\nFetching root-level nodes...")
    root_nodes = get_root_nodes()
    
    if not root_nodes:
        print("No root nodes found or error fetching nodes.")
        return
    
    print(f"Found {len(root_nodes)} root-level nodes:")
    for node in root_nodes:
        print(f"  - {node}")
    
    # Filter out 'ibge_data'
    nodes_to_delete = [node for node in root_nodes if node != 'ibge_data']
    
    if not nodes_to_delete:
        print("\n[INFO] No nodes to delete. Only 'ibge_data' exists at root level.")
        return
    
    print(f"\nDeleting {len(nodes_to_delete)} root-level nodes (keeping 'ibge_data')...")
    print("-"*60)
    
    results = []
    for node_name in nodes_to_delete:
        success = delete_firebase_node(node_name)
        results.append(success)
    
    # Summary
    successful = sum(1 for r in results if r)
    print("\n" + "="*60)
    print("CLEANUP SUMMARY")
    print("="*60)
    print(f"Deleted: {successful}/{len(results)} root-level nodes")
    print(f"Kept: 'ibge_data'")
    print("="*60)
    
    # Verify final state
    print("\nVerifying final state...")
    remaining_nodes = get_root_nodes()
    if remaining_nodes:
        print(f"Remaining root-level nodes: {', '.join(remaining_nodes)}")
        if 'ibge_data' in remaining_nodes and len(remaining_nodes) == 1:
            print("[SUCCESS] Only 'ibge_data' remains at root level!")
        else:
            print("[WARNING] Some unexpected nodes remain.")
    else:
        print("[ERROR] No nodes found at root level.")

if __name__ == "__main__":
    main()

