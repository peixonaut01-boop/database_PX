"""
Script to delete the old flat Firebase structure before migrating to nested structure.
"""
import requests
from urllib.parse import quote

FIREBASE_BASE_URL = 'https://peixo-28d2d-default-rtdb.firebaseio.com'

# List of old table names (flat structure at root level)
OLD_TABLE_NAMES = [
    'Série encadeada do índice de volume trimestral (Base: média 1995 = 100) (nº1620)',
    'Série encadeada do índice de volume trimestral com ajuste sazonal (Base: média 1995 = 100) (nº1621)',
    'Valores a preços correntes (nº1846)',
    'Valores encadeados a preços de 1995 (nº6612)',
    'Valores encadeados a preços de 1995 com ajuste sazonal (nº6613)',
    'Taxa de poupança (nº6726)',
    'Taxa de investimento (nº6727)',
    # Multi-sheet tables (old format with hyphens)
    'Contas econômicas trimestrais (nº2072) - Produto_Interno_Bruto_(Milh',
    'Contas econômicas trimestrais (nº2072) - (+)_Salários_(líquidos_receb',
    'Contas econômicas trimestrais (nº2072) - (+)_Rendas_de_propriedade_(l',
    'Contas econômicas trimestrais (nº2072) - (=)_Renda_nacional_bruta_(Mi',
    'Contas econômicas trimestrais (nº2072) - (+)_Outras_transferências_co',
    'Contas econômicas trimestrais (nº2072) - (=)_Renda_nacional_disponível',
    'Contas econômicas trimestrais (nº2072) - (-)_Despesa_de_consumo_final',
    'Contas econômicas trimestrais (nº2072) - (=)_Poupança_bruta_(Milhões',
    'Contas econômicas trimestrais (nº2072) - (-)_Formação_bruta_de_capita',
    'Contas econômicas trimestrais (nº2072) - (+)_Cessão_de_ativos_não_fin',
    'Contas econômicas trimestrais (nº2072) - (+)_Transferências_de_capita',
    'Contas econômicas trimestrais (nº2072) - (=)_Capacidade_necessidade',
    'Taxa de variação do índice de volume trimestral (nº5932) - Taxa_trimestral_(em_relação',
    'Taxa de variação do índice de volume trimestral (nº5932) - Taxa_acumulada_em_quatro_tri',
    'Taxa de variação do índice de volume trimestral (nº5932) - Taxa_acumulada_ao_longo_do_a',
    'Taxa de variação do índice de volume trimestral (nº5932) - Taxa_trimestre_contra_trimes',
]

def delete_firebase_node(node_name):
    """Delete a node from Firebase."""
    try:
        encoded_name = quote(node_name, safe="")
        firebase_url = f'{FIREBASE_BASE_URL}/{encoded_name}.json'
        
        # Use DELETE method to remove the node
        response = requests.delete(firebase_url)
        
        if response.status_code == 200:
            print(f"[SUCCESS] Deleted: {node_name[:60]}...")
            return True
        elif response.status_code == 404:
            print(f"[SKIP] Not found (already deleted): {node_name[:60]}...")
            return True
        else:
            print(f"[ERROR] Failed to delete {node_name[:60]}... Status: {response.status_code}")
            return False
    except Exception as e:
        print(f"[ERROR] Exception deleting {node_name[:60]}...: {e}")
        return False

def main():
    """Delete all old table nodes."""
    print("="*60)
    print("Cleaning up old Firebase structure")
    print("="*60)
    print(f"Deleting {len(OLD_TABLE_NAMES)} old table nodes...\n")
    
    results = []
    for table_name in OLD_TABLE_NAMES:
        success = delete_firebase_node(table_name)
        results.append(success)
    
    # Also check for any remaining old structure patterns
    print("\n" + "="*60)
    print("Checking for any remaining old structure...")
    print("="*60)
    
    # Try to delete the old ibge_cnt if it exists
    old_ibge_cnt = delete_firebase_node('ibge_cnt')
    
    # Summary
    successful = sum(1 for r in results if r)
    print(f"\n{'='*60}")
    print("CLEANUP SUMMARY")
    print(f"{'='*60}")
    print(f"Deleted: {successful}/{len(results)} old table nodes")
    print(f"{'='*60}\n")
    
    print("Old structure cleanup complete!")
    print("You can now run the table scripts to upload to the new nested structure.")

if __name__ == "__main__":
    main()

