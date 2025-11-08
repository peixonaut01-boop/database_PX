"""
Helper script to test IBGE URLs before using them in the main scripts.
"""
import requests
import sys

def test_url(url, table_number):
    """Test if an IBGE URL is valid."""
    print(f"\nTesting Table {table_number}...")
    print(f"URL: {url}")
    
    try:
        response = requests.get(url, stream=True)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            # Check content type
            content_type = response.headers.get('Content-Type', '')
            print(f"Content-Type: {content_type}")
            
            if 'excel' in content_type or 'spreadsheet' in content_type:
                print("[SUCCESS] URL is valid and returns Excel file")
                return True
            else:
                print("[WARNING] URL returns 200 but may not be Excel format")
                print(f"Response preview: {response.text[:200]}")
                return False
        else:
            print(f"[ERROR] URL returned status {response.status_code}")
            print(f"Response: {response.text[:500]}")
            return False
            
    except Exception as e:
        print(f"[ERROR] Exception occurred: {e}")
        return False

if __name__ == "__main__":
    # Test URLs for all tables
    tables = {
        '1620': 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela1620.xlsx&terr=N&rank=-&query=t/1620/n1/all/v/all/p/all/c11255/all/d/v583%202/l/,v%2Bc11255,t%2Bp',
        '1621': 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela1621.xlsx&terr=N&rank=-&query=t/1621/n1/all/v/all/p/all/c11255/all/d/v583%202/l/,v%2Bc11255,t%2Bp',
        '1846': 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela1846.xlsx&terr=N&rank=-&query=t/1846/n1/all/v/all/p/all/c11255/all/d/v583%202/l/,v%2Bc11255,t%2Bp',
        '2072': 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela2072.xlsx&terr=N&rank=-&query=t/2072/n1/all/v/all/p/all/c11255/all/d/v583%202/l/,v%2Bc11255,t%2Bp',
        '2205': 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela2205.xlsx&terr=N&rank=-&query=t/2205/n1/all/v/all/p/all/c11255/all/d/v583%202/l/,v%2Bc11255,t%2Bp',
        '5932': 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5932.xlsx&terr=N&rank=-&query=t/5932/n1/all/v/all/p/all/c11255/all/d/v583%202/l/,v%2Bc11255,t%2Bp',
        '6612': 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6612.xlsx&terr=N&rank=-&query=t/6612/n1/all/v/all/p/all/c11255/all/d/v583%202/l/,v%2Bc11255,t%2Bp',
        '6613': 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6613.xlsx&terr=N&rank=-&query=t/6613/n1/all/v/all/p/all/c11255/all/d/v583%202/l/,v%2Bc11255,t%2Bp',
        '6726': 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6726.xlsx&terr=N&rank=-&query=t/6726/n1/all/v/all/p/all/c11255/all/d/v583%202/l/,v%2Bc11255,t%2Bp',
        '6727': 'https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6727.xlsx&terr=N&rank=-&query=t/6727/n1/all/v/all/p/all/c11255/all/d/v583%202/l/,v%2Bc11255,t%2Bp',
    }
    
    print("="*60)
    print("Testing IBGE URLs")
    print("="*60)
    
    results = {}
    for table_num, url in tables.items():
        results[table_num] = test_url(url, table_num)
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    for table_num, success in results.items():
        status = "[OK]" if success else "[FAIL]"
        print(f"{status} Table {table_num}")
    
    successful = sum(1 for v in results.values() if v)
    print(f"\n{successful}/{len(results)} URLs are valid")

