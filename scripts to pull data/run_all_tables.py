"""
Master script to run all IBGE table scripts.
This script will fetch and upload data from all 10 IBGE tables.
"""
import subprocess
import sys
import os

# List of all table scripts
TABLE_SCRIPTS = [
    'ibge_CNT.py',      # Table 1620 - Single sheet
    'ibge_1621.py',     # Table 1621 - Single sheet
    'ibge_1846.py',     # Table 1846 - Single sheet
    'ibge_2072.py',     # Table 2072 - 12 sheets
    'ibge_5932.py',     # Table 5932 - 4 sheets
    'ibge_6612.py',     # Table 6612 - Single sheet
    'ibge_6613.py',     # Table 6613 - Single sheet
    'ibge_6726.py',     # Table 6726 - Single sheet
    'ibge_6727.py',     # Table 6727 - Single sheet
]

def run_script(script_name):
    """Run a single script and return success status."""
    print(f"\n{'='*60}")
    print(f"Running {script_name}...")
    print(f"{'='*60}")
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=False,
            text=True
        )
        return result.returncode == 0
    except Exception as e:
        print(f"[ERROR] Failed to run {script_name}: {e}")
        return False

def main():
    """Run all table scripts."""
    print("="*60)
    print("IBGE Data Fetching - Running All Tables")
    print("="*60)
    print(f"Total tables to process: {len(TABLE_SCRIPTS)}")
    
    results = {}
    for script in TABLE_SCRIPTS:
        success = run_script(script)
        results[script] = success
    
    # Print summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    successful = sum(1 for v in results.values() if v)
    failed = len(results) - successful
    
    for script, success in results.items():
        status = "[SUCCESS]" if success else "[FAILED]"
        print(f"{status} {script}")
    
    print(f"\nTotal: {successful} successful, {failed} failed out of {len(TABLE_SCRIPTS)} tables")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()

