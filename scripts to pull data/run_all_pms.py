"""
Master script to run all PMS (Pesquisa Mensal de Serviços) table scripts.
This script executes all PMS data fetching scripts in sequence.
"""
import sys
import subprocess
import os

# List of PMS table scripts to run
PMS_SCRIPTS = [
    'ibge_5906_receita.py',  # Table 5906 - Receita (Revenue) - 6 sheets
    'ibge_5906_volume.py',   # Table 5906 - Volume - 6 sheets
    'ibge_8163.py',          # Table 8163 - Receita & Volume by segments
]

def run_script(script_name):
    """Run a single script and return True if successful, False otherwise."""
    print(f"\n{'='*80}")
    print(f"Running: {script_name}")
    print(f"{'='*80}\n")
    
    try:
        # Change to the script directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)
        
        # Run the script
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=False,  # Show output in real-time
            text=True,
            check=False  # Don't raise exception on non-zero exit
        )
        
        if result.returncode == 0:
            print(f"\n[SUCCESS] {script_name} completed successfully")
            return True
        else:
            print(f"\n[ERROR] {script_name} failed with exit code {result.returncode}")
            return False
            
    except Exception as e:
        print(f"\n[ERROR] Exception while running {script_name}: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all PMS table scripts."""
    print("="*80)
    print("PMS DATA FETCHING - MASTER SCRIPT")
    print("="*80)
    print(f"Total scripts to run: {len(PMS_SCRIPTS)}")
    print(f"Scripts: {', '.join(PMS_SCRIPTS)}")
    print("="*80)
    
    results = {}
    
    for script in PMS_SCRIPTS:
        success = run_script(script)
        results[script] = success
    
    # Print summary
    print("\n" + "="*80)
    print("FINAL SUMMARY")
    print("="*80)
    
    successful = sum(1 for v in results.values() if v)
    failed = len(results) - successful
    
    for script, success in results.items():
        status = "[SUCCESS]" if success else "[FAILED]"
        print(f"{status} {script}")
    
    print(f"\nTotal: {successful} successful, {failed} failed out of {len(results)} scripts")
    print("="*80)
    
    # Exit with error code if any script failed
    if failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()

