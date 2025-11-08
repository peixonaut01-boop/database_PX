"""
Master script to run all PMC (Pesquisa Mensal de Comércio) table scripts.
"""
from __future__ import annotations

import os
import subprocess
import sys

PMC_SCRIPTS = [
    'ibge_8190.py',
]


def run_script(script_name: str) -> bool:
    print("=" * 80)
    print(f"Running: {script_name}")
    print("=" * 80)
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        result = subprocess.run(
            [sys.executable, os.path.join(script_dir, script_name)],
            text=True,
            check=False,
        )
        if result.returncode == 0:
            print(f"[SUCCESS] {script_name} completed")
            return True
        print(f"[ERROR] {script_name} exited with code {result.returncode}")
    except Exception as exc:
        print(f"[ERROR] Failed to run {script_name}: {exc}")
    return False


def main() -> None:
    results = {script: run_script(script) for script in PMC_SCRIPTS}
    success = sum(1 for ok in results.values() if ok)
    failure = len(results) - success

    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    for script, ok in results.items():
        print(f"{'[SUCCESS]' if ok else '[FAILED]'} {script}")
    print(f"Total: {success} success, {failure} failure")
    if failure:
        sys.exit(1)


if __name__ == '__main__':
    main()
