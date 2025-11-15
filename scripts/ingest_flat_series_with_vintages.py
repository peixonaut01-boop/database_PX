#!/usr/bin/env python3
"""
Ingestão com detecção de vintages (revisões históricas).

Busca série completa e compara com versão anterior para detectar revisões.
Armazena histórico de vintages para análise de mudanças.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
import sys
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

CATALOG_PATH = REPO_ROOT / "data_exports" / "series_catalog_flat.json"

try:
    from api.firebase_client import get_reference
    from scripts.ingest_flat_series import (
        parse_sidra_components, 
        parse_period, 
        load_catalog,
        fetch_series_data
    )
except ImportError as exc:
    raise SystemExit(f"Unable to import required modules: {exc}") from exc


def compare_series(old_values: Dict[str, float], new_values: Dict[str, float]) -> Dict:
    """
    Compara duas versões de uma série e detecta mudanças.
    
    Retorna:
    - added: novos períodos
    - removed: períodos removidos
    - changed: períodos com valores alterados
    - unchanged: períodos sem mudança
    """
    old_dates = set(old_values.keys())
    new_dates = set(new_values.keys())
    
    added = new_dates - old_dates
    removed = old_dates - new_dates
    common = old_dates & new_dates
    
    changed = []
    unchanged = []
    
    for date in common:
        old_val = old_values[date]
        new_val = new_values[date]
        
        # Comparar com tolerância para floating point
        if not math.isclose(old_val, new_val, rel_tol=1e-9):
            changed.append({
                'date': date,
                'old_value': old_val,
                'new_value': new_val,
                'change': new_val - old_val,
                'change_pct': ((new_val - old_val) / old_val * 100) if old_val != 0 else None
            })
        else:
            unchanged.append(date)
    
    return {
        'added': sorted(added),
        'removed': sorted(removed),
        'changed': changed,
        'unchanged_count': len(unchanged),
        'total_changes': len(added) + len(removed) + len(changed)
    }


def save_vintage(px_code: str, values: Dict[str, float], metadata: Dict) -> None:
    """Salva uma versão (vintage) da série no histórico."""
    timestamp = datetime.now().isoformat()
    vintage_key = f"vintage_{timestamp.replace(':', '-').replace('.', '-')}"
    
    ref = get_reference(f"flat_series/{px_code}/vintages/{vintage_key}")
    ref.set({
        'timestamp': timestamp,
        'values': values,
        'metadata_snapshot': metadata,
    })


def ingest_record_with_vintages(record: Dict, dry_run: bool = False) -> Tuple[bool, Optional[str]]:
    """
    Ingere série completa e compara com versão anterior para detectar revisões.
    """
    api_url = record["api_url"]
    px_code = record["px_code"]
    components = parse_sidra_components(api_url)
    
    # Buscar série completa da API
    try:
        new_values, row_meta = fetch_series_data(api_url)
    except Exception as exc:
        return False, f"SIDRA fetch failed: {exc}"
    
    if not new_values:
        return False, "No data returned from SIDRA"
    
    # Buscar versão anterior do Firebase
    ref = get_reference(f"flat_series/{px_code}")
    existing_data = ref.get() or {}
    old_values = existing_data.get('values', {})
    
    # Comparar vintages
    comparison = None
    has_changes = False
    
    if old_values:
        comparison = compare_series(old_values, new_values)
        has_changes = comparison['total_changes'] > 0
        
        if has_changes:
            # Salvar vintage anterior antes de atualizar
            if not dry_run:
                old_metadata = existing_data.get('metadata', {})
                save_vintage(px_code, old_values, old_metadata)
    
    # Preparar payload atualizado
    payload = {
        "metadata": {
            **existing_data.get('metadata', {}),
            "branch": record["branch"],
            "dataset": record["dataset"],
            "label": record["label"],
            "general_name": record["general_name"],
            "api_url": api_url,
            "px_code": px_code,
            "table": components["table"],
            "territory_level": components["territory_level"],
            "territory_id": components["territory_id"],
            "variable": components["variable"],
            "classifications": components["classifications"],
            "row_sample": row_meta,
            "last_updated": datetime.now().isoformat(),
            "vintage_comparison": comparison,
        },
        "values": new_values,
    }
    
    if dry_run:
        if comparison:
            msg = f"Dry-run: {len(new_values)} points, {comparison['total_changes']} changes detected"
            if comparison['changed']:
                msg += f" ({len(comparison['changed'])} revisions)"
        else:
            msg = f"Dry-run: {len(new_values)} points (first ingestion)"
        return True, msg
    
    # Atualizar Firebase
    ref.set(payload)
    
    # Mensagem de resultado
    if not old_values:
        return True, f"First ingestion: {len(new_values)} points"
    
    if has_changes:
        changes_summary = []
        if comparison['added']:
            changes_summary.append(f"+{len(comparison['added'])} new")
        if comparison['removed']:
            changes_summary.append(f"-{len(comparison['removed'])} removed")
        if comparison['changed']:
            changes_summary.append(f"~{len(comparison['changed'])} revised")
        
        return True, f"Updated: {', '.join(changes_summary)} (vintage saved)"
    else:
        return True, f"No changes: {len(new_values)} points (unchanged)"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest series with vintage detection and comparison."
    )
    parser.add_argument("--dataset", required=True, help="Dataset to ingest (e.g., ipca, inpc).")
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel workers.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write to Firebase.")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip series whose PX codes are already present.",
    )
    
    args = parser.parse_args()
    
    catalog = load_catalog()
    filtered = [r for r in catalog if r["dataset"] == args.dataset]
    
    if not filtered:
        print(f"[WARN] No series found for dataset '{args.dataset}'.")
        return
    
    print(f"[INFO] Ingesting '{args.dataset}' with vintage detection: {len(filtered)} series")
    print(f"[INFO] Full series will be fetched to detect revisions")
    print()
    
    existing_px: Set[str] = set()
    if args.resume:
        try:
            snapshot = get_reference("flat_series").get(shallow=True)
            if isinstance(snapshot, dict):
                existing_px = set(snapshot.keys())
            print(f"[INFO] Resume enabled; {len(existing_px)} PX codes already present will be updated.")
        except Exception as exc:
            print(f"[WARN] Unable to fetch existing PX codes: {exc}")
    
    to_process = []
    skipped = 0
    for record in filtered:
        if args.resume and record["px_code"] in existing_px:
            to_process.append(record)  # Incluir para atualizar e comparar vintages
        elif not args.resume:
            to_process.append(record)
        else:
            skipped += 1
    
    if skipped > 0:
        print(f"[INFO] Skipping {skipped} series not yet ingested.")
    
    successes = 0
    failures = 0
    with_changes = 0
    no_changes = 0
    first_ingestion = 0
    failed_records = []
    print_lock = Lock()
    
    def process_record(idx_record):
        idx, record = idx_record
        try:
            ok, message = ingest_record_with_vintages(record, dry_run=args.dry_run)
            status = "OK" if ok else "FAIL"
            
            # Categorizar resultado
            result_type = "unknown"
            if ok:
                if "First ingestion" in message:
                    result_type = "first"
                elif "revised" in message or "changed" in message.lower():
                    result_type = "changed"
                elif "No changes" in message:
                    result_type = "unchanged"
            
            with print_lock:
                print(f"[{status}] {idx}/{len(to_process)} {record['px_code']} | {message}")
            
            if not ok:
                failed_records.append({
                    "px_code": record["px_code"],
                    "label": record["label"],
                    "general_name": record["general_name"],
                    "api_url": record["api_url"],
                    "error": message
                })
            
            return ok, result_type
        except Exception as e:
            error_msg = str(e)
            with print_lock:
                print(f"[FAIL] {idx}/{len(to_process)} {record['px_code']} | Exception: {error_msg}")
            failed_records.append({
                "px_code": record["px_code"],
                "label": record["label"],
                "general_name": record["general_name"],
                "api_url": record["api_url"],
                "error": error_msg
            })
            return False, "error"
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_record, (idx, rec)): rec
            for idx, rec in enumerate(to_process, start=1)
        }
        
        for future in as_completed(futures):
            ok, result_type = future.result()
            if ok:
                successes += 1
                if result_type == "first":
                    first_ingestion += 1
                elif result_type == "changed":
                    with_changes += 1
                elif result_type == "unchanged":
                    no_changes += 1
            else:
                failures += 1
    
    # Log failed records
    if failed_records:
        log_file = REPO_ROOT / "data_exports" / f"failed_ingestion_{args.dataset}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with log_file.open("w", encoding="utf-8") as f:
            json.dump({
                "dataset": args.dataset,
                "timestamp": datetime.now().isoformat(),
                "total_failed": len(failed_records),
                "failed_records": failed_records
            }, f, indent=2, ensure_ascii=False)
        print(f"\n[WARN] {len(failed_records)} series failed. Logged to: {log_file}")
    
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total series:        {len(to_process)}")
    print(f"Success:             {successes}")
    print(f"  - First ingestion: {first_ingestion}")
    print(f"  - With revisions:  {with_changes}")
    print(f"  - No changes:      {no_changes}")
    print(f"Failed:              {failures}")
    print()
    if with_changes > 0:
        print(f"📊 {with_changes} series had revisions - vintages saved for analysis")


if __name__ == "__main__":
    main()

