#!/usr/bin/env python3
"""
Versão otimizada de ingestão que busca apenas novos dados desde a última atualização.

Esta versão é MUITO mais rápida porque:
1. Verifica a última data no Firebase
2. Busca apenas períodos novos desde essa data
3. Mescla com dados existentes
4. Evita re-baixar toda a série histórica
"""

from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple
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
    from scripts.ingest_flat_series import parse_sidra_components, parse_period, load_catalog
except ImportError as exc:
    raise SystemExit(f"Unable to import required modules: {exc}") from exc


def get_last_date_from_firebase(px_code: str) -> Optional[str]:
    """Retorna a última data (mais recente) de uma série no Firebase."""
    try:
        ref = get_reference(f"flat_series/{px_code}")
        data = ref.get()
        if not data or 'values' not in data:
            return None
        
        values = data.get('values', {})
        if not values:
            return None
        
        # Retornar a data mais recente (última chave ordenada)
        dates = sorted(values.keys())
        return dates[-1] if dates else None
    except Exception as e:
        print(f"Warning: Could not get last date for {px_code}: {e}")
        return None


def build_incremental_api_url(base_url: str, last_date: Optional[str]) -> str:
    """
    Modifica a URL da API SIDRA para buscar apenas períodos após last_date.
    
    A API SIDRA aceita parâmetro 'p' que pode ser:
    - 'all' - todos os períodos
    - 'lastN' - últimos N períodos
    - 'YYYYMM-YYYYMM' - range de períodos
    - 'YYYYMM' - período específico
    
    Vamos tentar construir um range a partir da última data.
    """
    if not last_date:
        return base_url  # Sem otimização, busca tudo
    
    try:
        # Converter last_date (YYYY-MM-DD) para formato SIDRA
        last_dt = datetime.strptime(last_date, "%Y-%m-%d")
        
        # Calcular próximo período
        # Para séries mensais: próximo mês
        # Para trimestrais: próximo trimestre
        # Para anuais: próximo ano
        
        # Por enquanto, vamos buscar a partir do próximo mês
        # (isso pode ser refinado baseado no tipo de série)
        next_month = last_dt + timedelta(days=32)  # Garantir próximo mês
        next_month = next_month.replace(day=1)
        
        # Formato SIDRA: YYYYMM
        start_period = next_month.strftime("%Y%m")
        
        # Buscar até o período atual (ou próximo ano para garantir)
        end_period = datetime.now().strftime("%Y%m")
        
        # Modificar URL: substituir 'p/all' por 'p/YYYYMM-YYYYMM'
        if '/p/all' in base_url:
            new_url = base_url.replace('/p/all', f'/p/{start_period}-{end_period}')
            return new_url
        elif '/p/' in base_url:
            # Já tem um parâmetro p, substituir
            pattern = r'/p/[^/]+'
            new_url = re.sub(pattern, f'/p/{start_period}-{end_period}', base_url)
            return new_url
        
        return base_url
    except Exception as e:
        print(f"Warning: Could not build incremental URL: {e}")
        return base_url  # Fallback para URL original


def fetch_incremental_series_data(api_url: str, last_date: Optional[str]) -> Tuple[Dict[str, float], Dict]:
    """
    Busca dados da série, mas apenas períodos novos desde last_date.
    Retorna apenas os novos valores.
    """
    # Construir URL incremental
    incremental_url = build_incremental_api_url(api_url, last_date)
    
    response = requests.get(incremental_url, timeout=120)
    response.raise_for_status()
    data = response.json()
    
    if len(data) < 2:
        raise ValueError("No data returned from SIDRA.")
    
    header = data[0]
    period_key = None
    for key, label in header.items():
        if not isinstance(label, str):
            continue
        label_lower = label.lower()
        if any(word in label_lower for word in ("mês", "mes", "trimestre", "ano", "período", "periodo")):
            period_key = key
            break
    
    if not period_key:
        raise ValueError("Unable to determine period column from SIDRA response.")
    
    name_key = period_key.replace("C", "N", 1) if period_key.startswith("D") else None
    new_values: Dict[str, float] = {}
    
    # Se temos last_date, filtrar apenas períodos após essa data
    last_dt = None
    if last_date:
        try:
            last_dt = datetime.strptime(last_date, "%Y-%m-%d")
        except:
            pass
    
    for row in data[1:]:
        raw_value = row.get("V")
        if raw_value in (None, "", "..", "..."):
            continue
        try:
            numeric_value = float(raw_value)
            if math.isnan(numeric_value):
                continue
        except (TypeError, ValueError):
            continue
        
        date_code = row.get(period_key)
        date_name = row.get(name_key) if name_key else None
        
        normalized_period = parse_period(date_code, date_name)
        if not normalized_period:
            continue
        
        # Filtrar: apenas períodos após last_date
        if last_dt:
            try:
                period_dt = datetime.strptime(normalized_period, "%Y-%m-%d")
                if period_dt <= last_dt:
                    continue  # Pular dados antigos
            except:
                pass  # Se não conseguir parsear, incluir
        
        new_values[normalized_period] = numeric_value
    
    if not new_values:
        # Se não há novos valores, pode ser que não há dados novos ou a série não atualizou
        # Retornar dict vazio (não é erro, apenas não há novos dados)
        pass
    
    # Metadata (usar primeira linha)
    metadata_snapshot = {
        "territory_code": data[1].get("D1C") if len(data) > 1 else None,
        "territory_name": data[1].get("D1N") if len(data) > 1 else None,
        "variable_code": data[1].get("D2C") if len(data) > 1 else None,
        "variable_name": data[1].get("D2N") if len(data) > 1 else None,
    }
    
    return new_values, metadata_snapshot


def update_record_incremental(record: Dict, dry_run: bool = False) -> Tuple[bool, Optional[str]]:
    """
    Atualiza uma série de forma incremental: busca apenas novos dados.
    """
    px_code = record["px_code"]
    api_url = record["api_url"]
    
    # Verificar última data no Firebase
    last_date = get_last_date_from_firebase(px_code)
    
    if last_date:
        print(f"  → Last update: {last_date}, fetching only new data...")
    else:
        print(f"  → No existing data, fetching all...")
    
    try:
        new_values, row_meta = fetch_incremental_series_data(api_url, last_date)
    except Exception as exc:
        return False, f"SIDRA fetch failed: {exc}"
    
    if not new_values:
        return True, f"No new data (last: {last_date or 'N/A'})"
    
    if dry_run:
        return True, f"Dry-run: {len(new_values)} new points since {last_date or 'beginning'}"
    
    # Buscar dados existentes do Firebase
    ref = get_reference(f"flat_series/{px_code}")
    existing_data = ref.get() or {}
    existing_values = existing_data.get('values', {})
    
    # Mesclar: novos valores sobrescrevem existentes (caso haja overlap)
    merged_values = {**existing_values, **new_values}
    
    # Atualizar payload
    components = parse_sidra_components(api_url)
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
        },
        "values": merged_values,
    }
    
    ref.set(payload)
    return True, f"Updated: +{len(new_values)} new points (total: {len(merged_values)})"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Incremental update: fetch only new data since last update."
    )
    parser.add_argument("--dataset", required=True, help="Dataset to update (e.g., ipca, inpc).")
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel workers.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write to Firebase.")
    
    args = parser.parse_args()
    
    catalog = load_catalog()
    filtered = [r for r in catalog if r["dataset"] == args.dataset]
    
    if not filtered:
        print(f"[WARN] No series found for dataset '{args.dataset}'.")
        return
    
    print(f"[INFO] Incremental update for '{args.dataset}': {len(filtered)} series")
    print(f"[INFO] This will be FAST - only fetching new data since last update!")
    print()
    
    successes = 0
    failures = 0
    no_updates = 0
    print_lock = Lock()
    
    def process_record(idx_record):
        idx, record = idx_record
        try:
            ok, message = update_record_incremental(record, dry_run=args.dry_run)
            status = "OK" if ok else "FAIL"
            with print_lock:
                print(f"[{status}] {idx}/{len(filtered)} {record['px_code']} | {message}")
            
            if ok:
                if "No new data" in message:
                    return "no_update"
                return "success"
            return "failure"
        except Exception as e:
            with print_lock:
                print(f"[FAIL] {idx}/{len(filtered)} {record['px_code']} | Exception: {e}")
            return "failure"
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_record, (idx, rec)): rec
            for idx, rec in enumerate(filtered, start=1)
        }
        
        for future in as_completed(futures):
            result = future.result()
            if result == "success":
                successes += 1
            elif result == "no_update":
                no_updates += 1
            else:
                failures += 1
    
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total series:     {len(filtered)}")
    print(f"Updated:          {successes}")
    print(f"No new data:      {no_updates}")
    print(f"Failed:           {failures}")
    print()
    print(f"⚡ Incremental update completed! Much faster than full re-ingestion.")


if __name__ == "__main__":
    main()

