#!/usr/bin/env python3
"""
Sistema inteligente de atualização baseado no calendário IBGE.

- Verifica calendário semanalmente
- Detecta divulgações da semana
- Atualiza datasets correspondentes
- Prioriza dados nacionais (territory_level=1) antes dos regionais
"""

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

try:
    from api.firebase_client import get_reference
    from scripts.ingest_flat_series import load_catalog
except ImportError as exc:
    raise SystemExit(f"Unable to import required modules: {exc}") from exc

CALENDAR_API = "https://servicodados.ibge.gov.br/api/v3/calendario/"

# Mapeamento de palavras-chave do calendário para datasets
# Ordem importa: datasets mais específicos primeiro
DATASET_KEYWORDS = {
    'ipca15': ['IPCA-15', 'IPCA 15', 'Índice Nacional de Preços ao Consumidor Amplo 15'],
    'ipca': ['IPCA', 'Índice Nacional de Preços ao Consumidor Amplo'],
    'inpc': ['INPC', 'Índice Nacional de Preços ao Consumidor'],
    'ipp': ['IPP', 'Índice de Preços ao Produtor'],
    'pimpf': ['PIM-PF', 'PIM', 'Produção Industrial', 'Produção Física'],
    'pmc': ['PMC', 'Pesquisa Mensal de Comércio'],
    'pms': ['PMS', 'Pesquisa Mensal de Serviços'],
    'pnadct': ['PNAD Contínua Trimestral', 'pnadc2', 'Divulgação trimestral'],
    'pnadcm': ['PNAD Contínua Mensal', 'pnadc1', 'Divulgação mensal'],
    'lspa': ['LSPA', 'Levantamento Sistemático da Produção Agrícola'],
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(REPO_ROOT / 'logs' / 'smart_update.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def fetch_calendar_week() -> List[Dict]:
    """Busca eventos do calendário IBGE para a semana atual."""
    try:
        response = requests.get(CALENDAR_API, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # API retorna lista diretamente ou objeto com 'items'
        items = data if isinstance(data, list) else (data.get('items', []) if isinstance(data, dict) else [])
        
        # Filtrar eventos da semana atual
        today = datetime.now()
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=7)
        
        week_events = []
        for event in items:
            date_str = event.get('data_divulgacao', '')
            if not date_str:
                continue
            
            try:
                event_date = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
                if week_start <= event_date <= week_end:
                    week_events.append(event)
            except Exception as e:
                logger.debug(f"Could not parse date '{date_str}': {e}")
                continue
        
        return week_events
    except Exception as e:
        logger.error(f"Error fetching calendar: {e}")
        return []


def detect_dataset_from_event(event: Dict) -> Optional[str]:
    """
    Detecta qual dataset corresponde a um evento do calendário.
    Verifica datasets na ordem definida (mais específicos primeiro).
    """
    title = event.get('titulo', '').upper()
    produto = event.get('nome_produto', '').upper()
    desc = event.get('descricao', '').upper()
    desc_produto = event.get('descricao_produto', '').upper()
    
    text = f"{title} {produto} {desc} {desc_produto}"
    
    # Verificar cada dataset na ordem (mais específicos primeiro)
    for dataset, keywords in DATASET_KEYWORDS.items():
        if any(kw.upper() in text for kw in keywords):
            return dataset
    
    return None


def get_releases_this_week() -> Dict[str, List[Dict]]:
    """
    Retorna divulgações da semana agrupadas por dataset.
    
    Returns:
        {
            'ipca': [event1, event2, ...],
            'inpc': [event1, ...],
            ...
        }
    """
    events = fetch_calendar_week()
    releases = {}
    
    for event in events:
        dataset = detect_dataset_from_event(event)
        if dataset:
            if dataset not in releases:
                releases[dataset] = []
            releases[dataset].append(event)
    
    return releases


def filter_national_series(catalog: List[Dict], dataset: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Separa séries nacionais (prioridade) das regionais.
    
    Returns:
        (national_series, regional_series)
    """
    dataset_series = [r for r in catalog if r.get('dataset') == dataset]
    
    national = []
    regional = []
    
    for record in dataset_series:
        api_url = record.get('api_url', '')
        # Séries nacionais têm /n1/1/ (territory_level=1, territory_id=1 = Brasil)
        if '/n1/1/' in api_url:
            national.append(record)
        else:
            regional.append(record)
    
    return national, regional


def update_dataset_prioritized(dataset: str, national_first: bool = True, workers: int = 10) -> bool:
    """
    Atualiza dataset priorizando séries nacionais.
    Usa processamento paralelo para velocidade.
    
    Args:
        dataset: Nome do dataset
        national_first: Se True, atualiza nacionais primeiro, depois regionais
        workers: Número de workers paralelos
    """
    logger.info(f"Starting update for dataset: {dataset}")
    
    try:
        catalog = load_catalog()
        national, regional = filter_national_series(catalog, dataset)
        
        logger.info(f"  National series: {len(national)}")
        logger.info(f"  Regional series: {len(regional)}")
        
        from scripts.ingest_flat_series import ingest_record
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from threading import Lock
        
        print_lock = Lock()
        total_updated = 0
        total_failed = 0
        failed_records = []
        
        def process_record(record, phase_name):
            nonlocal total_updated, total_failed
            try:
                ok, msg = ingest_record(record, dry_run=False)
                with print_lock:
                    if ok:
                        total_updated += 1
                        logger.debug(f"    [{phase_name}] {record['px_code']} - OK")
                    else:
                        total_failed += 1
                        failed_records.append({
                            'px_code': record['px_code'],
                            'error': msg
                        })
                        logger.warning(f"    [{phase_name}] {record['px_code']} - {msg}")
                return ok
            except Exception as e:
                with print_lock:
                    total_failed += 1
                    failed_records.append({
                        'px_code': record.get('px_code', 'unknown'),
                        'error': str(e)
                    })
                    logger.error(f"    [{phase_name}] {record.get('px_code', 'unknown')} - {e}")
                return False
        
        # Fase 1: Atualizar séries nacionais (prioridade) com paralelismo
        if national_first and national:
            logger.info(f"  Phase 1: Updating {len(national)} NATIONAL series (workers={workers})...")
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(process_record, rec, "NATIONAL"): rec 
                          for rec in national}
                for future in as_completed(futures):
                    future.result()  # Aguardar conclusão
        
        # Fase 2: Atualizar séries regionais com paralelismo
        if regional:
            logger.info(f"  Phase 2: Updating {len(regional)} REGIONAL series (workers={workers})...")
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(process_record, rec, "REGIONAL"): rec 
                          for rec in regional}
                for future in as_completed(futures):
                    future.result()
        
        logger.info(f"  ✓ Updated: {total_updated}, Failed: {total_failed}")
        
        # Salvar falhas se houver
        if failed_records:
            failed_file = REPO_ROOT / 'data_exports' / f'failed_update_{dataset}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            with open(failed_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'dataset': dataset,
                    'timestamp': datetime.now().isoformat(),
                    'total_failed': len(failed_records),
                    'failed_records': failed_records
                }, f, indent=2, ensure_ascii=False)
            logger.warning(f"  Failed records saved to: {failed_file}")
        
        return total_failed == 0
        
    except Exception as e:
        logger.error(f"Error updating dataset {dataset}: {e}", exc_info=True)
        return False


def main():
    """Função principal: verifica calendário e atualiza datasets necessários."""
    logger.info("=" * 70)
    logger.info("Smart Update Scheduler - Starting")
    logger.info("=" * 70)
    
    # Buscar divulgações da semana
    logger.info("Fetching calendar for this week...")
    releases = get_releases_this_week()
    
    if not releases:
        logger.info("No releases detected for this week. Nothing to update.")
        return
    
    logger.info(f"Found releases for {len(releases)} datasets:")
    for dataset, events in releases.items():
        logger.info(f"  {dataset.upper()}: {len(events)} event(s)")
        for event in events:
            logger.info(f"    - {event.get('titulo', 'N/A')} on {event.get('data_divulgacao', 'N/A')}")
    
    logger.info("")
    logger.info("Starting updates (NATIONAL series first, then REGIONAL)...")
    logger.info("")
    
    # Atualizar cada dataset detectado (com priorização nacional)
    results = {}
    for dataset in releases.keys():
        success = update_dataset_prioritized(dataset, national_first=True, workers=10)
        results[dataset] = success
    
    # Resumo
    logger.info("")
    logger.info("=" * 70)
    logger.info("UPDATE SUMMARY")
    logger.info("=" * 70)
    for dataset, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{dataset.upper():15s} - {status}")
    
    # Salvar resultados
    results_file = REPO_ROOT / 'data_exports' / f'update_results_{datetime.now().strftime("%Y%m%d")}.json'
    results_file.parent.mkdir(parents=True, exist_ok=True)
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump({
            'date': datetime.now().isoformat(),
            'releases_detected': {k: len(v) for k, v in releases.items()},
            'results': results,
        }, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\nResults saved to: {results_file}")


if __name__ == '__main__':
    main()

