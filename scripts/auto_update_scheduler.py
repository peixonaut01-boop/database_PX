#!/usr/bin/env python3
"""
Sistema inteligente de atualização automática de séries.

Este script gerencia atualizações automáticas baseadas na frequência
de publicação de cada dataset, priorizando séries críticas.
"""

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

try:
    from api.firebase_client import get_reference
    from scripts.ingest_flat_series import ingest_record, load_catalog
except ImportError as exc:
    raise SystemExit(f"Unable to import required modules: {exc}") from exc

# Configuração de frequências de atualização por dataset
UPDATE_SCHEDULE = {
    # IBGE - Mensais (atualizar no dia 15-20 de cada mês, após publicação)
    'ipca': {
        'frequency': 'monthly',
        'day_of_month': [15, 16, 17, 18, 19, 20],  # IBGE publica IPCA no dia 10-12
        'priority': 'high',
        'retry_days': [21, 22, 23],  # Retry se falhar
    },
    'inpc': {
        'frequency': 'monthly',
        'day_of_month': [15, 16, 17, 18, 19, 20],
        'priority': 'high',
        'retry_days': [21, 22, 23],
    },
    'ipca15': {
        'frequency': 'monthly',
        'day_of_month': [20, 21, 22, 23, 24, 25],  # Publicado no dia 15-17
        'priority': 'high',
        'retry_days': [26, 27, 28],
    },
    'ipp': {
        'frequency': 'monthly',
        'day_of_month': [15, 16, 17, 18, 19, 20],
        'priority': 'medium',
        'retry_days': [21, 22, 23],
    },
    'pimpf': {
        'frequency': 'monthly',
        'day_of_month': [5, 6, 7, 8, 9, 10],  # Publicado no início do mês
        'priority': 'medium',
        'retry_days': [11, 12, 13],
    },
    'pmc': {
        'frequency': 'monthly',
        'day_of_month': [15, 16, 17, 18, 19, 20],
        'priority': 'medium',
        'retry_days': [21, 22, 23],
    },
    'pms': {
        'frequency': 'monthly',
        'day_of_month': [15, 16, 17, 18, 19, 20],
        'priority': 'medium',
        'retry_days': [21, 22, 23],
    },
    'lspa': {
        'frequency': 'monthly',
        'day_of_month': [10, 11, 12, 13, 14, 15],  # Publicado no início do mês
        'priority': 'low',  # Muitas séries bloqueadas
        'retry_days': [16, 17, 18],
    },
    
    # IBGE - Trimestrais (atualizar após publicação trimestral)
    'pnadct': {
        'frequency': 'quarterly',
        'months': [2, 5, 8, 11],  # Fevereiro, Maio, Agosto, Novembro (após publicação)
        'day_of_month': [15, 16, 17, 18, 19, 20],
        'priority': 'high',
        'retry_days': [21, 22, 23, 24, 25],
    },
    'pnadcm': {
        'frequency': 'monthly',  # PNADCM é mensal (rolling quarter)
        'day_of_month': [15, 16, 17, 18, 19, 20],
        'priority': 'high',
        'retry_days': [21, 22, 23],
    },
    
    # STN - Mensal (atualizar após publicação do RTN)
    'stn': {
        'frequency': 'monthly',
        'day_of_month': [1, 2, 3, 4, 5, 6, 7],  # RTN publicado no final do mês anterior
        'priority': 'high',
        'retry_days': [8, 9, 10],
    },
    
    # BACEN - Frequências variadas
    'bacen': {
        'frequency': 'daily',  # Muitas séries são diárias
        'time': '09:00',  # Atualizar diariamente às 9h
        'priority': 'high',
        'weekly_retry': True,  # Retry semanal para séries semanais
    },
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(REPO_ROOT / 'logs' / 'auto_update.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def should_update_dataset(dataset: str, today: Optional[datetime] = None) -> bool:
    """Verifica se um dataset deve ser atualizado hoje."""
    if today is None:
        today = datetime.now()
    
    schedule = UPDATE_SCHEDULE.get(dataset)
    if not schedule:
        logger.warning(f"No schedule configured for dataset: {dataset}")
        return False
    
    freq = schedule.get('frequency')
    
    if freq == 'daily':
        return True  # Sempre atualizar séries diárias
    
    elif freq == 'monthly':
        day_of_month = schedule.get('day_of_month', [])
        if today.day in day_of_month:
            return True
        
        # Verificar retry days
        retry_days = schedule.get('retry_days', [])
        if today.day in retry_days:
            # Verificar se última atualização falhou
            return check_last_update_failed(dataset)
        
        return False
    
    elif freq == 'quarterly':
        months = schedule.get('months', [])
        day_of_month = schedule.get('day_of_month', [])
        if today.month in months and today.day in day_of_month:
            return True
        
        retry_days = schedule.get('retry_days', [])
        if today.month in months and today.day in retry_days:
            return check_last_update_failed(dataset)
        
        return False
    
    return False


def check_last_update_failed(dataset: str) -> bool:
    """Verifica se a última atualização falhou."""
    # Verificar logs de falhas recentes
    failed_logs = list((REPO_ROOT / 'data_exports').glob(f'failed_ingestion_{dataset}_*.json'))
    if not failed_logs:
        return False
    
    # Pegar o log mais recente
    latest_log = max(failed_logs, key=lambda p: p.stat().st_mtime)
    
    # Verificar se é recente (últimos 7 dias)
    log_time = datetime.fromtimestamp(latest_log.stat().st_mtime)
    if (datetime.now() - log_time).days > 7:
        return False
    
    # Verificar se há falhas
    try:
        with open(latest_log, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if data.get('total_failed', 0) > 0:
                return True
    except Exception as e:
        logger.error(f"Error checking failed log: {e}")
    
    return False


def get_datasets_to_update() -> List[str]:
    """Retorna lista de datasets que devem ser atualizados hoje."""
    datasets = []
    for dataset in UPDATE_SCHEDULE.keys():
        if should_update_dataset(dataset):
            datasets.append(dataset)
    
    # Ordenar por prioridade
    priority_order = {'high': 0, 'medium': 1, 'low': 2}
    datasets.sort(key=lambda d: priority_order.get(UPDATE_SCHEDULE[d].get('priority', 'low'), 2))
    
    return datasets


def update_dataset(dataset: str) -> bool:
    """Atualiza um dataset específico."""
    logger.info(f"Starting update for dataset: {dataset}")
    
    try:
        # Carregar catálogo
        catalog = load_catalog()
        dataset_series = [r for r in catalog if r.get('dataset') == dataset]
        
        if not dataset_series:
            logger.warning(f"No series found for dataset: {dataset}")
            return False
        
        logger.info(f"Found {len(dataset_series)} series for {dataset}")
        
        # Verificar quais séries precisam atualização
        # (comparar última data no Firebase com última data disponível na API)
        # Por enquanto, vamos atualizar todas
        
        # Usar versão INCREMENTAL - muito mais rápida!
        # Busca apenas novos dados desde última atualização
        from scripts.ingest_flat_series_incremental import main as incremental_main
        import sys as sys_module
        
        # Simular argumentos
        original_argv = sys_module.argv
        sys_module.argv = ['ingest_flat_series_incremental.py', '--dataset', dataset, '--workers', '10']
        
        try:
            incremental_main()
            logger.info(f"Successfully updated dataset: {dataset} (incremental)")
            return True
        except Exception as e:
            logger.warning(f"Incremental update failed, trying full update: {e}")
            # Fallback para versão completa se incremental falhar
            from scripts.ingest_flat_series import main as full_main
            sys_module.argv = ['ingest_flat_series.py', '--dataset', dataset, '--resume', '--workers', '10']
            try:
                full_main()
                logger.info(f"Successfully updated dataset: {dataset} (full)")
                return True
            except Exception as e2:
                logger.error(f"Full update also failed: {e2}")
                return False
        finally:
            sys_module.argv = original_argv
        
    except Exception as e:
        logger.error(f"Error updating dataset {dataset}: {e}", exc_info=True)
        return False


def main():
    """Função principal do scheduler."""
    logger.info("=" * 70)
    logger.info("Auto Update Scheduler - Starting")
    logger.info("=" * 70)
    
    datasets_to_update = get_datasets_to_update()
    
    if not datasets_to_update:
        logger.info("No datasets scheduled for update today")
        return
    
    logger.info(f"Datasets to update today: {', '.join(datasets_to_update)}")
    
    results = {}
    for dataset in datasets_to_update:
        success = update_dataset(dataset)
        results[dataset] = success
    
    # Resumo
    logger.info("=" * 70)
    logger.info("Update Summary")
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
            'datasets_updated': datasets_to_update,
            'results': results,
            'success_count': sum(1 for v in results.values() if v),
            'failure_count': sum(1 for v in results.values() if not v),
        }, f, indent=2)
    
    logger.info(f"Results saved to: {results_file}")


if __name__ == '__main__':
    main()

