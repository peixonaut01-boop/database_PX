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
import os
import smtplib
import sys
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
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

# Email configuration
EMAIL_RECIPIENTS = [
    "lucasgmartins04@gmail.com",
    "macro@vitorvidalconsulting.com"
]  # Lista de destinatários
EMAIL_FROM = os.getenv("EMAIL_FROM", "peixonaut01@gmail.com")
EMAIL_SMTP_HOST = os.getenv("EMAIL_SMTP_HOST", "smtp.gmail.com")
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")

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


def create_email_html(releases: Dict[str, List[Dict]], results: Optional[Dict[str, bool]] = None) -> str:
    """Cria template HTML para o email do calendário."""
    today = datetime.now()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=7)
    
    week_str = f"{week_start.strftime('%d/%m/%Y')} a {week_end.strftime('%d/%m/%Y')}"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px 10px 0 0; }}
            .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 10px 10px; }}
            .dataset {{ background: white; margin: 15px 0; padding: 20px; border-radius: 8px; border-left: 4px solid #667eea; }}
            .dataset h3 {{ margin-top: 0; color: #667eea; }}
            .event {{ margin: 10px 0; padding: 10px; background: #f0f0f0; border-radius: 5px; }}
            .event-date {{ color: #666; font-size: 0.9em; }}
            .status {{ display: inline-block; padding: 5px 15px; border-radius: 20px; font-size: 0.85em; font-weight: bold; }}
            .status-success {{ background: #d4edda; color: #155724; }}
            .status-failed {{ background: #f8d7da; color: #721c24; }}
            .status-pending {{ background: #fff3cd; color: #856404; }}
            .summary {{ background: white; padding: 20px; margin: 20px 0; border-radius: 8px; }}
            .footer {{ text-align: center; margin-top: 30px; color: #666; font-size: 0.9em; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📅 Calendário de Atualizações IBGE</h1>
                <p>Semana: {week_str}</p>
            </div>
            <div class="content">
                <h2>Divulgações Detectadas</h2>
    """
    
    for dataset, events in releases.items():
        status_html = ""
        if results and dataset in results:
            status = "status-success" if results[dataset] else "status-failed"
            status_text = "✓ Atualizado" if results[dataset] else "✗ Falhou"
            status_html = f'<span class="status {status}">{status_text}</span>'
        else:
            status_html = '<span class="status status-pending">⏳ Pendente</span>'
        
        html += f"""
                <div class="dataset">
                    <h3>{dataset.upper()} {status_html}</h3>
        """
        
        for event in events:
            titulo = event.get('titulo', 'N/A')
            data = event.get('data_divulgacao', 'N/A')
            html += f"""
                    <div class="event">
                        <strong>{titulo}</strong>
                        <div class="event-date">📆 {data}</div>
                    </div>
            """
        
        html += "</div>"
    
    html += """
                <div class="summary">
                    <h3>📊 Resumo</h3>
                    <p><strong>Total de datasets:</strong> """ + str(len(releases)) + """</p>
                    <p><strong>Total de eventos:</strong> """ + str(sum(len(v) for v in releases.values())) + """</p>
    """
    
    if results:
        success_count = sum(1 for v in results.values() if v)
        html += f"""
                    <p><strong>Atualizações bem-sucedidas:</strong> {success_count}/{len(results)}</p>
        """
    
    html += """
                </div>
            </div>
            <div class="footer">
                <p>Este é um email automático do sistema de atualização Peixonaut.</p>
                <p>Gerado em """ + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + """</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html


def send_calendar_email(releases: Dict[str, List[Dict]], results: Optional[Dict[str, bool]] = None) -> bool:
    """Envia email com o calendário de atualizações."""
    if not EMAIL_USER or not EMAIL_PASSWORD:
        logger.warning("Email credentials not configured. Skipping email send.")
        return False
    
    try:
        # Criar mensagem
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"📅 Calendário IBGE - Semana {datetime.now().strftime('%d/%m/%Y')}"
        msg['From'] = EMAIL_FROM
        msg['To'] = ", ".join(EMAIL_RECIPIENTS)
        
        # Criar conteúdo HTML
        html_content = create_email_html(releases, results)
        html_part = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(html_part)
        
        # Enviar email
        with smtplib.SMTP(EMAIL_SMTP_HOST, EMAIL_SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)
        
        logger.info(f"Email sent successfully to {', '.join(EMAIL_RECIPIENTS)}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send email: {e}", exc_info=True)
        return False


def main():
    """Função principal: verifica calendário e atualiza datasets necessários."""
    logger.info("=" * 70)
    logger.info("Smart Update Scheduler - Starting")
    logger.info("=" * 70)
    
    # Buscar divulgações da semana
    logger.info("Fetching calendar for this week...")
    releases = get_releases_this_week()
    
    # Enviar email mesmo se não houver releases (para informar)
    if not releases:
        logger.info("No releases detected for this week. Nothing to update.")
        send_calendar_email({}, None)  # Enviar email informando que não há releases
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
    
    # Enviar email com resumo
    logger.info("")
    logger.info("Sending calendar email...")
    send_calendar_email(releases, results)


if __name__ == '__main__':
    main()

