"""Generate a status summary covering GitHub workflows and Firebase data."""
from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml

FIREBASE_BASE_URL = 'https://peixo-28d2d-default-rtdb.firebaseio.com'
REPO_OWNER = 'guithom04'
REPO_NAME = 'database_PX'
REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_FILES = [
    '.github/workflows/update_ibge_cnt.yaml',
    '.github/workflows/update_ibge_pms.yaml',
    '.github/workflows/test_single_table.yaml',
]
OUTPUT_FILE = REPO_ROOT / 'status_summary.txt'


def load_workflow_schedule(workflow_path: str) -> Dict[str, Any]:
    full_path = REPO_ROOT / workflow_path
    with open(full_path, 'r', encoding='utf-8') as fh:
        data = yaml.safe_load(fh)
    schedules = []
    triggers = data.get('on') or data.get(True) or {}
    # schedule can be list or dict
    if isinstance(triggers, dict) and 'schedule' in triggers:
        cron_entries = triggers['schedule']
        if isinstance(cron_entries, list):
            for entry in cron_entries:
                if isinstance(entry, dict) and 'cron' in entry:
                    schedules.append(entry['cron'])
        elif isinstance(cron_entries, dict) and 'cron' in cron_entries:
            schedules.append(cron_entries['cron'])
    manual = 'workflow_dispatch' in triggers
    return {
        'name': data.get('name', os.path.basename(workflow_path)),
        'schedules': schedules,
        'manual_trigger': manual,
    }


def fetch_latest_run(workflow_file: str) -> Optional[Dict[str, Any]]:
    api_url = (
        f'https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/workflows/'
        f'{Path(workflow_file).name}/runs?per_page=1'
    )
    response = requests.get(api_url, timeout=30)
    if response.status_code != 200:
        return None
    payload = response.json()
    runs = payload.get('workflow_runs', [])
    if not runs:
        return None
    run = runs[0]
    return {
        'status': run.get('status'),
        'conclusion': run.get('conclusion'),
        'event': run.get('event'),
        'run_started_at': run.get('run_started_at'),
        'updated_at': run.get('updated_at'),
        'html_url': run.get('html_url'),
    }


def firebase_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{FIREBASE_BASE_URL}/{path}.json"
    response = requests.get(url, params=params, timeout=30)
    if response.status_code != 200:
        return None
    try:
        return response.json()
    except requests.exceptions.JSONDecodeError:
        return None


def analyze_entries(entries: Any) -> Dict[str, Any]:
    info: Dict[str, Any] = {'records': None, 'last_period': None}
    if isinstance(entries, list):
        info['records'] = len(entries)
        if entries:
            last_entry = entries[-1]
            if isinstance(last_entry, dict):
                for key in ('Trimestre', 'Periodo', 'Período', 'periodo', 'period'):
                    if key in last_entry:
                        info['last_period'] = last_entry[key]
                        break
    return info


def build_branch_summary(base_path: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        'table_number': metadata.get('table_number'),
        'table_name': metadata.get('table_name'),
        'period_range': metadata.get('period_range'),
        'subbranch': metadata.get('subbranch'),
    }

    data = firebase_get(f'{base_path}/data')
    if data is not None:
        info = analyze_entries(data)
        summary.update(info)

    sheets = firebase_get(f'{base_path}/sheets')
    if isinstance(sheets, dict) and sheets:
        summary['sheet_count'] = len(sheets)
        sheet_rows = []
        for sheet_name, sheet_blob in sheets.items():
            entries = sheet_blob.get('data') if isinstance(sheet_blob, dict) else sheet_blob
            info = analyze_entries(entries)
            info['sheet'] = sheet_name
            sheet_rows.append(info)
        summary['sheets'] = sheet_rows
    elif metadata.get('sheet_count'):
        summary['sheet_count'] = metadata['sheet_count']
        summary['sheets'] = metadata.get('sheets', [])

    return summary


def summarize_table(category: str, table_key: str) -> Dict[str, Any]:
    base_path = f'ibge_data/{category}/{table_key}'
    metadata = firebase_get(f'{base_path}/metadata') or {}

    if metadata:
        summary = build_branch_summary(base_path, metadata)
        summary['table_key'] = table_key
        return summary

    # Handle tables that are comprised of sub-branches (e.g., PMS receita/volume)
    children = firebase_get(f'{base_path}', params={'shallow': 'true'})
    subbranches: List[Dict[str, Any]] = []
    if isinstance(children, dict):
        for child_key in sorted(children.keys()):
            if child_key == 'metadata':
                continue
            child_meta = firebase_get(f'{base_path}/{child_key}/metadata') or {}
            child_meta['subbranch'] = child_key
            branch_summary = build_branch_summary(f'{base_path}/{child_key}', child_meta)
            branch_summary['table_key'] = f'{table_key}/{child_key}'
            subbranches.append(branch_summary)
    return {
        'table_key': table_key,
        'subbranches': subbranches,
    }


def summarize_category(category: str) -> Dict[str, Any]:
    shallow = firebase_get(f'ibge_data/{category}', params={'shallow': 'true'})
    tables = sorted(shallow.keys()) if isinstance(shallow, dict) else []
    return {
        'category': category,
        'tables': [summarize_table(category, table) for table in tables],
    }


def human_datetime(iso_str: Optional[str]) -> str:
    if not iso_str:
        return 'N/A'
    try:
        dt_obj = dt.datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        return dt_obj.astimezone(dt.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    except Exception:
        return iso_str


def build_summary() -> str:
    lines: List[str] = []
    lines.append('=== Workflow Status ===')
    for workflow in WORKFLOW_FILES:
        info = load_workflow_schedule(workflow)
        run = fetch_latest_run(workflow)
        lines.append(f"Workflow: {info['name']} ({Path(workflow).name})")
        if info['schedules']:
            for cron in info['schedules']:
                lines.append(f"  Schedule: {cron}")
        else:
            lines.append('  Schedule: none')
        lines.append(f"  Manual trigger: {'yes' if info['manual_trigger'] else 'no'}")
        if run:
            lines.append(
                f"  Last run: status={run['status']} conclusion={run['conclusion']} event={run['event']}"
            )
            lines.append(f"            started {human_datetime(run['run_started_at'])}")
            lines.append(f"            updated {human_datetime(run['updated_at'])}")
            if run.get('html_url'):
                lines.append(f"            url: {run['html_url']}")
        else:
            lines.append('  Last run: unavailable')
        lines.append('')

    lines.append('=== Firebase Data Summary ===')
    for category in ('cnt', 'pms'):
        category_summary = summarize_category(category)
        lines.append(f"Category: {category.upper()}")
        if not category_summary['tables']:
            lines.append('  No tables found')
            continue
        for table in category_summary['tables']:
            if table.get('subbranches'):
                lines.append(f"  {table['table_key']}:")
                for sub in table['subbranches']:
                    lines.append(
                        f"    {sub['table_key']}: {sub.get('table_name', 'N/A')}"
                    )
                    if sub.get('period_range'):
                        lines.append(f"      period: {sub['period_range']}")
                    if sub.get('records') is not None:
                        lines.append(f"      records: {sub['records']}")
                    if sub.get('last_period'):
                        lines.append(f"      last entry: {sub['last_period']}")
                    if sub.get('sheet_count'):
                        lines.append(f"      sheets: {sub['sheet_count']}")
                        for sheet in sub.get('sheets', []):
                            lines.append(
                                f"        - {sheet.get('sheet')}: records={sheet.get('records')} last={sheet.get('last_period')}"
                            )
                continue

            lines.append(
                f"  {table['table_key']}: {table.get('table_name', 'N/A')}"
            )
            lines.append(f"    period: {table.get('period_range', 'N/A')}")
            if table.get('records') is not None:
                lines.append(f"    records: {table['records']}")
            if table.get('last_period'):
                lines.append(f"    last entry: {table['last_period']}")
            if table.get('sheet_count'):
                lines.append(f"    sheets: {table['sheet_count']}")
                for sheet in table.get('sheets', []):
                    lines.append(
                        f"      - {sheet['sheet']}: records={sheet.get('records')} last={sheet.get('last_period')}"
                    )
        lines.append('')

    generated_at = dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    lines.append(f'Report generated at {generated_at}')
    return '\n'.join(lines)


def main() -> None:
    summary_text = build_summary()
    OUTPUT_FILE.write_text(summary_text, encoding='utf-8')
    print(f"Summary written to {OUTPUT_FILE.resolve()}")


if __name__ == '__main__':
    main()
