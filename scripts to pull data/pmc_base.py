"""Utility functions for Pesquisa Mensal de Comércio (PMC) ingestion scripts."""
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
import re
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests

from ibge_base import clean_firebase_key, upload_to_firebase_path


@dataclass
class SubBranchConfig:
    name: str
    url: str
    table_name: str


def fetch_excel(url: str) -> pd.ExcelFile:
    response = requests.get(url)
    response.raise_for_status()
    return pd.ExcelFile(BytesIO(response.content))


def upload_simple_table(
    table_number: int,
    period_range: str,
    subbranches: Dict[str, SubBranchConfig],
    base_path: str = 'ibge_data/pmc',
) -> None:
    for config in subbranches.values():
        print("=" * 80)
        print(f"Processing subbranch '{config.name}' for table {table_number}...")
        excel = fetch_excel(config.url)
        sheet_names = [name for name in excel.sheet_names if name.lower() not in ['notas', 'notes']]
        print(f"  Found {len(sheet_names)} sheets")

        for sheet_name in sheet_names:
            df = excel.parse(sheet_name=sheet_name, skiprows=4, header=None)
            df.columns = ['territory', 'periodo', 'valor']
            df.replace(['..', '...', '-'], pd.NA, inplace=True)
            df['territory'] = df['territory'].ffill()
            df.dropna(subset=['periodo'], inplace=True)
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
            df.dropna(subset=['valor'], inplace=True)
            df['periodo'] = df['periodo'].astype(str).str.strip()
            df['indicator'] = sheet_name.strip()

            clean_name = clean_firebase_key(sheet_name)
            sheet_path = f'{base_path}/table_{table_number}/{config.name}/sheets/{clean_name}/data'
            success = upload_to_firebase_path(df[['territory', 'periodo', 'indicator', 'valor']], sheet_path)
            status = 'SUCCESS' if success else 'ERROR'
            print(f"  -> {sheet_name} [{status}] {len(df)} records")

        metadata = {
            'table_number': table_number,
            'table_name': config.table_name,
            'subbranch': config.name,
            'sheet_count': len(sheet_names),
            'period_range': period_range,
        }
        metadata_path = f'{base_path}/table_{table_number}/{config.name}/metadata'
        if upload_to_firebase_path(metadata, metadata_path):
            print(f"  [SUCCESS] Metadata uploaded for '{config.name}'")
        else:
            print(f"  [ERROR] Failed to upload metadata for '{config.name}'")


ACTIVITY_PATTERN = re.compile(r'(?P<codigo>\d+(?:\.\d+)*)\s+(?P<nome>.+)')


def upload_activity_table(
    table_number: int,
    period_range: str,
    subbranches: Dict[str, SubBranchConfig],
    base_path: str = 'ibge_data/pmc',
) -> None:
    for config in subbranches.values():
        print("=" * 80)
        print(f"Processing subbranch '{config.name}' for table {table_number}...")
        excel = fetch_excel(config.url)
        sheet_names = [name for name in excel.sheet_names if name.lower() not in ['notas', 'notes']]
        print(f"  Found {len(sheet_names)} sheets")

        all_activities: set[str] = set()

        for sheet_name in sheet_names:
            df = excel.parse(sheet_name=sheet_name, skiprows=4, header=None)
            df.columns = ['territory', 'atividade', 'periodo', 'valor']
            df.replace(['..', '...', '-'], pd.NA, inplace=True)
            df['territory'] = df['territory'].ffill()
            df['atividade'] = df['atividade'].ffill()
            df.dropna(subset=['periodo'], inplace=True)
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
            df.dropna(subset=['valor'], inplace=True)
            df['periodo'] = df['periodo'].astype(str).str.strip()

            codes: List[Optional[str]] = []
            names: List[str] = []
            textos: List[str] = []
            for text in df['atividade']:
                if isinstance(text, str):
                    match = ACTIVITY_PATTERN.match(text.strip())
                    if match:
                        codes.append(match.group('codigo'))
                        names.append(match.group('nome'))
                        textos.append(text.strip())
                    else:
                        codes.append(None)
                        names.append(text.strip())
                        textos.append(text.strip())
                else:
                    codes.append(None)
                    names.append(text)
                    textos.append(text)
            df = df.assign(
                atividade_codigo=codes,
                atividade_nome=names,
                atividade_texto=textos,
                indicator=sheet_name.strip(),
            )

            clean_name = clean_firebase_key(sheet_name)
            sheet_path = f'{base_path}/table_{table_number}/{config.name}/sheets/{clean_name}/data'
            success = upload_to_firebase_path(
                df[['territory', 'atividade_codigo', 'atividade_nome', 'atividade_texto', 'periodo', 'indicator', 'valor']],
                sheet_path,
            )
            status = 'SUCCESS' if success else 'ERROR'
            print(f"  -> {sheet_name} [{status}] {len(df)} records")
            all_activities.update(text for text in textos if text)

        metadata = {
            'table_number': table_number,
            'table_name': config.table_name,
            'subbranch': config.name,
            'sheet_count': len(sheet_names),
            'period_range': period_range,
            'activities': sorted(all_activities),
        }
        metadata_path = f'{base_path}/table_{table_number}/{config.name}/metadata'
        if upload_to_firebase_path(metadata, metadata_path):
            print(f"  [SUCCESS] Metadata uploaded for '{config.name}'")
        else:
            print(f"  [ERROR] Failed to upload metadata for '{config.name}'")
