"""
Script to fetch IBGE Table 8190 - PMC atacado especializado (receita e volume).
"""
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Dict

import pandas as pd
import requests

from ibge_base import clean_firebase_key, upload_to_firebase_path

TABLE_NUMBER = 8190
PERIOD_RANGE = 'janeiro 2022 a agosto 2025'


@dataclass
class SubBranchConfig:
    name: str
    url: str
    table_name: str


SUBBRANCHES: Dict[str, SubBranchConfig] = {
    'receita': SubBranchConfig(
        name='receita',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8190.xlsx&terr=N&rank=-&query=t/8190/n1/all/v/all/p/all/c11046/56739/d/v7169%205,v11709%201,v11710%201,v11711%201/l/v,c11046,t%2Bp',
        table_name='PMC - Índices de receita nominal de atacado especializado em produtos alimentícios, bebidas e fumo (2022 = 100) - Receita (nº8190)',
    ),
    'volume': SubBranchConfig(
        name='volume',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8190.xlsx&terr=N&rank=-&query=t/8190/n1/all/v/all/p/all/c11046/56740/d/v7169%205,v11709%201,v11710%201,v11711%201/l/v,c11046,t%2Bp',
        table_name='PMC - Índices de volume de atacado especializado em produtos alimentícios, bebidas e fumo (2022 = 100) - Volume (nº8190)',
    ),
}


def fetch_excel(url: str) -> pd.ExcelFile:
    response = requests.get(url)
    response.raise_for_status()
    return pd.ExcelFile(BytesIO(response.content))


def transform_sheet(excel: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    df = excel.parse(sheet_name=sheet_name, skiprows=4, header=None)
    df.columns = ['territory', 'periodo', 'valor']

    df.replace(['..', '...', '-'], pd.NA, inplace=True)
    df['territory'] = df['territory'].ffill()
    df.dropna(subset=['periodo'], inplace=True)

    df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
    df.dropna(subset=['valor'], inplace=True)
    df['periodo'] = df['periodo'].astype(str).str.strip()
    df['indicator'] = sheet_name.strip()

    return df[['territory', 'periodo', 'indicator', 'valor']]


def process_subbranch(config: SubBranchConfig) -> None:
    print(f"Processing subbranch '{config.name}'...")
    excel = fetch_excel(config.url)
    sheet_names = [name for name in excel.sheet_names if name.lower() not in ['notas', 'notes']]
    print(f"  Found {len(sheet_names)} sheets")

    for sheet_name in sheet_names:
        df = transform_sheet(excel, sheet_name)
        clean_name = clean_firebase_key(sheet_name)
        sheet_path = f'ibge_data/pmc/table_{TABLE_NUMBER}/{config.name}/sheets/{clean_name}/data'
        success = upload_to_firebase_path(df, sheet_path)
        status = 'SUCCESS' if success else 'ERROR'
        print(f"  -> {sheet_name} [{status}] {len(df)} records")

    metadata = {
        'table_number': TABLE_NUMBER,
        'table_name': config.table_name,
        'subbranch': config.name,
        'sheet_count': len(sheet_names),
        'period_range': PERIOD_RANGE,
    }
    metadata_path = f'ibge_data/pmc/table_{TABLE_NUMBER}/{config.name}/metadata'
    if upload_to_firebase_path(metadata, metadata_path):
        print(f"  [SUCCESS] Metadata uploaded for '{config.name}'")
    else:
        print(f"  [ERROR] Failed to upload metadata for '{config.name}'")


def fetch_and_upload_ibge_data() -> None:
    for config in SUBBRANCHES.values():
        print("=" * 80)
        process_subbranch(config)
    print("=" * 80)
    print("PMC Table 8190 processing completed.")


if __name__ == '__main__':
    fetch_and_upload_ibge_data()
