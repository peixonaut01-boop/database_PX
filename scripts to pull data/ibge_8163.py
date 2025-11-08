"""
Script to fetch IBGE Table 8163 - PMS segments (receita and volume).
Each subbranch (receita/volume) contains 6 sheets with monthly data
segmented by 20 service categories.
"""
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Dict, List

import pandas as pd
import requests

from ibge_base import clean_firebase_key, upload_to_firebase_path

TABLE_NUMBER = 8163
PERIOD_RANGE = 'janeiro 2011 a agosto 2025'

@dataclass
class SubBranchConfig:
    name: str
    url: str
    table_name: str


SUBBRANCHES: Dict[str, SubBranchConfig] = {
    'receita': SubBranchConfig(
        name='receita',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8163.xlsx&terr=N&rank=-&query=t/8163/n1/all/v/all/p/all/c11046/56725/c1274/all/d/v7167%205,v7168%205,v11623%201,v11624%201,v11625%201,v11626%201/l/v,c11046,t%2Bc1274%2Bp',
        table_name='PMS - Índice e variação da receita nominal e do volume de serviços, por segmentos de serviços (2022 = 100) - Receita (nº8163)',
    ),
    'volume': SubBranchConfig(
        name='volume',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8163.xlsx&terr=N&rank=-&query=t/8163/n1/all/v/all/p/all/c11046/56726/c1274/all/d/v7167%205,v7168%205,v11623%201,v11624%201,v11625%201,v11626%201/l/v,c11046,t%2Bc1274%2Bp',
        table_name='PMS - Índice e variação da receita nominal e do volume de serviços, por segmentos de serviços (2022 = 100) - Volume (nº8163)',
    ),
}


def fetch_excel(url: str) -> pd.ExcelFile:
    response = requests.get(url)
    response.raise_for_status()
    data = BytesIO(response.content)
    return pd.ExcelFile(data)


def clean_segmented_sheet(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.replace(['..', '...'], pd.NA)
    df = df.iloc[:, :4]
    df.columns = ['territory', 'segment', 'period', 'value']
    df['territory'] = df['territory'].ffill()
    df['segment'] = df['segment'].ffill()
    df.dropna(subset=['period'], inplace=True)
    df['period'] = df['period'].astype(str)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df.dropna(subset=['segment', 'value'], inplace=True)
    df = df[df['segment'].str.lower() != 'notas']
    return df[['segment', 'period', 'value']]


def process_subbranch(config: SubBranchConfig) -> None:
    print(f"1. Fetching data for subbranch '{config.name}'...")
    excel = fetch_excel(config.url)
    sheet_names = [name for name in excel.sheet_names if name.lower() not in ['notas', 'notes']]
    print(f"Found {len(sheet_names)} sheets")

    processed = {}
    all_segments: set[str] = set()

    for sheet_name in sheet_names:
        print(f"\nProcessing sheet: {sheet_name}")
        df_raw = excel.parse(sheet_name=sheet_name, skiprows=4, header=None)
        print(f"  Raw shape: {df_raw.shape}")
        df_clean = clean_segmented_sheet(df_raw)
        print(f"  Cleaned rows: {len(df_clean)}")
        processed[sheet_name] = df_clean
        all_segments.update(df_clean['segment'].unique())

    firebase_base = f'ibge_data/pms/table_{TABLE_NUMBER}/{config.name}'

    for sheet_name, df in processed.items():
        clean_name = clean_firebase_key(sheet_name)
        sheet_path = f'{firebase_base}/sheets/{clean_name}/data'
        success = upload_to_firebase_path(df, sheet_path)
        status = 'SUCCESS' if success else 'ERROR'
        print(f"[{status}] Uploaded sheet '{sheet_name}'")

    metadata = {
        'table_number': TABLE_NUMBER,
        'table_name': config.table_name,
        'subbranch': config.name,
        'period_range': PERIOD_RANGE,
        'sheet_count': len(processed),
        'segments': sorted(all_segments),
    }
    metadata_path = f'{firebase_base}/metadata'
    if upload_to_firebase_path(metadata, metadata_path):
        print(f"[SUCCESS] Metadata uploaded for subbranch '{config.name}'")
    else:
        print(f"[ERROR] Failed to upload metadata for subbranch '{config.name}'")


def fetch_and_upload_ibge_data() -> None:
    for subbranch, config in SUBBRANCHES.items():
        print("=" * 80)
        process_subbranch(config)
    print("=" * 80)
    print("All subbranches processed.")


if __name__ == '__main__':
    fetch_and_upload_ibge_data()
