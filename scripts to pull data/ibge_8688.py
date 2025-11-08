"""
Script to fetch IBGE Table 8688 - PMS activities and subactivities (receita/volume).
Handles hierarchical activity codes and melts monthly series into long format.
"""
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
import re
from typing import Dict, List, Tuple

import pandas as pd
import requests

from ibge_base import clean_firebase_key, upload_to_firebase_path

TABLE_NUMBER = 8688
PERIOD_RANGE = 'janeiro 2011 a agosto 2025'

@dataclass
class SubBranchConfig:
    name: str
    url: str
    table_name: str


SUBBRANCHES: Dict[str, SubBranchConfig] = {
    'receita': SubBranchConfig(
        name='receita',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8688.xlsx&terr=N&rank=-&query=t/8688/n1/all/v/all/p/all/c11046/56725/c12355/all/d/v7167%205,v7168%205,v11623%201,v11624%201,v11625%201,v11626%201/l/v,p%2Bc11046,t%2Bc12355',
        table_name='PMS - Índice e variação da receita nominal e do volume de serviços, por atividades de serviços e suas subdivisões (2022 = 100) - Receita (nº8688)',
    ),
    'volume': SubBranchConfig(
        name='volume',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8688.xlsx&terr=N&rank=-&query=t/8688/n1/all/v/all/p/all/c11046/56726/c12355/all/d/v7167%205,v7168%205,v11623%201,v11624%201,v11625%201,v11626%201/l/v,p%2Bc11046,t%2Bc12355',
        table_name='PMS - Índice e variação da receita nominal e do volume de serviços, por atividades de serviços e suas subdivisões (2022 = 100) - Volume (nº8688)',
    ),
}

ACTIVITY_PATTERN = re.compile(r'(?P<codigo>\d+(?:\.\d+)*)\s+(?P<nome>.+)')


def fetch_excel(url: str) -> pd.ExcelFile:
    response = requests.get(url)
    response.raise_for_status()
    return pd.ExcelFile(BytesIO(response.content))


def standardize_columns(columns: List[Tuple[str, str, str]]) -> pd.MultiIndex:
    cleaned: List[Tuple[str, str, str]] = []
    for c0, c1, c2 in columns:
        if c0.startswith('Unnamed') and c1.startswith('Unnamed'):
            cleaned.append(('territory', '', ''))
        elif c0.startswith('Atividades'):
            cleaned.append(('atividade', '', ''))
        else:
            cleaned.append((c0, c1, c2))
    return pd.MultiIndex.from_tuples(cleaned)


def split_activity(text: str | float | int) -> Tuple[str | None, str | None, str | None]:
    if not isinstance(text, str):
        return None, None, None
    match = ACTIVITY_PATTERN.match(text)
    if match:
        return match.group('codigo'), match.group('nome'), text
    return None, text, text


def transform_sheet(excel: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    df = excel.parse(sheet_name=sheet_name, header=[2, 3, 4])
    df.columns = standardize_columns(df.columns.tolist())

    territory_col = ('territory', '', '')
    activity_col = ('atividade', '', '')

    df[territory_col] = df[territory_col].ffill()
    df[activity_col] = df[activity_col].ffill()

    value_columns = [col for col in df.columns if col[0].startswith('M')]
    new_column_names: List[Tuple[str, str, str] | str] = []
    for col in df.columns:
        if col in value_columns:
            period_label = str(col[1]).strip()
            indicator_label = str(col[2]).strip()
            new_column_names.append(f"{period_label}||{indicator_label}")
        else:
            new_column_names.append(col)
    df.columns = new_column_names

    melt_columns = [name for name in df.columns if isinstance(name, str) and '||' in name]

    melted = df.melt(
        id_vars=[territory_col, activity_col],
        value_vars=melt_columns,
        var_name='period_indicator',
        value_name='valor',
    )

    melted[['periodo', 'subbranch_indicator']] = (
        melted['period_indicator'].str.split('||', n=1, expand=True, regex=False)
    )
    melted.drop(columns=['period_indicator'], inplace=True)
    melted['periodo'] = melted['periodo'].astype(str).str.strip()
    melted['subbranch_indicator'] = melted['subbranch_indicator'].astype(str).str.strip()
    melted.replace({'periodo': {'': None, 'nan': None}, 'subbranch_indicator': {'': None, 'nan': None}}, inplace=True)

    melted['valor'] = pd.to_numeric(melted['valor'], errors='coerce')
    melted.dropna(subset=['valor', 'periodo'], inplace=True)

    if melted.empty:
        return pd.DataFrame(columns=['territory', 'atividade_codigo', 'atividade_nome', 'atividade_texto', 'periodo', 'subbranch_indicator', 'valor'])

    codes, names, textos = zip(*melted[activity_col].apply(split_activity))
    melted = melted.assign(
        atividade_codigo=list(codes),
        atividade_nome=list(names),
        atividade_texto=list(textos),
    )

    melted.rename(columns={territory_col: 'territory'}, inplace=True)
    melted['periodo'] = melted['periodo'].astype(str)
    melted.drop(columns=[activity_col], inplace=True)

    return melted[['territory', 'atividade_codigo', 'atividade_nome', 'atividade_texto', 'periodo', 'subbranch_indicator', 'valor']]


def process_subbranch(config: SubBranchConfig) -> None:
    print(f"Processing subbranch '{config.name}'...")
    excel = fetch_excel(config.url)
    sheet_names = [name for name in excel.sheet_names if name.lower() not in ['notas', 'notes']]
    print(f"  Found {len(sheet_names)} data sheets")

    all_segments: set[str] = set()

    for sheet_name in sheet_names:
        print(f"  -> {sheet_name}")
        df = transform_sheet(excel, sheet_name)
        clean_name = clean_firebase_key(sheet_name)
        sheet_path = f'ibge_data/pms/table_{TABLE_NUMBER}/{config.name}/sheets/{clean_name}/data'
        success = upload_to_firebase_path(df, sheet_path)
        status = 'SUCCESS' if success else 'ERROR'
        print(f"     [{status}] {len(df)} records uploaded")
        all_segments.update(segment for segment in df['atividade_texto'].dropna().unique())

    metadata = {
        'table_number': TABLE_NUMBER,
        'table_name': config.table_name,
        'subbranch': config.name,
        'period_range': PERIOD_RANGE,
        'sheet_count': len(sheet_names),
        'activities': sorted(all_segments),
    }
    metadata_path = f'ibge_data/pms/table_{TABLE_NUMBER}/{config.name}/metadata'
    if upload_to_firebase_path(metadata, metadata_path):
        print(f"  [SUCCESS] Metadata uploaded for '{config.name}'")
    else:
        print(f"  [ERROR] Failed to upload metadata for '{config.name}'")


def fetch_and_upload_ibge_data() -> None:
    for config in SUBBRANCHES.values():
        print("=" * 80)
        process_subbranch(config)
    print("=" * 80)
    print("Table 8688 processing completed.")


if __name__ == '__main__':
    fetch_and_upload_ibge_data()
