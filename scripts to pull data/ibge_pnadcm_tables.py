"""
Ingestion helper for IBGE PNAD Contínua Móvel (PNADCM) tables.

This script fetches a curated list of PNADCM tables (moving-quarter
indicators), cleans their content, and uploads the results to Firebase.
"""

import argparse
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from ibge_base import (
    clean_and_structure_data,
    fetch_all_sheets,
    upload_multiple_sheets_to_firebase,
    upload_table_data,
)

# --- Configuration ---------------------------------------------------------

CATEGORY = "pnadcm"

PNADCM_TABLES: Dict[int, Dict[str, object]] = {
    3918: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência - "
            "Total, coeficiente de variação, variações percentuais e absolutas em relação "
            "aos três trimestres móveis anteriores e ao mesmo trimestre móvel do ano "
            "anterior - por contribuição para instituto de previdência em qualquer trabalho "
            "(nº3918)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela3918.xlsx&terr=N&rank=-"
            "&query=t/3918/n1/all/v/4090/p/all/c12027/all/l/v,c12027,t%2Bp"
        ),
    },
    3919: {
        "name": (
            "Percentual de pessoas contribuintes de instituto de previdência em qualquer trabalho "
            "na população de 14 anos ou mais de idade, ocupada na semana de referência - Total, "
            "coeficiente de variação, variações em relação aos três trimestres móveis anteriores e "
            "ao mesmo trimestre móvel do ano anterior (nº3919)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela3919.xlsx&terr=N&rank=-"
            "&query=t/3919/n1/all/v/8463/p/all/d/v8463%201/l/v,,t%2Bp"
        ),
    },
    5944: {
        "name": (
            "Taxa de participação na força de trabalho, na semana de referência, das pessoas de "
            "14 anos ou mais de idade - Total, coeficiente de variação, variações em relação "
            "aos três trimestres móveis anteriores e ao mesmo trimestre móvel do ano anterior "
            "(nº5944)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5944.xlsx&terr=N&rank=-"
            "&query=t/5944/n1/all/v/4096/p/all/d/v4096%201/l/v,,t%2Bp"
        ),
    },
    6022: {
        "name": (
            "População - Total, coeficiente de variação, variações percentuais e absolutas em relação "
            "aos três trimestres móveis anteriores e ao mesmo trimestre móvel do ano anterior (nº6022)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6022.xlsx&terr=N&rank=-"
            "&query=t/6022/n1/all/v/606/p/all/l/v,,t%2Bp"
        ),
    },
    6318: {
        "name": (
            "Pessoas de 14 anos ou mais de idade - Total, coeficiente de variação, variações percentuais "
            "e absolutas em relação aos três trimestres móveis anteriores e ao mesmo trimestre móvel do ano "
            "anterior, por condição em relação à força de trabalho e condição de ocupação (nº6318)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6318.xlsx&terr=N&rank=-"
            "&query=t/6318/n1/all/v/1641/p/all/c629/all/l/v,c629,t%2Bp"
        ),
    },
    6320: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência - Total, coeficiente "
            "de variação, variações percentuais e absolutas em relação aos três trimestres móveis anteriores "
            "e ao mesmo trimestre móvel do ano anterior - por posição na ocupação e categoria do emprego no "
            "trabalho principal (nº6320)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6320.xlsx&terr=N&rank=-"
            "&query=t/6320/n1/all/v/4090/p/all/c11913/all/l/v,c11913,t%2Bp"
        ),
    },
    6323: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência - Total, coeficiente "
            "de variação, variações percentuais e absolutas em relação aos três trimestres móveis anteriores "
            "e ao mesmo trimestre móvel do ano anterior - por grupamento de atividade no trabalho principal "
            "(nº6323)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6323.xlsx&terr=N&rank=-"
            "&query=t/6323/n1/all/v/4090/p/all/c888/all/l/v,c888,t%2Bp"
        ),
    },
    6379: {
        "name": (
            "Nível da ocupação, na semana de referência, das pessoas de 14 anos ou mais de idade - "
            "Total, coeficiente de variação, variações em relação aos três trimestres móveis anteriores "
            "e ao mesmo trimestre móvel do ano anterior (nº6379)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6379.xlsx&terr=N&rank=-"
            "&query=t/6379/n1/all/v/4097/p/all/d/v4097%201/l/v,,t%2Bp"
        ),
    },
    6380: {
        "name": (
            "Nível da desocupação, na semana de referência, das pessoas de 14 anos ou mais de idade - "
            "Total, coeficiente de variação, variações em relação aos três trimestres móveis anteriores "
            "e ao mesmo trimestre móvel do ano anterior (nº6380)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6380.xlsx&terr=N&rank=-"
            "&query=t/6380/n1/all/v/4098/p/all/d/v4098%201/l/v,,t%2Bp"
        ),
    },
    6381: {
        "name": (
            "Taxa de desocupação, na semana de referência, das pessoas de 14 anos ou mais de idade - "
            "Total, coeficiente de variação, variações em relação aos três trimestres móveis anteriores "
            "e ao mesmo trimestre móvel do ano anterior (nº6381)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6381.xlsx&terr=N&rank=-"
            "&query=t/6381/n1/all/v/4099/p/all/d/v4099%201/l/v,,t%2Bp"
        ),
    },
    6387: {
        "name": (
            "Rendimento médio mensal real e nominal das pessoas de 14 anos ou mais de idade ocupadas "
            "na semana de referência com rendimento de trabalho, efetivamente recebido em todos os trabalhos "
            "- Total, coeficiente de variação, variações em relação aos três trimestres móveis anteriores e "
            "ao mesmo trimestre móvel do ano anterior (nº6387)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6387.xlsx&terr=N&rank=-"
            "&query=t/6387/n1/all/v/5935/p/all/l/v,,t%2Bp"
        ),
    },
    6388: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana de "
            "referência com rendimento de trabalho, efetivamente recebido no trabalho principal - Total, "
            "coeficiente de variação, variações em relação aos três trimestres móveis anteriores e ao mesmo "
            "trimestre móvel do ano anterior (nº6388)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6388.xlsx&terr=N&rank=-"
            "&query=t/6388/n1/all/v/5934/p/all/l/v,,t%2Bp"
        ),
    },
    6389: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana de "
            "referência com rendimento de trabalho, habitualmente recebido no trabalho principal - Total, "
            "coeficiente de variação, variações em relação aos três trimestres móveis anteriores e ao mesmo "
            "trimestre móvel do ano anterior - por posição na ocupação e categoria do emprego no trabalho "
            "principal (nº6389)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6389.xlsx&terr=N&rank=-"
            "&query=t/6389/n1/all/v/5932/p/last%201/c11913/all/l/v,c11913,t%2Bp"
        ),
    },
    6390: {
        "name": (
            "Rendimento médio mensal real e nominal das pessoas de 14 anos ou mais de idade ocupadas na "
            "semana de referência com rendimento de trabalho, habitualmente recebido em todos os trabalhos "
            "- Total, coeficiente de variação, variações em relação aos três trimestres móveis anteriores e "
            "ao mesmo trimestre móvel do ano anterior (nº6390)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6390.xlsx&terr=N&rank=-"
            "&query=t/6390/n1/all/v/5933/p/all/l/v,,t%2Bp"
        ),
    },
    6391: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana de "
            "referência com rendimento de trabalho, habitualmente recebido no trabalho principal - Total, "
            "coeficiente de variação, variações em relação aos três trimestres móveis anteriores e ao mesmo "
            "trimestre móvel do ano anterior - por grupamento de atividade no trabalho principal (nº6391)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6391.xlsx&terr=N&rank=-"
            "&query=t/6391/n1/all/v/5932/p/all/c888/all/l/v,c888,t%2Bp"
        ),
    },
    6392: {
        "name": (
            "Massa de rendimento mensal real e nominal das pessoas de 14 anos ou mais de idade ocupadas "
            "na semana de referência com rendimento de trabalho, habitualmente recebido em todos os trabalhos "
            "- Total, coeficiente de variação, variações percentuais e absolutas em relação aos três trimestres "
            "móveis anteriores e ao mesmo trimestre móvel do ano anterior (nº6392)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6392.xlsx&terr=N&rank=-"
            "&query=t/6392/n1/all/v/6293/p/all/l/v,,t%2Bp"
        ),
    },
    6393: {
        "name": (
            "Massa de rendimento mensal real e nominal das pessoas de 14 anos ou mais de idade ocupadas "
            "na semana de referência com rendimento de trabalho, efetivamente recebido em todos os trabalhos "
            "- Total, coeficiente de variação, variações percentuais e absolutas em relação aos três trimestres "
            "móveis anteriores e ao mesmo trimestre móvel do ano anterior (nº6393)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6393.xlsx&terr=N&rank=-"
            "&query=t/6393/n1/all/v/6295/p/all/l/v,,t%2Bp"
        ),
    },
    6438: {
        "name": (
            "Pessoas de 14 anos ou mais de idade - Total, coeficiente de variação, variações percentuais e "
            "absolutas em relação aos três trimestres móveis anteriores e ao mesmo trimestre móvel do ano anterior "
            "- por tipo de medida de subutilização da força de trabalho na semana de referência (nº6438)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6438.xlsx&terr=N&rank=-"
            "&query=t/6438/n1/all/v/1641/p/all/c604/all/l/v,c604,t%2Bp"
        ),
    },
    6439: {
        "name": (
            "Taxa combinada de desocupação e de subocupação por insuficiência de horas trabalhadas - Total, "
            "coeficiente de variação, variações em relação aos três trimestres móveis anteriores e ao mesmo "
            "trimestre móvel do ano anterior (nº6439)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6439.xlsx&terr=N&rank=-"
            "&query=t/6439/n1/all/v/4114/p/all/d/v4114%201/l/v,,t%2Bp"
        ),
    },
    6440: {
        "name": (
            "Taxa combinada da desocupação e da força de trabalho potencial - Total, coeficiente de variação, "
            "variações em relação aos três trimestres móveis anteriores e ao mesmo trimestre móvel do ano anterior "
            "(nº6440)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6440.xlsx&terr=N&rank=-"
            "&query=t/6440/n1/all/v/all/p/all/d/v4116%201,v4117%201,v8650%201,v8654%201/l/v,,t%2Bp"
        ),
    },
    6441: {
        "name": (
            "Taxa composta da subutilização da força de trabalho - Total, coeficiente de variação, variações "
            "em relação aos três trimestres móveis anteriores e ao mesmo trimestre móvel do ano anterior (nº6441)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6441.xlsx&terr=N&rank=-"
            "&query=t/6441/n1/all/v/4118/p/all/d/v4118%201/l/,v,t%2Bp"
        ),
    },
    6785: {
        "name": (
            "Taxa de subocupação por insuficiência de horas trabalhadas - Total, coeficiente de variação, "
            "variações em relação aos três trimestres móveis anteriores e ao mesmo trimestre móvel do ano anterior "
            "(nº6785)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6785.xlsx&terr=N&rank=-"
            "&query=t/6785/n1/all/v/9819/p/all/d/v9819%201/l/v,,t%2Bp"
        ),
    },
    6807: {
        "name": (
            "Percentual de pessoas desalentadas na população na força de trabalho ou desalentada - Total, "
            "coeficiente de variação, variações em relação aos três trimestres móveis anteriores e ao mesmo "
            "trimestre móvel do ano anterior (nº6807)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6807.xlsx&terr=N&rank=-"
            "&query=t/6807/n1/all/v/9869/p/all/d/v9869%201/l/v,,t%2Bp"
        ),
    },
    8501: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência - Total, coeficiente de "
            "variação, variações percentuais e absolutas em relação aos três trimestres móveis anteriores e ao "
            "mesmo trimestre móvel do ano anterior - por situação de informalidade no trabalho principal (nº8501)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8501.xlsx&terr=N&rank=-"
            "&query=t/8501/n1/all/v/4090/p/all/c1350/all/l/v,c1350,t%2Bp"
        ),
    },
    8513: {
        "name": (
            "Taxa de informalidade das pessoas de 14 anos ou mais de idade, ocupadas na semana de referência "
            "- Total, coeficiente de variação, variações em relação aos três trimestres móveis anteriores e ao "
            "mesmo trimestre móvel do ano anterior (nº8513)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8513.xlsx&terr=N&rank=-"
            "&query=t/8513/n1/all/v/12466/p/all/d/v12466%201/l/v,,t%2Bp"
        ),
    },
}


# --- Helpers ----------------------------------------------------------------


def detect_period_column(df: pd.DataFrame) -> str:
    """Return the most likely period column name."""
    for col in df.columns:
        col_lower = str(col).lower()
        if "trimestre" in col_lower or "móvel" in col_lower or "m�vel" in col_lower:
            return col
        if "periodo" in col_lower or "período" in col_lower:
            return col
    return df.columns[0]


def determine_period_range_from_dataframe(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    """Determine the start and end period from a DataFrame."""
    if df.empty:
        return None, None

    period_col = detect_period_column(df)
    series = df[period_col].dropna()
    if series.empty:
        return None, None

    start = str(series.iloc[0])
    end = str(series.iloc[-1])
    return start, end


def determine_period_range_from_sheets(sheets: Dict[str, pd.DataFrame]) -> Tuple[Optional[str], Optional[str]]:
    """Determine start/end period from the first sheet containing timeline data."""
    for df in sheets.values():
        start, end = determine_period_range_from_dataframe(df)
        if start and end:
            return start, end
    return None, None


def fetch_and_clean_all_sheets(url: str) -> Dict[str, pd.DataFrame]:
    """Fetch all sheets for a table and return cleaned DataFrames keyed by sheet name."""
    sheets_raw = fetch_all_sheets(url)
    cleaned = {}
    for sheet_name, (df_raw, sector_names) in sheets_raw.items():
        if sheet_name and sheet_name.lower() in {"notas", "notes", "metadata"}:
            continue
        try:
            df_clean = clean_and_structure_data(df_raw, sector_names)
        except Exception as exc:
            print(f"  [WARNING] Failed to clean sheet '{sheet_name}': {exc}")
            continue
        if not df_clean.empty:
            cleaned[sheet_name] = df_clean
    return cleaned


def upload_single_table(
    table_number: int, name: str, df: pd.DataFrame, period_range: Tuple[Optional[str], Optional[str]]
) -> bool:
    """Upload a single-sheet table to Firebase."""
    start, end = period_range
    period = None
    if start and end:
        period = f"{start} a {end}"
        print(f"   Detected period range: {period}")

    return upload_table_data(
        df,
        table_number,
        name,
        period_range=period,
        category=CATEGORY,
    )


def upload_multi_sheet_table(
    table_number: int,
    name: str,
    sheets: Dict[str, pd.DataFrame],
    period_range: Tuple[Optional[str], Optional[str]],
) -> Dict[str, bool]:
    """Upload a multi-sheet table to Firebase."""
    start, end = period_range
    period = None
    if start and end:
        period = f"{start} a {end}"
        print(f"   Detected period range: {period}")

    return upload_multiple_sheets_to_firebase(
        sheets,
        table_number,
        name,
        period_range=period,
        category=CATEGORY,
    )


def process_table(table_number: int, config: Dict[str, object]) -> None:
    """Fetch, process, and upload a PNADCM table."""
    name = config["name"]
    url = config["url"]
    explicit_multi = config.get("multi_sheet", False)

    print(f"\n{'='*80}")
    print(f"Processing table {table_number} - {name}")
    print(f"URL: {url}")

    if explicit_multi:
        print("-> Multi-sheet table (configured)")
        sheets = fetch_and_clean_all_sheets(url)
        print(f"   Cleaned sheets: {len(sheets)}")
        if not sheets:
            print("   [WARNING] No data sheets found; skipping upload.")
            return

        period_range = determine_period_range_from_sheets(sheets)
        results = upload_multi_sheet_table(table_number, name, sheets, period_range)
        success = all(results.values())
        print(f"   Upload status: {'SUCCESS' if success else 'PARTIAL'}")
        return

    sheets = fetch_and_clean_all_sheets(url)
    if not sheets:
        print("   [WARNING] No data sheets found; skipping upload.")
        return

    if len(sheets) == 1:
        sheet_name, df = next(iter(sheets.items()))
        print(f"-> Single-sheet table (detected). Sheet: {sheet_name}")
        period_range = determine_period_range_from_dataframe(df)
        success = upload_single_table(table_number, name, df, period_range)
        print(f"   Upload status: {'SUCCESS' if success else 'FAILED'}")
    else:
        print(f"-> Multi-sheet table (detected). Sheets: {len(sheets)}")
        period_range = determine_period_range_from_sheets(sheets)
        results = upload_multi_sheet_table(table_number, name, sheets, period_range)
        success = all(results.values())
        print(f"   Upload status: {'SUCCESS' if success else 'PARTIAL'}")


def fetch_and_upload_pnadcm_tables(selected_tables: Optional[Sequence[int]] = None) -> None:
    """Process all configured PNADCM tables or a selected subset."""
    if selected_tables:
        target_tables = selected_tables
    else:
        target_tables = PNADCM_TABLES.keys()

    for table_number in target_tables:
        config = PNADCM_TABLES.get(table_number)
        if not config:
            print(f"\n[WARNING] Table {table_number} is not configured; skipping.")
            continue
        try:
            process_table(table_number, config)
        except Exception as exc:
            print(f"[ERROR] Failed to process table {table_number}: {exc}")
            import traceback

            traceback.print_exc()


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Atualiza tabelas PNADCM selecionadas.")
    parser.add_argument(
        "--table",
        type=int,
        action="append",
        dest="tables",
        help="Número da tabela a atualizar (pode ser usado múltiplas vezes). Omissão = todas.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)
    tables: Optional[List[int]] = args.tables if args.tables else None
    fetch_and_upload_pnadcm_tables(tables)


if __name__ == "__main__":
    main()


