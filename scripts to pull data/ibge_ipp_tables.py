"""
Ingestion helper for IBGE IPP (Producer Price Index) tables.

Fetches the selected multi-sheet tables and uploads them to Firebase.
"""

from typing import Dict, Optional, Sequence, Tuple

import pandas as pd

from ibge_base import (
    clean_and_structure_data,
    fetch_all_sheets,
    upload_multiple_sheets_to_firebase,
)

# --- Configuration ---------------------------------------------------------

CATEGORY = "ipp"

IPP_TABLES: Dict[int, Dict[str, object]] = {
    6723: {
        "name": (
            "Índice de Preços ao Produtor, por tipo de índice e grupos industriais "
            "selecionados (dezembro de 2018 = 100) (nº6723)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6723.xlsx&terr=N&rank=-"
            "&query=t/6723/n1/all/v/all/p/all/c844/all/d/v1394%202,v1395%202,v1396%202,v10008%205/l/v,c844,t%2Bp"
        ),
        "multi_sheet": True,
    },
    6903: {
        "name": (
            "Índice de Preços ao Produtor, por tipo de índice, indústria geral, indústrias "
            "extrativas e indústrias de transformação e atividades (dezembro de 2018 = 100) "
            "(nº6903)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6903.xlsx&terr=N&rank=-"
            "&query=t/6903/n1/all/v/all/p/all/c842/all/d/v1394%202,v1395%202,v1396%202,v10008%205/l/v,c842,t%2Bp"
        ),
        "multi_sheet": True,
    },
    6904: {
        "name": (
            "Índice de Preços ao Produtor, por tipo de índice e grandes categorias econômicas "
            "(dezembro de 2018 = 100) (nº6904)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6904.xlsx&terr=N&rank=-"
            "&query=t/6904/n1/all/v/all/p/all/c543/all/d/v1394%202,v1395%202,v1396%202,v10008%205/l/v,c543,t%2Bp"
        ),
        "multi_sheet": True,
    },
}


# --- Helpers ----------------------------------------------------------------


def detect_period_column(df: pd.DataFrame) -> str:
    for col in df.columns:
        col_lower = str(col).lower()
        if "mês" in col_lower or "mes" in col_lower or "periodo" in col_lower or "período" in col_lower:
            return col
    return df.columns[0]


def determine_period_range_from_dataframe(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
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
    for df in sheets.values():
        start, end = determine_period_range_from_dataframe(df)
        if start and end:
            return start, end
    return None, None


def fetch_and_clean_all_sheets(url: str) -> Dict[str, pd.DataFrame]:
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


def process_table(table_number: int, config: Dict[str, object]) -> None:
    name = config["name"]
    url = config["url"]

    print(f"\n{'='*80}")
    print(f"Processing table {table_number} - {name}")
    print(f"URL: {url}")

    sheets = fetch_and_clean_all_sheets(url)
    print(f"   Cleaned sheets: {len(sheets)}")
    if not sheets:
        print("   [WARNING] No data sheets found; skipping upload.")
        return

    period_range = determine_period_range_from_sheets(sheets)
    start, end = period_range
    period = None
    if start and end:
        period = f"{start} a {end}"
        print(f"   Detected period range: {period}")

    results = upload_multiple_sheets_to_firebase(
        sheets,
        table_number,
        name,
        period_range=period,
        category=CATEGORY,
    )
    success = all(results.values())
    print(f"   Upload status: {'SUCCESS' if success else 'PARTIAL'}")


def fetch_and_upload_ipp_tables(selected_tables: Optional[Sequence[int]] = None) -> None:
    if selected_tables:
        target_tables = selected_tables
    else:
        target_tables = IPP_TABLES.keys()

    for table_number in target_tables:
        config = IPP_TABLES.get(table_number)
        if not config:
            print(f"\n[WARNING] Table {table_number} is not configured; skipping.")
            continue
        try:
            process_table(table_number, config)
        except Exception as exc:
            print(f"[ERROR] Failed to process table {table_number}: {exc}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    fetch_and_upload_ipp_tables()


