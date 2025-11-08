# IBGE Data Fetching Scripts

This directory contains scripts to fetch data from IBGE (Brazilian Institute of Geography and Statistics) and upload it to Firebase Realtime Database.

## Structure

- `ibge_base.py` - Base module with reusable functions for fetching and uploading data
- Individual table scripts (ibge_XXXX.py) - Scripts for each IBGE table
- `run_all_tables.py` - Master script to run all CNT table scripts
- `run_all_pms.py` - Master script to run all PMS table scripts
- `verify_structure.py` - Verifies CNT Firebase structure
- `verify_pms_structure.py` - Verifies PMS Firebase structure

## Tables

### Contas Nacionais Trimestrais (Quarterly National Accounts)

1. **Table 1620** - `ibge_CNT.py`
   - **Name:** Série encadeada do índice de volume trimestral (Base: média 1995 = 100)
   - **Period:** 1º trimestre 1996 a 2º trimestre 2025
   - **Firebase Table:** Série encadeada do índice de volume trimestral (Base: média 1995 = 100) (nº1620)

2. **Table 1621** - `ibge_1621.py`
   - **Name:** Série encadeada do índice de volume trimestral com ajuste sazonal (Base: média 1995 = 100)
   - **Period:** 1º trimestre 1996 a 2º trimestre 2025
   - **Firebase Table:** Série encadeada do índice de volume trimestral com ajuste sazonal (Base: média 1995 = 100) (nº1621)

3. **Table 1846** - `ibge_1846.py`
   - **Name:** Valores a preços correntes
   - **Period:** 1º trimestre 1996 a 2º trimestre 2025
   - **Firebase Table:** Valores a preços correntes (nº1846)

4. **Table 2072** - `ibge_2072.py`
   - **Name:** Contas econômicas trimestrais
   - **Period:** 1º trimestre 2000 a 2º trimestre 2025
   - **Sheets:** 12 sheets (each uploaded as separate Firebase node)
   - **Firebase Tables:** Contas econômicas trimestrais (nº2072) - [Sheet Name] for each sheet

5. **Table 5932** - `ibge_5932.py`
   - **Name:** Taxa de variação do índice de volume trimestral
   - **Period:** 1º trimestre 1996 a 2º trimestre 2025
   - **Sheets:** 4 sheets (each uploaded as separate Firebase node)
   - **Firebase Tables:** Taxa de variação do índice de volume trimestral (nº5932) - [Sheet Name] for each sheet

6. **Table 6612** - `ibge_6612.py`
   - **Name:** Valores encadeados a preços de 1995
   - **Period:** 1º trimestre 1996 a 2º trimestre 2025
   - **Firebase Table:** Valores encadeados a preços de 1995 (nº6612)

7. **Table 6613** - `ibge_6613.py`
   - **Name:** Valores encadeados a preços de 1995 com ajuste sazonal
   - **Period:** 1º trimestre 1996 a 2º trimestre 2025
   - **Firebase Table:** Valores encadeados a preços de 1995 com ajuste sazonal (nº6613)

8. **Table 6726** - `ibge_6726.py`
   - **Name:** Taxa de poupança
   - **Period:** 1º trimestre 2000 a 2º trimestre 2025
   - **Firebase Table:** Taxa de poupança (nº6726)

9. **Table 6727** - `ibge_6727.py`
   - **Name:** Taxa de investimento
   - **Period:** 1º trimestre 1996 a 2º trimestre 2025
   - **Firebase Table:** Taxa de investimento (nº6727)

## Multi-Sheet Tables

Some tables contain multiple sheets in the Excel file:
- **Table 2072**: 12 sheets - Each sheet is uploaded as a separate Firebase node
- **Table 5932**: 4 sheets - Each sheet is uploaded as a separate Firebase node

For multi-sheet tables, each sheet is uploaded with the format: `[Table Name] - [Sheet Name]`

### Pesquisa Mensal de Serviços (Monthly Services Survey - PMS)

1. **Table 5906 - Receita** - `ibge_5906_receita.py`
   - **Name:** Índice e variação da receita nominal e do volume de serviços (2022 = 100) - Receita
   - **Period:** janeiro 2011 a agosto 2025
   - **Sheets:** 6 sheets (each uploaded as separate Firebase node)
   - **Firebase Path:** `ibge_data/pms/table_5906/receita`

2. **Table 5906 - Volume** - `ibge_5906_volume.py`
   - **Name:** Índice e variação da receita nominal e do volume de serviços (2022 = 100) - Volume
   - **Period:** janeiro 2011 a agosto 2025
   - **Sheets:** 6 sheets (each uploaded as separate Firebase node)
   - **Firebase Path:** `ibge_data/pms/table_5906/volume`

### Current Status:

- ✅ **Table 1620** - Working (Single sheet)
- ✅ **Table 1621** - Working (Single sheet)
- ✅ **Table 1846** - Working (Single sheet)
- ✅ **Table 2072** - Working (12 sheets - each uploaded as separate node)
- ✅ **Table 5932** - Working (4 sheets - each uploaded as separate node)
- ✅ **Table 6612** - Working (Single sheet)
- ✅ **Table 6613** - Working (Single sheet)
- ✅ **Table 6726** - Working (Single sheet)
- ✅ **Table 6727** - Working (Single sheet)
- ❌ **Table 2205** - Excluded (not needed)

## Usage

### Run a single table script:
```bash
python ibge_CNT.py
```

### Run all tables:
```bash
python run_all_tables.py
```

### Run all PMS tables:
```bash
python run_all_pms.py
```

### Generate status summary:
```bash
python summarize_status.py
```
Creates `status_summary.txt` at the project root summarizing workflow schedules/status and Firebase data freshness.

**Note:** Make sure all URLs are configured correctly before running all tables.

## Configuration

All scripts use the same Firebase database:
- **Base URL:** `https://peixo-28d2d-default-rtdb.firebaseio.com`
- **CNT Base Path:** `ibge_data/cnt/`
- **PMS Base Path:** `ibge_data/pms/`

## Firebase Data Structure

### CNT Nested Structure

All CNT tables are stored in a nested structure under `ibge_data/cnt/`:

```
ibge_data/
├── cnt/
│   ├── table_1620/
│   │   ├── data (array of records)
│   │   └── metadata (table info)
│   ├── table_1621/
│   │   ├── data
│   │   └── metadata
│   ├── table_2072/
│   │   ├── sheets/
│   │   │   ├── produto_interno_bruto/
│   │   │   │   └── data
│   │   │   ├── salarios/
│   │   │   │   └── data
│   │   │   └── ... (other sheets)
│   │   └── metadata (includes sheet list)
│   └── table_5932/
│       ├── sheets/
│       │   └── ... (4 sheets)
│       └── metadata
└── pms/
    └── table_5906/
        ├── receita/
        │   ├── sheets/
        │   │   └── ... (6 sheets)
        │   └── metadata
        └── volume/
            ├── sheets/
            │   └── ... (6 sheets)
            └── metadata
```

### CNT Single-Sheet Tables
- **Path:** `ibge_data/cnt/table_{number}/data`
- **Metadata:** `ibge_data/cnt/table_{number}/metadata`

### CNT Multi-Sheet Tables
- **Sheets Path:** `ibge_data/cnt/table_{number}/sheets/{sheet_name}/data`
- **Metadata:** `ibge_data/cnt/table_{number}/metadata` (includes sheet list)

### PMS Nested Structure

PMS tables are stored under `ibge_data/pms/`.

### Metadata Structure

Each table has metadata containing:
- `table_number`: Table number (e.g., 1620)
- `table_name`: Full descriptive name
- `period_range`: Period range (e.g., "1º trimestre 1996 a 2º trimestre 2025")
- `sheet_count`: Number of sheets (for multi-sheet tables)
- `sheets`: List of sheet information (for multi-sheet tables)

## Data Processing

Each script:
1. Fetches data from IBGE SIDRA API
2. Cleans and processes the data (handles missing values, converts to numeric)
3. Structures data with proper column names (Trimestre + sector names)
4. Uploads to Firebase Realtime Database with nested structure
5. Uploads metadata for easy discovery

## Requirements

See `../requirements.txt` for all dependencies.

Main dependencies:
- requests
- pandas
- openpyxl (for Excel file reading)

