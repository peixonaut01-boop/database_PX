# Firebase Storage Structure Analysis

## Current Structure

### Single-Sheet Tables
```
Firebase Root
├── Série encadeada do índice de volume trimestral (Base: média 1995 = 100) (nº1620)
├── Série encadeada do índice de volume trimestral com ajuste sazonal (Base: média 1995 = 100) (nº1621)
├── Valores a preços correntes (nº1846)
├── Valores encadeados a preços de 1995 (nº6612)
├── Valores encadeados a preços de 1995 com ajuste sazonal (nº6613)
├── Taxa de poupança (nº6726)
└── Taxa de investimento (nº6727)
```

### Multi-Sheet Tables
```
Firebase Root
├── Contas econômicas trimestrais (nº2072) - Produto_Interno_Bruto_(Milh...
├── Contas econômicas trimestrais (nº2072) - (+)_Salários_(líquidos_receb...
├── Contas econômicas trimestrais (nº2072) - (+)_Rendas_de_propriedade_(l...
├── ... (10 more sheets)
├── Taxa de variação do índice de volume trimestral (nº5932) - Taxa_trimestral_(em_relação...
├── Taxa de variação do índice de volume trimestral (nº5932) - Taxa_acumulada_em_quatro_tri...
└── ... (2 more sheets)
```

## Issues with Current Approach

### 1. **Flat Structure**
- All tables at root level
- No logical grouping
- Hard to organize and navigate

### 2. **Long Node Names**
- Very long descriptive names
- Makes Firebase console hard to read
- Difficult to reference in code

### 3. **Multi-Sheet Tables Disconnected**
- Sheets from same table stored as separate nodes
- No easy way to query "all sheets from table 2072"
- Relationship between sheets is lost

### 4. **Scalability**
- As more tables are added, root becomes cluttered
- Hard to maintain and manage

### 5. **Query Limitations**
- Can't easily get "all IBGE data"
- Can't group by table type
- No metadata per table

## Recommended Structure

### Option 1: Nested by Table (Recommended)
```
Firebase Root
└── ibge_data/
    ├── table_1620/
    │   └── data (array of records)
    ├── table_1621/
    │   └── data
    ├── table_2072/
    │   ├── sheets/
    │   │   ├── produto_interno_bruto/
    │   │   │   └── data
    │   │   ├── salarios/
    │   │   │   └── data
    │   │   └── ... (other sheets)
    │   └── metadata
    │       ├── table_name
    │       ├── table_number
    │       └── sheet_count
    └── table_5932/
        ├── sheets/
        │   └── ... (4 sheets)
        └── metadata
```

**Benefits:**
- ✅ Clear organization
- ✅ Easy to query all data from a table
- ✅ Can add metadata per table
- ✅ Scales well
- ✅ Relationship between sheets preserved

### Option 2: Grouped by Category
```
Firebase Root
└── contas_nacionais/
    ├── volume_indices/
    │   ├── table_1620/
    │   └── table_1621/
    ├── valores/
    │   ├── table_1846/
    │   ├── table_6612/
    │   └── table_6613/
    ├── taxas/
    │   ├── table_5932/
    │   ├── table_6726/
    │   └── table_6727/
    └── contas_economicas/
        └── table_2072/
```

**Benefits:**
- ✅ Logical grouping by data type
- ✅ Easy to find related tables
- ✅ Better for visualization apps

### Option 3: Hybrid (Best for Large Scale)
```
Firebase Root
└── ibge/
    ├── tables/
    │   ├── 1620/
    │   ├── 1621/
    │   └── ...
    ├── metadata/
    │   └── tables_list (index of all tables with names, descriptions)
    └── categories/
        └── contas_nacionais_trimestrais/
            └── table_ids: [1620, 1621, ...]
```

## Recommendations

### For Your Use Case:
1. **Use Option 1 (Nested by Table)** - Simple, clear, works well for current scale
2. **Add Metadata**: Store table name, description, period range per table
3. **Use Short Keys**: Use table numbers as keys, store full names in metadata
4. **Preserve Sheet Relationships**: For multi-sheet tables, nest sheets under table

### Implementation:
- Change base path to `ibge_data/<category>/table_{number}/` (e.g., `ibge_data/cnt/table_1620/`)
- For multi-sheet: `ibge_data/<category>/table_{number}/sheets/{sheet_name}/`
- Add metadata node: `ibge_data/<category>/table_{number}/metadata/`

## Migration Path

If you want to change the structure:
1. Update `upload_to_firebase()` to use nested paths
2. Re-run all scripts (they will overwrite with new structure)
3. Optionally, keep old structure for backward compatibility during transition

