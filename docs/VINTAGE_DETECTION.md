# 📊 Detecção de Vintages (Revisões Históricas)

## Por que Vintages?

Dados econômicos são frequentemente **revisados** pelo IBGE e outras fontes:
- Dados preliminares → Dados revisados
- Correções de erros históricos
- Mudanças metodológicas
- Ajustes sazonais recalculados

**Exemplo**: IPCA de janeiro pode ser publicado como 0.54%, depois revisado para 0.56% no mês seguinte.

## Como Funciona

### 1. Busca Série Completa
- Sempre busca **toda a série** da API SIDRA
- Não usa atualização incremental (precisamos comparar tudo)

### 2. Compara com Versão Anterior
```python
old_values = existing_data['values']  # Versão anterior no Firebase
new_values = fetch_series_data(api_url)  # Nova versão da API

comparison = compare_series(old_values, new_values)
```

### 3. Detecta Mudanças
- **Added**: Novos períodos adicionados
- **Removed**: Períodos removidos
- **Changed**: Valores revisados (com diferença e % de mudança)

### 4. Salva Vintage
Antes de atualizar, salva versão anterior em:
```
flat_series/{px_code}/vintages/vintage_{timestamp}/
```

## Estrutura no Firebase

```
flat_series/
  PX_5A3839DC0F64/
    metadata/
      - label, dataset, api_url, etc.
      - vintage_comparison: {
          added: ["2025-11-01"],
          removed: [],
          changed: [
            {
              date: "2025-10-01",
              old_value: 0.54,
              new_value: 0.56,
              change: 0.02,
              change_pct: 3.70
            }
          ],
          unchanged_count: 598,
          total_changes: 2
        }
    values/
      - 2025-01-01: 0.52
      - 2025-02-01: 0.54
      - 2025-10-01: 0.56  # ← Revisado!
      - ...
    vintages/
      vintage_2025-11-15T10-30-00/
        timestamp: "2025-11-15T10:30:00"
        values: { ... }  # Versão anterior completa
        metadata_snapshot: { ... }
```

## Análise de Revisões

### Exemplo de Output

```
[OK] 1/37842 PX_5A3839DC0F64 | Updated: +1 new, ~2 revised (vintage saved)
[OK] 2/37842 PX_6B4930ED1G75 | No changes: 601 points (unchanged)
[OK] 3/37842 PX_7C5041FE2H86 | Updated: ~5 revised (vintage saved)

SUMMARY
Total series:        37,842
Success:             37,620
  - First ingestion: 0
  - With revisions:  1,234
  - No changes:      36,386
Failed:              222

📊 1,234 series had revisions - vintages saved for analysis
```

## Uso

### Atualização com Detecção de Vintages
```bash
python scripts/ingest_flat_series_with_vintages.py --dataset ipca --resume --workers 10
```

### Análise de Vintages (Futuro)
```python
# Buscar histórico de vintages
ref = get_reference("flat_series/PX_5A3839DC0F64/vintages")
vintages = ref.get()

# Comparar versões ao longo do tempo
for vintage_key, vintage_data in vintages.items():
    timestamp = vintage_data['timestamp']
    values = vintage_data['values']
    # Analisar mudanças...
```

## Casos de Uso

### 1. Detectar Revisões Recentes
- Identificar quais séries foram revisadas na última atualização
- Alertar sobre mudanças significativas

### 2. Análise de Qualidade
- Quantas séries são revisadas com frequência?
- Qual a magnitude típica das revisões?

### 3. Precisão de Dados
- Comparar dados preliminares vs revisados
- Calcular viés sistemático

### 4. Auditoria
- Rastreabilidade completa de mudanças
- Histórico de todas as versões

## Performance

**Nota**: Esta versão busca série completa (não incremental), então:
- Mais lenta que versão incremental
- Mas necessária para detectar revisões
- Ainda eficiente com processamento paralelo

**Tempo estimado**:
- IPCA completo: 2-4 horas (com workers=10)
- Mas detecta todas as revisões!

## Próximos Passos

1. ✅ Detecção de vintages implementada
2. ⏳ Dashboard de revisões
3. ⏳ Alertas de mudanças significativas
4. ⏳ API para consultar histórico de vintages
5. ⏳ Análise estatística de padrões de revisão

---

**Resultado**: Sistema completo de rastreamento de revisões históricas! 📊

