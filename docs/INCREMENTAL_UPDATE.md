# ⚡ Atualização Incremental - Como Funciona

## O Problema

Atualizar todas as séries toda vez é **lento**:
- IPCA: ~50 anos de dados = ~600 pontos
- PNADCT: Milhares de séries com décadas de histórico
- Re-baixar tudo toda vez = desperdício de tempo e recursos

## A Solução: Atualização Incremental

### Como Funciona

1. **Verifica última atualização**
   ```python
   last_date = get_last_date_from_firebase(px_code)
   # Exemplo: "2025-10-01"
   ```

2. **Modifica URL da API SIDRA**
   ```python
   # Antes: /p/all (todos os períodos)
   # Depois: /p/202510-202511 (apenas novos períodos)
   incremental_url = build_incremental_api_url(api_url, last_date)
   ```

3. **Busca apenas novos dados**
   - API retorna apenas períodos após `last_date`
   - Muito menos dados = muito mais rápido

4. **Mescla com dados existentes**
   ```python
   existing_values = existing_data['values']
   new_values = fetch_incremental_series_data(...)
   merged = {**existing_values, **new_values}
   ```

5. **Atualiza Firebase**
   - Apenas escreve dados mesclados
   - Não sobrescreve série completa

## Ganhos de Performance

| Dataset | Séries | Pontos Totais | Pontos Novos (mensal) | Ganho |
|---------|--------|---------------|----------------------|-------|
| IPCA | 37,842 | ~22M pontos | ~37K pontos | **600x mais rápido** |
| INPC | 1,794 | ~1M pontos | ~1.8K pontos | **550x mais rápido** |
| PNADCT | 58,238 | ~50M pontos | ~58K pontos | **860x mais rápido** |

**Tempo estimado:**
- **Antes**: 2-4 horas para atualizar IPCA completo
- **Agora**: 2-5 minutos para atualizar apenas novos dados

## Uso

### Script Incremental
```bash
# Atualizar apenas novos dados
python scripts/ingest_flat_series_incremental.py --dataset ipca --workers 10
```

### Comparação

**Versão Completa** (re-baixa tudo):
```bash
python scripts/ingest_flat_series.py --dataset ipca --resume --workers 10
# Tempo: ~2-4 horas
```

**Versão Incremental** (apenas novos):
```bash
python scripts/ingest_flat_series_incremental.py --dataset ipca --workers 10
# Tempo: ~2-5 minutos
```

## Quando Usar Cada Versão

### Use Incremental (padrão):
- ✅ Atualizações automáticas diárias/mensais
- ✅ Séries já existentes no Firebase
- ✅ Quando você quer velocidade

### Use Completa:
- ⚠️ Primeira ingestão (não há dados anteriores)
- ⚠️ Correção de dados históricos
- ⚠️ Re-sincronização completa

## Fallback Automático

O scheduler automático:
1. Tenta versão incremental primeiro
2. Se falhar, tenta versão completa
3. Loga ambos os resultados

## Limitações

- **API SIDRA**: Nem todas as tabelas suportam range de períodos
  - Se não suportar, fallback para busca completa
- **Primeira vez**: Sem `last_date`, busca tudo (normal)
- **Gaps**: Se houver gaps na série, pode precisar busca completa

## Exemplo de Output

```
[INFO] Incremental update for 'ipca': 37,842 series
[INFO] This will be FAST - only fetching new data since last update!

[OK] 1/37842 PX_5A3839DC0F64 | Last update: 2025-10-01, fetching only new data...
[OK] 1/37842 PX_5A3839DC0F64 | Updated: +1 new points (total: 601)

[OK] 2/37842 PX_6B4930ED1G75 | No new data (last: 2025-10-01)

...

SUMMARY
Total series:     37,842
Updated:          35,120
No new data:      2,500
Failed:           222

⚡ Incremental update completed! Much faster than full re-ingestion.
```

---

**Resultado**: Atualizações automáticas que antes levavam horas agora levam minutos! 🚀

