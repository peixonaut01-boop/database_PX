# 🔄 Estratégia de Atualização Automática

## Visão Geral

Sistema inteligente de atualização automática que respeita as frequências de publicação de cada dataset e prioriza séries críticas.

## Frequências de Atualização

### Diárias
- **BACEN**: Séries financeiras críticas (Selic, câmbio, reservas)
  - **Horário**: 9h BRT diariamente
  - **Workflow**: `auto_update_daily.yaml`

### Mensais
- **IPCA, INPC, IPCA15**: Publicados no dia 10-12 do mês
  - **Atualização**: Dias 15-20 do mês
  - **Retry**: Dias 21-23 se falhar
  
- **IPP, PMC, PMS, PIMPF**: Publicados no início/mitad do mês
  - **Atualização**: Dias 15-20 do mês
  - **Retry**: Dias 21-23 se falhar

- **PNADCM**: Rolling quarter (mensal)
  - **Atualização**: Dias 15-20 do mês
  - **Retry**: Dias 21-23 se falhar

- **LSPA**: Publicado no início do mês
  - **Atualização**: Dias 10-15 do mês
  - **Retry**: Dias 16-18 se falhar

- **STN (RTN)**: Publicado no final do mês anterior
  - **Atualização**: Dias 1-7 do mês
  - **Workflow**: `auto_update_stn.yaml`

### Trimestrais
- **PNADCT**: Publicado trimestralmente
  - **Atualização**: Dias 15-20 de Fevereiro, Maio, Agosto, Novembro
  - **Retry**: Dias 21-25 se falhar

## Workflows GitHub Actions

### 1. `auto_update_daily.yaml`
- **Frequência**: Diária às 9h BRT
- **Datasets**: BACEN
- **Ação**: Atualiza séries diárias do Banco Central

### 2. `auto_update_monthly.yaml`
- **Frequência**: Diária às 10h BRT (verifica se deve atualizar)
- **Datasets**: Todos os mensais (IPCA, INPC, IPCA15, IPP, PMC, PMS, PIMPF, PNADCM, LSPA)
- **Ação**: Usa `auto_update_scheduler.py` para decidir quais datasets atualizar

### 3. `auto_update_stn.yaml`
- **Frequência**: Dias 1-7 de cada mês às 8h BRT
- **Datasets**: STN (RTN)
- **Ação**: Scraping e ingestão do Boletim RTN

## Sistema Inteligente

O script `auto_update_scheduler.py`:

1. **Verifica Calendário**: Determina quais datasets devem ser atualizados hoje
2. **Verifica Prioridade**: Ordena por importância (high > medium > low)
3. **Verifica Retry**: Se está em dia de retry, verifica se última atualização falhou
4. **Executa Atualização INCREMENTAL**: ⚡ **MUITO MAIS RÁPIDO!**
   - Usa `ingest_flat_series_incremental.py`
   - Busca apenas novos dados desde última atualização
   - Não re-baixa série histórica completa
   - Mescla novos dados com existentes
5. **Registra Resultados**: Salva logs e resultados em JSON

### ⚡ Otimização Incremental

**Por que é mais rápido?**
- **Antes**: Re-baixava todas as séries toda vez (ex: 50 anos de IPCA = milhares de pontos)
- **Agora**: Busca apenas novos períodos (ex: 1 mês novo = 1 ponto)
- **Ganho**: 10-100x mais rápido dependendo do tamanho da série

**Como funciona:**
1. Verifica última data no Firebase
2. Modifica URL SIDRA para buscar apenas períodos após essa data
3. Mescla novos dados com existentes
4. Atualiza Firebase apenas com dados novos

## Configuração

### Variáveis de Ambiente Necessárias

No GitHub Secrets:
- `FIREBASE_CREDENTIALS`: JSON das credenciais do Firebase
- `FIREBASE_DATABASE_URL`: URL do Firebase Realtime Database

### Ajustar Schedules

Edite `scripts/auto_update_scheduler.py` para modificar:
- Dias do mês para atualização
- Prioridades
- Frequências

## Monitoramento

### Logs
- **Local**: `logs/auto_update.log`
- **GitHub Actions**: Logs dos workflows
- **Resultados**: `data_exports/update_results_YYYYMMDD.json`

### Notificações (Futuro)
- Email/Slack quando atualizações falharem
- Dashboard de status
- Alertas de dados desatualizados

## Retry Strategy

1. **Primeira Tentativa**: Dia programado
2. **Retry Automático**: Dias de retry configurados
3. **Retry Manual**: Via `workflow_dispatch` no GitHub Actions
4. **Logs de Falha**: Salvos em `data_exports/failed_ingestion_*.json`

## Exemplo de Uso Manual

```bash
# Atualizar dataset específico
python scripts/ingest_flat_series.py --dataset ipca --resume --workers 10

# Rodar scheduler manualmente
python scripts/auto_update_scheduler.py

# Verificar status
python scripts/verify_dataset_completeness.py ipca
```

## Próximos Passos

1. ✅ Configurar workflows básicos
2. ⏳ Implementar notificações
3. ⏳ Dashboard de monitoramento
4. ⏳ Alertas de dados desatualizados
5. ⏳ Métricas de performance

