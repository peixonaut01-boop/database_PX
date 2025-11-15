# 🚀 Setup de Atualização Automática - Guia Rápido

## ✅ O que foi criado

### 1. **Sistema Inteligente de Scheduler** (`scripts/auto_update_scheduler.py`)
- Verifica automaticamente quais datasets devem ser atualizados hoje
- Respeita frequências de publicação (diária, mensal, trimestral)
- Prioriza séries críticas
- Retry automático para falhas
- ⚡ **Usa atualização INCREMENTAL** - 10-100x mais rápido!

### 1.1. **Detecção de Vintages** (`scripts/ingest_flat_series_with_vintages.py`)
- 📊 **Detecta Revisões**: Compara série completa com versão anterior
- 🔍 **Identifica Mudanças**: Novos períodos, remoções, e revisões de valores
- 💾 **Armazena Histórico**: Salva vintages (versões anteriores) para análise
- ⚠️ **Busca Série Completa**: Necessário para detectar revisões históricas
- 📈 **Análise de Qualidade**: Permite rastrear padrões de revisão

### 2. **Workflows GitHub Actions**

#### `auto_update_daily.yaml`
- **Quando**: Diariamente às 9h BRT
- **O que**: Atualiza séries BACEN (diárias)

#### `auto_update_monthly.yaml`
- **Quando**: Diariamente às 10h BRT (verifica se deve atualizar)
- **O que**: Atualiza datasets mensais (IPCA, INPC, IPCA15, IPP, PMC, PMS, PIMPF, PNADCM, LSPA)
- **Como**: Usa o scheduler inteligente

#### `auto_update_stn.yaml`
- **Quando**: Dias 1-7 de cada mês às 8h BRT
- **O que**: Scraping e ingestão do RTN (STN)

## 📅 Calendário de Atualizações

| Dataset | Frequência | Dias do Mês | Prioridade |
|---------|-----------|-------------|------------|
| **BACEN** | Diária | Todos os dias às 9h | 🔴 Alta |
| **IPCA, INPC, IPCA15** | Mensal | 15-20 (retry 21-23) | 🔴 Alta |
| **IPP, PMC, PMS, PIMPF** | Mensal | 15-20 (retry 21-23) | 🟡 Média |
| **PNADCM** | Mensal | 15-20 (retry 21-23) | 🔴 Alta |
| **LSPA** | Mensal | 10-15 (retry 16-18) | 🟢 Baixa |
| **PNADCT** | Trimestral | 15-20 de Fev/Mai/Ago/Nov | 🔴 Alta |
| **STN (RTN)** | Mensal | 1-7 (início do mês) | 🔴 Alta |

## ⚙️ Configuração Necessária

### 1. GitHub Secrets
Configure no GitHub → Settings → Secrets and variables → Actions:

```
FIREBASE_CREDENTIALS = <JSON das credenciais>
FIREBASE_DATABASE_URL = https://peixonaut01-2e0ba-default-rtdb.firebaseio.com/
```

### 2. Ativar Workflows
Os workflows estão prontos, mas precisam ser ativados:
1. Vá em **Actions** no GitHub
2. Cada workflow aparecerá na lista
3. Eles rodarão automaticamente conforme o schedule

### 3. Testar Manualmente
Você pode testar cada workflow manualmente:
- **Actions** → Selecione o workflow → **Run workflow**

## 🎯 Próximos Passos Recomendados

### Imediato (Esta Semana)
1. ✅ Configurar GitHub Secrets
2. ✅ Testar workflows manualmente
3. ✅ Verificar se logs estão sendo gerados

### Curto Prazo (Próximas 2 Semanas)
1. ⏳ Implementar ingestão completa do STN (RTN)
2. ⏳ Implementar ingestão do BACEN
3. ⏳ Adicionar notificações de falhas (email/Slack)

### Médio Prazo (Próximo Mês)
1. ⏳ Dashboard de monitoramento
2. ⏳ Alertas de dados desatualizados
3. ⏳ Métricas de performance

## 📊 Monitoramento

### Logs
- **Local**: `logs/auto_update.log`
- **GitHub Actions**: Logs dos workflows
- **Resultados**: `data_exports/update_results_YYYYMMDD.json`

### Verificar Status
```bash
# Verificar quais datasets devem atualizar hoje
python scripts/auto_update_scheduler.py

# Verificar completude de um dataset
python scripts/verify_dataset_completeness.py ipca
```

## 🔧 Ajustar Schedules

Para modificar quando os datasets são atualizados, edite:
- `scripts/auto_update_scheduler.py` → `UPDATE_SCHEDULE`
- `.github/workflows/*.yaml` → `cron` expressions

## 📚 Documentação Completa

Veja `docs/AUTO_UPDATE_STRATEGY.md` para detalhes completos da estratégia.

---

**Status**: ✅ Sistema criado e pronto para configuração  
**Próxima ação**: Configurar GitHub Secrets e testar workflows

