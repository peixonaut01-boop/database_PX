# 🎯 Sistema de Atualização Inteligente

## Como Funciona

### 1. Verificação Semanal do Calendário
- **Quando**: Toda segunda-feira às 8h BRT
- **O que**: Consulta API de calendário do IBGE (`/api/v3/calendario/`)
- **Busca**: Divulgações agendadas para a semana atual

### 2. Detecção Automática de Datasets
O sistema identifica automaticamente qual dataset corresponde a cada divulgação:

| Dataset | Palavras-chave no Calendário |
|---------|------------------------------|
| IPCA | "IPCA", "Índice Nacional de Preços ao Consumidor Amplo" |
| INPC | "INPC", "Índice Nacional de Preços ao Consumidor" |
| IPCA15 | "IPCA-15", "IPCA 15" |
| IPP | "IPP", "Índice de Preços ao Produtor" |
| PIMPF | "PIM", "Produção Industrial", "PIM-PF" |
| PMC | "PMC", "Pesquisa Mensal de Comércio" |
| PMS | "PMS", "Pesquisa Mensal de Serviços" |
| PNADCM | "PNAD", "PNAD Contínua Mensal" |
| PNADCT | "PNAD", "PNAD Contínua Trimestral" |
| LSPA | "LSPA", "Levantamento Sistemático" |

### 3. Atualização Priorizada

**PRIORIDADE 1: Dados Nacionais** 🔴
- Séries com `territory_level=1` e `territory_id=1` (Brasil)
- Atualizadas PRIMEIRO
- Exemplo: IPCA Brasil antes de IPCA por estado

**PRIORIDADE 2: Dados Regionais** 🟡
- Séries estaduais, municipais, etc.
- Atualizadas DEPOIS dos nacionais
- Exemplo: IPCA por estado, IPCA por município

### 4. Processo de Atualização

Para cada dataset detectado no calendário:

1. **Fase 1**: Atualizar todas séries nacionais
2. **Fase 2**: Atualizar todas séries regionais
3. **Log**: Registrar sucessos e falhas
4. **Resultado**: Salvar em JSON para análise

## Exemplo de Execução

```
[INFO] Smart Update Scheduler - Starting
[INFO] Fetching calendar for this week...
[INFO] Found releases for 2 datasets:
[INFO]   ipca: 1 event(s)
[INFO]     - IPCA - Índice Nacional de Preços ao Consumidor Amplo on 10/11/2025 09:00:00
[INFO]   inpc: 1 event(s)
[INFO]     - INPC - Índice Nacional de Preços ao Consumidor on 10/11/2025 09:00:00
[INFO]
[INFO] Starting updates (NATIONAL series first, then REGIONAL)...
[INFO]
[INFO] Starting update for dataset: ipca
[INFO]   National series: 12,450
[INFO]   Regional series: 25,392
[INFO]   Phase 1: Updating 12,450 NATIONAL series...
[INFO]   Phase 2: Updating 25,392 REGIONAL series...
[INFO]   ✓ Updated: 37,842, Failed: 0
[INFO]
[INFO] UPDATE SUMMARY
[INFO] IPCA           - ✓ SUCCESS
[INFO] INPC           - ✓ SUCCESS
```

## Vantagens

✅ **Eficiência**: Só atualiza quando há divulgação real  
✅ **Priorização**: Dados nacionais sempre primeiro  
✅ **Automático**: Zero intervenção manual  
✅ **Rastreável**: Logs completos de cada atualização  

## Configuração

### GitHub Secrets

**Firebase:**
```
FIREBASE_CREDENTIALS = <JSON>
FIREBASE_DATABASE_URL = https://peixonaut01-2e0ba-default-rtdb.firebaseio.com/
```

**Email (para receber calendário semanal):**
```
EMAIL_FROM = noreply@peixonaut.com (ou seu email)
EMAIL_SMTP_HOST = smtp.gmail.com (ou seu servidor SMTP)
EMAIL_SMTP_PORT = 587
EMAIL_USER = seu-email@gmail.com
EMAIL_PASSWORD = sua-senha-de-app (para Gmail, use App Password)
```

**Nota para Gmail:**
1. Ative a verificação em 2 etapas
2. Gere uma "Senha de app" em: https://myaccount.google.com/apppasswords
3. Use essa senha no `EMAIL_PASSWORD`

**Destinatários:**
O email é enviado para: `lucasgmartins04@gmail.com` (configurado no código)

### Execução Manual
Você pode executar manualmente a qualquer momento:
- **Actions** → "Smart Update - Weekly Calendar Check" → **Run workflow**

## Tempo Estimado por Dataset

Com priorização (nacionais primeiro):

| Dataset | Séries Nacionais | Séries Regionais | Tempo Total |
|---------|------------------|------------------|-------------|
| IPCA | ~12,450 | ~25,392 | 12-18 horas |
| INPC | ~600 | ~1,194 | 40-60 minutos |
| PNADCT | ~8,000 | ~50,238 | 18-30 horas |

**Nota**: Dados nacionais são atualizados primeiro e ficam disponíveis mais rápido!

## Email Semanal

Toda segunda-feira, você receberá um email com:

✅ **Calendário da semana** - Todas as divulgações agendadas  
✅ **Status de atualização** - Quais datasets foram atualizados com sucesso  
✅ **Resumo estatístico** - Total de eventos e datasets  
✅ **Design responsivo** - Visualização clara e organizada  

O email é enviado mesmo quando não há divulgações (para informar que não há atualizações na semana).

## Próximos Passos

- [x] Adicionar notificações por email quando atualização completar
- [ ] Dashboard de status das atualizações
- [ ] Métricas de tempo de atualização
- [ ] Alertas para falhas críticas

---

**Resultado**: Sistema que atualiza automaticamente baseado no calendário oficial do IBGE e envia email semanal! 📅📧

