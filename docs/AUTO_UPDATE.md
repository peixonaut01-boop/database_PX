# 🔄 Sistema de Atualização Automática

## Visão Geral

Sistema simples de atualização automática usando GitHub Actions para manter os dados atualizados.

## Workflows Configurados

### `auto_update_ibge.yaml`
- **Frequência**: Diariamente às 10h BRT (13h UTC)
- **Datasets**: IPCA, INPC, IPCA15 (principais indicadores mensais)
- **Ação**: Atualiza séries usando `ingest_flat_series.py --resume`

## Como Funciona

1. **GitHub Actions** roda diariamente
2. **Verifica** se há novos dados disponíveis
3. **Atualiza** apenas séries que mudaram (usando `--resume`)
4. **Verifica** completude após atualização

## Configuração

### GitHub Secrets Necessários

No GitHub → Settings → Secrets and variables → Actions:

```
FIREBASE_CREDENTIALS = <JSON das credenciais do Firebase>
FIREBASE_DATABASE_URL = https://peixonaut01-2e0ba-default-rtdb.firebaseio.com/
```

## Adicionar Mais Datasets

Para adicionar mais datasets ao workflow, edite `.github/workflows/auto_update_ibge.yaml`:

```yaml
- name: Update PMC
  env:
    FIREBASE_CREDENTIALS: ${{ secrets.FIREBASE_CREDENTIALS }}
    FIREBASE_DATABASE_URL: ${{ secrets.FIREBASE_DATABASE_URL }}
  run: |
    python scripts/ingest_flat_series.py --dataset pmc --resume --workers 10
```

## Execução Manual

Você pode executar manualmente:
1. Vá em **Actions** no GitHub
2. Selecione "Auto Update - IBGE Datasets"
3. Clique em **Run workflow**

## Logs

- **GitHub Actions**: Logs disponíveis na aba Actions
- **Local**: Execute manualmente para ver logs em tempo real

## Próximos Passos

- Adicionar mais datasets ao schedule
- Configurar notificações de falhas
- Dashboard de status

