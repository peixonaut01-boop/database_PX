# 📧 Configuração de Email - Guia Completo

## Como Acessar os GitHub Secrets

### Método 1: Via Interface do GitHub (Recomendado)

1. **Acesse o repositório:**
   ```
   https://github.com/peixonaut01-boop/database_PX
   ```

2. **Vá para Settings:**
   - Clique na aba **Settings** no topo do repositório
   - Se não ver a aba Settings, você precisa de permissões de administrador

3. **Acesse Secrets:**
   - No menu lateral esquerdo, procure por **"Secrets and variables"**
   - Clique em **"Actions"**
   - Você verá a lista de secrets existentes

4. **Adicione os Secrets:**
   - Clique em **"New repository secret"**
   - Adicione cada um dos secrets abaixo:

### Secrets Necessários para Email

| Secret Name | Valor | Descrição |
|------------|-------|-----------|
| `EMAIL_FROM` | `seu-email@gmail.com` | Email remetente |
| `EMAIL_SMTP_HOST` | `smtp.gmail.com` | Servidor SMTP |
| `EMAIL_SMTP_PORT` | `587` | Porta SMTP (TLS) |
| `EMAIL_USER` | `seu-email@gmail.com` | Usuário SMTP |
| `EMAIL_PASSWORD` | `senha-de-app` | **Senha de App do Gmail** |

### Configuração do Gmail

**⚠️ IMPORTANTE:** Para Gmail, você NÃO pode usar sua senha normal!

1. **Ative a verificação em 2 etapas:**
   - Acesse: https://myaccount.google.com/security
   - Ative "Verificação em duas etapas"

2. **Gere uma Senha de App:**
   - Acesse: https://myaccount.google.com/apppasswords
   - Selecione "App": Mail
   - Selecione "Device": Other (Custom name)
   - Digite: "GitHub Actions"
   - Clique em "Generate"
   - **Copie a senha gerada** (16 caracteres, sem espaços)
   - Use essa senha no secret `EMAIL_PASSWORD`

### Exemplo de Configuração

```
EMAIL_FROM = lucasgmartins04@gmail.com
EMAIL_SMTP_HOST = smtp.gmail.com
EMAIL_SMTP_PORT = 587
EMAIL_USER = lucasgmartins04@gmail.com
EMAIL_PASSWORD = abcd efgh ijkl mnop  (senha de app gerada)
```

## Verificação

Após configurar os secrets:

1. Vá para **Actions** no repositório
2. Execute manualmente o workflow "Smart Update - Weekly Calendar Check"
3. Verifique os logs para ver se o email foi enviado
4. Verifique sua caixa de entrada (e spam) em `lucasgmartins04@gmail.com`

## Troubleshooting

### Erro 404 ao acessar Secrets
- Certifique-se de estar logado no GitHub
- Verifique se você tem permissões de administrador no repositório
- Tente acessar via: Settings → Secrets and variables → Actions

### Email não está sendo enviado
- Verifique os logs do workflow em Actions
- Confirme que todos os secrets estão configurados
- Verifique se a senha de app está correta (sem espaços)
- Teste as credenciais localmente primeiro

### Email vai para Spam
- Adicione o remetente aos contatos
- Verifique a pasta de spam
- Considere usar um email profissional (não Gmail)

## Teste Local (Opcional)

Para testar o envio de email localmente antes de configurar no GitHub:

```python
# Criar arquivo .env com:
EMAIL_FROM=lucasgmartins04@gmail.com
EMAIL_SMTP_HOST=smtp.gmail.com
EMAIL_SMTP_PORT=587
EMAIL_USER=lucasgmartins04@gmail.com
EMAIL_PASSWORD=sua-senha-de-app

# Executar:
python scripts/smart_update_scheduler.py
```

---

**Pronto!** Após configurar, você receberá emails toda segunda-feira às 8h BRT! 📧

