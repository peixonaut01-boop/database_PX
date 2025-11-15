# 🗺️ Roadmap de Desenvolvimento - Peixonaut01

**Repositório:** [peixonaut01-boop](https://github.com/peixonaut01-boop)  
**Última atualização:** Novembro 2025

## 📊 Status Atual do Projeto

### ✅ Concluído
- **Ingestão IBGE:** Sistema completo de ingestão de dados do IBGE
- **Estrutura Flat:** Nova arquitetura de dados otimizada para app development
- **Catálogo de Séries:** 123,574 séries catalogadas e organizadas
- **Datasets Ingeridos:**
  - ✅ INPC (1,794 séries - 100%)
  - ✅ IPCA (37,842 séries - 100%)
  - ✅ IPCA15 (1,794 séries - 100%)
  - ✅ IPP (220 séries - 100%)
  - ✅ PIMPF (1,914 séries - 100%)
  - ✅ PMC (3,672 séries - 100%)
  - ✅ PMS (888 séries - 100%)
  - ✅ PNADCM (572 séries - 100%)
  - ⚠️ LSPA (14,053/16,640 séries - 84.5%) - 2,587 faltantes (Safra 2024/2025 bloqueadas pelo IBGE)
  - ⚠️ PNADCT (58,233/58,238 séries - 99.99%) - 5 séries faltantes (erros de conexão)

- **API FastAPI:** Estrutura básica implementada
- **Firebase Integration:** Configuração completa com novo projeto
- **Scripts de Utilidade:** Verificação, retry, catalogação

---

## 🎯 Fase 1: Finalização e Estabilização (Sprint 1-2)

### 1.1 Completar Ingestão de Dados IBGE
**Prioridade:** 🔴 Alta  
**Estimativa:** 1-2 semanas

- [ ] **Retry LSPA Safra 2024/2025**
  - Monitorar liberação dos dados pelo IBGE
  - Implementar job agendado para retry automático
  - Documentar limitações conhecidas

- [ ] **Resolver 5 séries PNADCT faltantes**
  - Investigar erros de conexão específicos
  - Implementar retry com backoff exponencial
  - Considerar rate limiting mais conservador

- [ ] **Validação Final**
  - Script de validação completa de todos os datasets
  - Relatório de integridade dos dados
  - Verificação de gaps temporais

### 1.2 Ingestão de Dados do STN (Secretaria do Tesouro Nacional) - RTN
**Prioridade:** 🔴 Alta  
**Estimativa:** 2-3 semanas

**Objetivo: Ingerir dados do Boletim Resultado do Tesouro Nacional (RTN) mensalmente**

- [ ] **Scraping Automatizado**
  - Finalizar script de download automático do Excel RTN
  - Implementar detecção de novos boletins mensais
  - Tratamento de diferentes formatos de tabelas (27 tabelas identificadas)
  - Integração com API do Tesouro Transparente

- [ ] **Parser de Tabelas Excel**
  - Extrair dados de todas as 27 tabelas identificadas:
    - Resultado Primário (valores correntes e % PIB)
    - Investimento do Governo Central
    - Custeio Administrativo
    - Transferências e despesas
    - Valores a preços constantes
    - Demonstrativos de operações
  - Normalizar estrutura de dados
  - Mapear para formato flat_series

- [ ] **Ingestão no Firebase**
  - Adaptar `ingest_flat_series.py` para dados do STN
  - Criar dataset "stn" ou "rtn" no catálogo
  - Implementar atualização mensal automática
  - Tratamento de séries históricas

- [ ] **Validação e Testes**
  - Comparar dados extraídos com fonte original
  - Testes de integridade
  - Documentação do processo
  - Verificação de consistência temporal

### 1.3 Melhorias na API FastAPI
**Prioridade:** 🟡 Média  
**Estimativa:** 1-2 semanas

- [ ] **Endpoints Adicionais**
  - `/series/search` - Busca por texto (label, general_name)
  - `/series/filter` - Filtros avançados (dataset, período, etc)
  - `/datasets` - Lista de datasets disponíveis
  - `/datasets/{dataset}/stats` - Estatísticas por dataset

- [ ] **Otimizações**
  - Cache de consultas frequentes
  - Paginação em endpoints de listagem
  - Compressão de respostas (gzip)

- [ ] **Documentação API**
  - Swagger/OpenAPI completo
  - Exemplos de uso
  - Guia de integração

---

## 🚀 Fase 2: Expansão e Novas Funcionalidades (Sprint 3-4)

### 2.1 Ingestão de Dados do BACEN (Banco Central do Brasil)
**Prioridade:** 🔴 Alta  
**Estimativa:** 3-4 semanas

**Objetivo: Ingerir séries históricas e indicadores financeiros do Banco Central**

- [ ] **Análise da API/SIDRA do BACEN**
  - Identificar endpoints disponíveis
  - Mapear séries de interesse:
    - Taxa Selic (meta e efetiva)
    - CDI (Certificado de Depósito Interbancário)
    - IGP-M (Índice Geral de Preços - Mercado)
    - Taxa de câmbio (dólar, euro, outras moedas)
    - Reservas internacionais
    - Base monetária
    - Meios de pagamento (M1, M2, M3, M4)
    - Dívida líquida do setor público
    - Indicadores de crédito
  - Documentar estrutura de dados

- [ ] **Desenvolvimento de Scripts de Ingestão**
  - Criar `scripts/ingest_bacen_series.py`
  - Implementar parser para diferentes formatos de dados
  - Tratamento de frequências (diária, semanal, mensal)
  - Normalização de datas e valores

- [ ] **Integração com Firebase**
  - Adaptar para estrutura flat_series
  - Criar dataset "bacen" no catálogo
  - Implementar atualização automática (frequência variável por série)
  - Tratamento de atualizações intraday (para séries diárias)

- [ ] **Validação e Monitoramento**
  - Comparação com dados oficiais
  - Alertas para falhas de atualização
  - Verificação de consistência temporal
  - Documentação completa

### 2.2 Outras Fontes de Dados (Futuro)
**Prioridade:** 🟢 Baixa  
**Estimativa:** 3-4 semanas

- [ ] **ANP (Agência Nacional do Petróleo)**
  - Preços de combustíveis
  - Produção de petróleo

- [ ] **IPEA Data**
  - Séries históricas consolidadas
  - Indicadores econômicos

### 2.3 Sistema de Atualização Automática
**Prioridade:** 🟡 Média  
**Estimativa:** 2 semanas

- [ ] **Scheduler Inteligente**
  - Detecção automática de novos dados
  - Priorização de datasets críticos
  - Retry automático de falhas

- [ ] **Notificações**
  - Alertas de falhas de ingestão
  - Notificações de novos dados disponíveis
  - Dashboard de status

- [ ] **Monitoramento**
  - Health checks dos endpoints
  - Métricas de performance
  - Logs estruturados

### 2.4 Melhorias na Estrutura de Dados
**Prioridade:** 🟡 Média  
**Estimativa:** 1-2 semanas

- [ ] **Metadados Enriquecidos**
  - Tags e categorias
  - Relacionamentos entre séries
  - Frequência de atualização

- [ ] **Versionamento**
  - Histórico de mudanças
  - Rollback de dados
  - Auditoria

- [ ] **Otimização de Storage**
  - Compressão de dados históricos
  - Arquivo de dados antigos
  - Limpeza automática

---

## 🔧 Fase 3: Qualidade e Infraestrutura (Sprint 5-6)

### 3.1 Testes e Qualidade
**Prioridade:** 🟡 Média  
**Estimativa:** 2-3 semanas

- [ ] **Testes Unitários**
  - Cobertura mínima de 70%
  - Testes de parsers
  - Testes de normalização

- [ ] **Testes de Integração**
  - Testes end-to-end de ingestão
  - Testes de API
  - Testes de Firebase

- [ ] **Validação de Dados**
  - Schemas de validação
  - Detecção de outliers
  - Verificação de consistência

### 3.2 CI/CD e DevOps
**Prioridade:** 🟡 Média  
**Estimativa:** 1-2 semanas

- [ ] **GitHub Actions Melhorado**
  - Pipeline completo de testes
  - Deploy automático
  - Rollback automático em caso de falha

- [ ] **Containerização**
  - Docker para API
  - Docker Compose para ambiente local
  - Kubernetes (opcional)

- [ ] **Ambientes**
  - Staging environment
  - Production environment
  - Feature flags

### 3.3 Documentação
**Prioridade:** 🟢 Baixa  
**Estimativa:** 1-2 semanas

- [ ] **Documentação Técnica**
  - Arquitetura do sistema
  - Guia de desenvolvimento
  - Guia de contribuição

- [ ] **Documentação de Dados**
  - Glossário completo
  - Dicionário de dados
  - Guia de uso da API

- [ ] **Tutoriais**
  - Como adicionar nova fonte de dados
  - Como criar novos endpoints
  - Como fazer deploy

---

## 📱 Fase 4: Aplicação e Visualização (Sprint 7+)

### 4.1 Dashboard Web
**Prioridade:** 🟢 Baixa  
**Estimativa:** 4-6 semanas

- [ ] **Interface de Usuário**
  - Dashboard principal
  - Visualização de séries
  - Filtros e buscas

- [ ] **Gráficos Interativos**
  - Visualização temporal
  - Comparação de séries
  - Exportação de gráficos

- [ ] **Análises**
  - Correlações entre séries
  - Análises estatísticas básicas
  - Previsões simples

### 4.2 Mobile App
**Prioridade:** 🟢 Baixa  
**Estimativa:** 8-12 semanas

- [ ] **App React Native / Flutter**
  - Listagem de séries
  - Visualização de dados
  - Favoritos

- [ ] **Notificações Push**
  - Alertas de novos dados
  - Lembretes personalizados

---

## 🎯 Prioridades Imediatas (Próximas 2 Semanas)

1. **Resolver 5 séries PNADCT faltantes** ⚡
2. **Finalizar scraping e ingestão STN (RTN)** ⚡
3. **Iniciar ingestão de dados do BACEN** ⚡
4. **Melhorar endpoints da API** ⚡
5. **Documentação básica da API** 📝

---

## 📈 Métricas de Sucesso

### Fase 1
- ✅ 100% das séries IBGE ingeridas (exceto bloqueios conhecidos)
- ✅ STN (RTN) mensal automatizado
- ✅ BACEN com principais séries ingeridas
- ✅ API com 5+ endpoints funcionais

### Fase 2
- ✅ BACEN completamente integrado
- ✅ STN (RTN) funcionando com atualização mensal
- ✅ Sistema de atualização automática funcionando
- ✅ 99.9% uptime da API

### Fase 3
- ✅ 70%+ cobertura de testes
- ✅ Pipeline CI/CD completo
- ✅ Documentação completa

---

## 🔄 Processo de Desenvolvimento

### Sprints
- **Duração:** 2 semanas
- **Revisão:** Final de cada sprint
- **Planning:** Início de cada sprint

### Branching Strategy
- `main` - Produção
- `develop` - Desenvolvimento
- `feature/*` - Novas funcionalidades
- `fix/*` - Correções

### Code Review
- Todas as PRs requerem review
- Testes obrigatórios para novas features
- Documentação atualizada

---

## 📝 Notas

- **LSPA Safra 2024/2025:** Dados bloqueados pelo IBGE, monitorar liberação
- **Rate Limiting:** Implementar estratégias mais conservadoras para evitar bloqueios
- **Firebase Costs:** Monitorar uso de storage e otimizar quando necessário
- **API Performance:** Implementar cache e otimizações conforme necessário

---

## 🤝 Contribuindo

Para contribuir com este projeto:
1. Fork o repositório
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra uma Pull Request

---

**Última revisão:** Novembro 2025  
**Próxima revisão:** Dezembro 2025

