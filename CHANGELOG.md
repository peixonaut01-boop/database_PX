# Changelog

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [Unreleased]

### Adicionado
- Roadmap de desenvolvimento completo
- Templates de issues para GitHub
- Scripts de scraping para Tesouro Transparente (RTN)
- Sistema de retry para séries falhadas
- Verificação de completude de datasets

### Em Progresso
- Ingestão completa de dados RTN
- Melhorias na API FastAPI
- Resolução de 5 séries PNADCT faltantes

## [0.2.0] - 2025-11-14

### Adicionado
- Nova estrutura flat de dados no Firebase
- Catálogo unificado de séries (123,574 séries)
- Scripts de ingestão otimizados com processamento paralelo
- Sistema de verificação de completude de datasets
- Logs de falhas de ingestão para retry posterior

### Mudado
- Migração para novo projeto Firebase (peixonaut01-2e0ba)
- Estrutura de dados de hierárquica para flat
- Processamento sequencial para paralelo (ThreadPoolExecutor)

### Corrigido
- Problemas de encoding UTF-8 em arquivos CSV
- Rate limiting do IBGE com workers configuráveis
- Validação de links de API do catálogo

### Estatísticas
- **INPC:** 1,794 séries (100%)
- **IPCA:** 37,842 séries (100%)
- **IPCA15:** 1,794 séries (100%)
- **IPP:** 220 séries (100%)
- **PIMPF:** 1,914 séries (100%)
- **PMC:** 3,672 séries (100%)
- **PMS:** 888 séries (100%)
- **PNADCM:** 572 séries (100%)
- **LSPA:** 14,053 séries (84.5%) - 2,587 faltantes (Safra 2024/2025)
- **PNADCT:** 58,233 séries (99.99%) - 5 faltantes

## [0.1.0] - 2025-10-XX

### Adicionado
- Sistema inicial de ingestão de dados IBGE
- Scripts para CNT, PMS, PMC, PNADCT, PNADCM, IPP
- Integração com Firebase Realtime Database
- GitHub Actions workflows
- Estrutura hierárquica de dados

---

## Tipos de Mudanças

- **Adicionado** para novas funcionalidades
- **Mudado** para mudanças em funcionalidades existentes
- **Depreciado** para funcionalidades que serão removidas
- **Removido** para funcionalidades removidas
- **Corrigido** para correções de bugs
- **Segurança** para vulnerabilidades

