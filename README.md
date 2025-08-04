# 🎯 Sistema CAGED

Sistema automatizado para download, processamento e consolidação de dados mensais do **Cadastro Geral de Empregados e Desempregados (CAGED)** com arquitetura refatorada e CLI unificada.

## 🚀 Características

- **📊 Dados Mensais**: Processamento de dados de movimentação do mercado de trabalho formal
- **🔄 Pipeline Automatizado**: Download, descompactação, conversão e consolidação
- **⚡ Processamento Paralelo**: Otimizado para grandes volumes de dados
- **📦 Formato Parquet**: Arquivos compactos e otimizados para análise
- **🎛️ CLI Unificada**: Interface de linha de comando refatorada e intuitiva
- **🏗️ Arquitetura Modular**: Estrutura organizada em módulos especializados
- **⚙️ Sistema de Configuração**: Configuração centralizada e flexível
- **🔧 Pipeline Avançado**: Sistema de processamento com estágios configuráveis

## 📋 Requisitos

- Python 3.8+
- Espaço em disco: ~2GB por ano de dados
- Memória RAM: 4GB recomendado

## ⚙️ Instalação

```bash
# Clonar repositório
git clone [URL_DO_REPOSITORIO]
cd caged

# Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

# Instalar dependências
pip install -r requirements.txt
```

## 🎯 CLI Unificada

### 🔄 Comando Principal de Processamento
```bash
# Processamento completo (download + extração + conversão)
python main.py processar --ano 2024 --mes 1

# Processamento anual completo
python main.py processar --ano 2024 --anual

# Processamento por faixa de datas
python main.py processar --ano-inicio 2024 --mes-inicio 1 --ano-fim 2024 --mes-fim 6

# Apenas download
python main.py processar --ano 2024 --mes 1 --apenas-download

# Apenas extração
python main.py processar --ano 2024 --mes 1 --apenas-extracao

# Apenas conversão
python main.py processar --ano 2024 --mes 1 --apenas-conversao

# Pular etapas específicas
python main.py processar --ano 2024 --mes 1 --pular-download --pular-extracao
```

### ⚙️ Configuração do Sistema
```bash
# Criar arquivo de configuração padrão
python main.py config-create

# Mostrar configuração atual
python main.py config-show

# Usar arquivo de configuração específico
python main.py --config config/custom.yaml processar --ano 2024 --mes 1

# Usar profile de configuração
python main.py --profile producao processar --ano 2024 --mes 1
```

### 🔧 Opções Avançadas
```bash
# Modo debug
python main.py --debug processar --ano 2024 --mes 1

# Processamento com validação rigorosa
python main.py processar --ano 2024 --mes 1 --validacao-rigorosa

# Forçar reprocessamento
python main.py processar --ano 2024 --mes 1 --forcar

# Processamento paralelo customizado
python main.py processar --ano 2024 --mes 1 --workers 8
```

## 🏗️ Arquitetura

### 🎯 Benefícios da Arquitetura

- **🔧 Modularidade**: Separação clara de responsabilidades
- **⚙️ Configuração Centralizada**: Sistema de configuração YAML flexível
- **🔄 Pipeline Avançado**: Processamento em estágios configuráveis
- **🚨 Tratamento de Exceções**: Hierarquia de exceções customizadas
- **📊 Monitoramento**: Sistema de logging e métricas integrado
- **🎛️ CLI Unificada**: Interface simplificada e intuitiva

### 📦 Componentes Principais

- **Core**: Configuração, exceções e pipeline principal
- **Services**: Serviços de FTP, extração e conversão
- **Entities**: Modelos de dados do CAGED
- **Utils**: Utilitários, validadores e logging
- **CLI**: Interface de linha de comando unificada

## 📁 Estrutura do Projeto

```
caged/
├── main.py                    # Ponto de entrada principal
├── requirements.txt           # Dependências
├── README.md                 # Este arquivo
├── config/                   # Arquivos de configuração
├── cache/                    # Cache do sistema
├── files-zip/                # Arquivos baixados (7z)
├── files-unzip/              # Arquivos descompactados
├── parquet/                  # Dados processados (Parquet)
├── logs/                     # Logs de execução
└── src/                      # Código fonte
    ├── cli/                  # Interface de linha de comando
    │   ├── __init__.py
    │   └── commands.py       # Comandos CLI unificados
    ├── core/                 # Componentes centrais
    │   ├── __init__.py
    │   ├── config.py         # Sistema de configuração
    │   ├── exceptions.py     # Exceções customizadas
    │   └── pipeline.py       # Pipeline de processamento
    ├── entities/             # Entidades de dados
    │   ├── __init__.py
    │   ├── movimentacao.py
    │   ├── saldo_mensal.py
    │   └── ...
    ├── services/             # Serviços de negócio
    │   ├── __init__.py
    │   ├── ftp_service.py    # Serviço de FTP
    │   ├── extract_service.py # Serviço de extração
    │   └── convert_service.py # Serviço de conversão
    └── utils/                # Utilitários
        ├── __init__.py
        ├── logger.py         # Sistema de logging
        ├── validators.py     # Validadores
        ├── utilitarios.py    # Utilitários gerais
        └── filtro_caged.py   # Filtros CAGED
```

## 📊 Dados CAGED

O CAGED contém informações mensais sobre:

- **Movimentação**: Admissões, demissões e saldo líquido
- **Geografia**: Região, UF, município
- **Classificação**: CNAE 2.0, CBO 2002
- **Demografia**: Sexo, faixa etária, escolaridade
- **Especiais**: Tipo de deficiência, tipo de movimentação

## 📈 Indicadores Gerados

O sistema gera automaticamente diversos indicadores para análise do mercado de trabalho:

### 📊 Indicadores Básicos
- **Total de Registros**: Quantidade total de movimentações processadas
- **Total de Admissões**: Soma de todas as admissões no período
- **Total de Desligamentos**: Soma de todos os desligamentos no período
- **Saldo Líquido Calculado**: Diferença entre admissões e desligamentos

### 📈 Indicadores Avançados
- **Taxa de Rotatividade**: Percentual de rotatividade da força de trabalho
- **Taxa de Crescimento Líquido**: Percentual de crescimento do emprego
- **Razão Admissão/Desligamento**: Proporção entre admissões e desligamentos
- **Densidade de Movimentação**: Movimentações por registro

### 🌍 Indicadores por Segmento
- **Total de UFs Distintas**: Quantidade de Unidades Federativas com movimentação
- **Total de CNAEs Distintas**: Quantidade de atividades econômicas distintas
- **Total de CNPJs Distintos**: Quantidade de empresas com movimentação
- **Média de Movimentações por CNPJ**: Distribuição de movimentações por empresa

### 🎯 Características dos Indicadores
- **Validação Automática**: Todos os indicadores passam por validação de consistência
- **Tratamento de Erros**: Valores inválidos (NaN, infinito) são automaticamente filtrados
- **Performance Otimizada**: Cálculos realizados com expressões Polars otimizadas
- **Flexibilidade**: Indicadores adaptam-se aos campos disponíveis nos dados

## 🎯 Status do Desenvolvimento

- [x] ✅ Refatoração da estrutura de arquivos
- [x] ✅ Sistema de configuração centralizado
- [x] ✅ Hierarquia de exceções customizadas
- [x] ✅ Pipeline de processamento avançado
- [x] ✅ CLI unificada e intuitiva
- [x] ✅ Migração de módulos para nova estrutura
- [x] ✅ Sistema de logging refatorado
- [ ] 🔄 Testes automatizados
- [ ] 🔄 Documentação técnica
- [ ] 🔄 Interface web (planejada)

## 📞 Suporte

Para dúvidas, problemas ou sugestões, consulte a documentação completa ou abra uma issue no repositório.

---

**Desenvolvido com ❤️ para facilitar o acesso aos dados do mercado de trabalho brasileiro.**