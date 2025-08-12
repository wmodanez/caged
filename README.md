# 🎯 Sistema CAGED

Sistema automatizado para download, processamento e consolidação de dados mensais do **Cadastro Geral de Empregados e Desempregados (CAGED)** com arquitetura modular e CLI unificada.

## 🚀 Características

- **📊 Dados Mensais**: Processamento de dados de movimentação do mercado de trabalho formal
- **🔄 Pipeline Automatizado**: Download, descompactação, conversão e consolidação
- **⚡ Processamento Paralelo**: Preparado para grandes volumes de dados
- **📦 Formato Parquet**: Arquivos compactos e eficientes para análise
- **🎛️ CLI Unificada**: Interface de linha de comando intuitiva
- **🏗️ Arquitetura Modular**: Estrutura organizada em módulos especializados
- **⚙️ Sistema de Configuração**: Configuração centralizada e flexível
- **🔧 Pipeline Avançado**: Sistema de processamento com estágios configuráveis
- **💾 Cache Inteligente**: Sistema de cache com verificação de integridade e expiração automática
- **🔍 Validação Robusta**: Sistema de validação abrangente com testes automatizados
- **🔄 Sistema de Recovery**: Checkpoints automáticos e recuperação de falhas
- **📁 Organização Hierárquica**: Estrutura de diretórios espelhando o servidor FTP (AAAA/AAAAMM)
- **🎯 Processamento Anual Simplificado**: Processa todos os meses automaticamente com `--ano` apenas
- **📋 Processamento Completo CAGED**: Suporte automático para todos os tipos de arquivo (CAGEDMOV, CAGEDEXC, CAGEDFORA)
- **📊 Cálculo de Saldo e Estoque**: Sistema completo de cálculo de saldo mensal e estoque acumulado
- **🔧 Padronização Automática**: Harmonização automática de colunas entre diferentes tipos de arquivo

## 🎯 Funcionalidades

- **🎯 Processamento Anual Automático**: Processa todos os meses de um ano usando apenas `python main.py processar --ano 2024`
- **📁 Estrutura de Diretórios Hierárquica**: Os arquivos baixados são organizados em `files-zip/AAAA/AAAAMM/` espelhando a estrutura do servidor FTP
- **🔄 Integração com FTP Service**: Navega corretamente na estrutura do CAGED (`/pdet/microdados/NOVO CAGED`)
- **📋 CLI Intuitiva**: Interface com comportamento padrão inteligente
- **⚡ Paralelismo Entre Estágios**: O processamento paralelo entre estágios (download, extração, conversão) permite que a extração comece assim que o primeiro download termina
- **📋 Processamento Completo de Arquivos CAGED**: Sistema identifica e processa automaticamente todos os tipos de arquivo CAGED (CAGEDMOV, CAGEDEXC, CAGEDFORA)
- **📊 Cálculo Automático de Saldo**: Cálculo completo de saldo mensal e estoque acumulado seguindo a metodologia oficial do CAGED
- **🔧 Padronização de Colunas**: Sistema automático de harmonização de colunas entre diferentes tipos de arquivo
- **🎯 Tratamento de Ajustes**: Processamento de exclusões (CAGEDEXC) e movimentações fora do prazo (CAGEDFORA) com aplicação de sinais adequados
- **🔍 Validação Robusta**: Lógica de validação de parâmetros flexível e intuitiva
- **📁 Organização de Arquivos**: Estrutura de pastas que facilita a localização e gerenciamento dos dados
- **⚡ Paralelismo Automático**: O sistema calcula automaticamente o número ideal de workers (75% dos cores disponíveis)
- **🔄 Pipeline Eficiente**: Paralelismo entre estágios permitindo que a extração comece assim que o primeiro download termina

## 📋 Requisitos

- Python 3.8+
- Espaço em disco: ~20GB por ano de dados
- Memória RAM: 16GB recomendado

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
# Processamento anual completo (todos os meses do ano)
python main.py processar --ano 2024

# Processamento completo (download + extração + conversão)
python main.py processar --ano 2024 --mes 1

# Processamento por faixa de datas
python main.py processar --ano-inicio 2024 --mes-inicio 1 --ano-fim 2024 --mes-fim 6

# Apenas download
python main.py processar --ano 2024 --mes 1 --download

# Apenas extração
python main.py processar --ano 2024 --mes 1 --extract

# Apenas conversão
python main.py processar --ano 2024 --mes 1 --convert

# Cálculo de saldo mensal
python main.py processar --ano 2024 --mes 1 --calculate-saldo

# Processamento completo incluindo cálculo de saldo
python main.py processar --ano 2024 --mes 1 --convert --calculate-saldo

# Pular etapas específicas
python main.py processar --ano 2024 --mes 1 --skip-download --skip-extract --skip-convert

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

## 📊 Cálculo do Estoque de Empregos CAGED

O cálculo do estoque de empregos no CAGED (Cadastro Geral de Empregados e Desempregados), no contexto do CAGED integrado ao eSocial desde 2020, é uma medida do total de vínculos formais ativos em uma data específica. Ele reflete o saldo acumulado de admissões e desligamentos, ajustado por correções como movimentações fora do prazo e exclusões. Esses elementos são extraídos dos arquivos de dados disponíveis para download no portal do Ministério do Trabalho e Emprego, e o processo envolve consolidação de informações do eSocial e do sistema CAGED legado, com tratamentos para evitar erros e duplicidades.

### 🧮 Cálculo Básico do Estoque

O estoque de empregos é calculado de forma iterativa, mês a mês, com base em um estoque inicial (geralmente ancorado na RAIS - Relação Anual de Informações Sociais, que fornece o estoque anual consolidado). A fórmula geral é:

**Estoque final (t) = Estoque inicial (t-1) + Admissões (t) - Desligamentos (t) + Ajustes (fora do prazo e exclusões)**

- **Estoque inicial (t-1)**: Vem do mês anterior ou de uma base histórica (como a RAIS para o início da série).
- **Admissões (t)**: Contratos formais registrados no período.
- **Desligamentos (t)**: Demissões, rescisões ou términos de contrato no período.
- **Ajustes**: Incluem incorporações retroativas de movimentações fora do prazo e subtrações por exclusões, que podem alterar estoques de meses anteriores.

Esse cálculo é dinâmico, pois os dados são reprocessados mensalmente para incorporar correções, garantindo que a série histórica seja mantida atualizada. Por exemplo, se uma empresa declara uma admissão de janeiro em outubro, isso ajusta o estoque de janeiro retroativamente.

### 📁 Consideração dos Arquivos Baixados

Os arquivos de dados do CAGED (disponíveis no portal gov.br) são divididos em categorias que alimentam diretamente esse cálculo. Aqui vai como cada um é integrado:

1. **Arquivo de Movimentação (no prazo)**:
   - Contém admissões e desligamentos declarados até o dia 15 do mês seguinte à competência (ex.: movimentações de setembro declaradas até 15 de outubro).
   - Esses dados formam a base principal do saldo mensal (admissões - desligamentos).
   - São incorporados imediatamente no cálculo do estoque do mês corrente. Por exemplo, se houver 2 milhões de admissões e 1,8 milhões de desligamentos em um mês, o saldo contribui +200 mil para o estoque.

2. **Arquivo de Movimentação Fora do Prazo**:
   - Inclui declarações atrasadas, relativas a competências anteriores (podem retroagir até 2011, dependendo do caso).
   - São incorporadas retroativamente, ajustando os estoques históricos. Isso significa que o estoque de um mês passado pode ser revisado para cima ou para baixo quando declarações adicionais chegam.
   - Exemplo: No período de janeiro a abril de 2021, havia cerca de 278 mil movimentações fora do prazo; em maio de 2021, esse número subiu para 774 mil devido à transição para o eSocial (Grupo 3), mas após cruzamentos e verificações, 97,8% das admissões e 97% das demissões foram validadas e integradas.
   - Impacto: Aumenta a precisão, mas pode causar variações nos dados divulgados mensalmente (representando cerca de 2-3% das movimentações totais em períodos de transição).

3. **Arquivo de Exclusões**:
   - Registra remoções de movimentações informadas erroneamente (via evento S-3000 no eSocial, disponível desde outubro de 2021, com retroatividade a janeiro de 2020).
   - São subtraídas retroativamente do estoque. Por exemplo, entre abril de 2020 e outubro de 2021, foram excluídas 103.099 admissões e 54.850 demissões, impactando o saldo negativo em certos meses (ex.: -20.206 no saldo de maio de 2021).
   - Essas exclusões são tratadas como correções e mantidas nos microdados com indicativos, permitindo rastreabilidade.

### 🔧 Passos Metodológicos Detalhados

De acordo com notas técnicas oficiais, o processo segue estes passos principais para consolidação e cálculo:

1. **Coleta e Consolidação Inicial**: Dados do eSocial (priorizados) e CAGED são reunidos. Em casos de duplicidade (mesma movimentação em ambos os sistemas), prevalece o eSocial. Chaves como CNPJ raiz, CPF, competência e tipo de movimentação são usadas para cruzamentos e eliminação de duplicados.

2. **Imputação de Dados Faltantes**: Se uma empresa declara admissão mas não desligamento, usa-se dados do Empregador Web para imputar desligamentos, evitando subestimação do estoque.

3. **Incorporação de Ajustes**: Movimentações fora do prazo e exclusões são adicionadas/subtraídas retroativamente, reprocessando a série histórica. Isso é feito mensalmente, sem alterar a análise conjuntural geral (impacto médio de 2,78% nas movimentações de 2020-2021).

4. **Validação e Divulgação**: Após tratamentos, o estoque é calculado e divulgado mensalmente, com microdados disponíveis para download (incluindo os arquivos mencionados).

### ⚠️ Observações Importantes

- **Transição para o eSocial**: Desde 2020, o CAGED usa principalmente o eSocial, o que aumentou as declarações fora do prazo durante fases de implementação, mas aprimorou a qualidade dos dados.
- **Impacto nos Dados Históricos**: Devido aos ajustes, os estoques divulgados podem variar ligeiramente entre publicações mensais, mas isso é uma prática padrão em estatísticas trabalhistas para maior precisão.
- **Fontes para Download**: Os arquivos estão no portal https://pdet.mte.gov.br/novo-caged, separados por tipo (movimentação, fora do prazo, exclusões), permitindo que usuários repliquem cálculos com ferramentas como planilhas ou software de análise.

### 🎯 Implementação no Sistema

Este sistema implementa fielmente a metodologia oficial do CAGED:

- **Processamento Automático**: Identifica e processa todos os tipos de arquivo (CAGEDMOV, CAGEDEXC, CAGEDFORA)
- **Aplicação de Ajustes**: Exclusões são aplicadas com sinal invertido, movimentações fora do prazo são incorporadas retroativamente
- **Padronização**: Harmonização automática de colunas entre diferentes tipos de arquivo
- **Cálculo Incremental**: Processamento eficiente do saldo mensal sem reprocessar dados já calculados
- **Persistência**: Armazenamento em formato Parquet para consultas rápidas e análises posteriores

### 🔧 Opções Avançadas
```bash
# Modo debug
python main.py --debug processar --ano 2024 --mes 1

# Processamento com validação rigorosa
python main.py processar --ano 2024 --mes 1 --validacao-rigorosa

# Forçar reprocessamento
python main.py processar --ano 2024 --mes 1 --force

# Processamento com número de workers personalizado
python main.py processar --ano 2024 --mes 1 --workers 12

# Processamento sequencial (sem paralelismo)
python main.py processar --ano 2024 --mes 1 --sequential

# Processamento paralelo entre itens (em vez do padrão entre estágios)
python main.py processar --ano 2024 --mes 1 --parallel-items

# Usar sistema de cache
python main.py processar --ano 2024 --mes 1 --use-cache

# Limpar cache do sistema
python main.py cache-clear

# Verificar estatísticas do cache
python main.py cache-stats
```

### 🔄 Sistema de Recovery
```bash
# Listar checkpoints de recovery
python main.py recovery-list

# Listar apenas checkpoints falhos
python main.py recovery-list --status failed

# Listar checkpoints por tipo de operação
python main.py recovery-list --operation-type download

# Ver detalhes de um checkpoint específico
python main.py recovery-info [OPERATION_ID]

# Limpar checkpoints concluídos
python main.py recovery-clear --status completed --force

# Limpar todos os checkpoints
python main.py recovery-clear --all --force

# Limpar checkpoint específico
python main.py recovery-clear [OPERATION_ID] --force
```

### 📊 Sistema de Métricas
```bash
# Ver métricas de performance
python main.py metrics

# Métricas em formato tabular
python main.py metrics --format table

# Métricas em formato JSON
python main.py metrics --format json

# Salvar métricas em arquivo
python main.py metrics --save relatorio.json

# Ver apenas alertas de performance
python main.py metrics --alerts-only

# Executar exemplo de métricas
python examples/exemplo_metricas.py
```

## 🏗️ Arquitetura

### ⚡ Modos de Processamento

O sistema oferece três modos de processamento:

1. **🔄 Processamento Sequencial** (com `--sequential`)
   - Executa todas as etapas em sequência
   - Primeiro todos os downloads, depois todas as extrações, por fim todas as conversões
   - Útil para ambientes com recursos limitados

2. **⚡ Paralelismo Entre Estágios** (padrão)
   - Executa os estágios em paralelo para cada item
   - A extração de um item começa assim que seu download termina
   - A conversão de um item começa assim que sua extração termina
   - Gerencia o uso de recursos e reduz o tempo total de processamento
   - Número de workers calculado automaticamente (75% dos cores disponíveis)

3. **🔄 Paralelismo Entre Itens** (com `--parallel-items`)
   - Processa múltiplos itens simultaneamente
   - Todos os downloads são executados em paralelo, depois todas as extrações, etc.
   - Útil quando há muitos itens pequenos

### 🎯 Benefícios da Arquitetura

- **🔧 Modularidade**: Separação clara de responsabilidades
- **⚙️ Configuração Centralizada**: Sistema de configuração YAML flexível
- **🔄 Pipeline Avançado**: Processamento em estágios configuráveis
- **🚨 Tratamento de Exceções**: Hierarquia de exceções customizadas
- **📊 Monitoramento**: Sistema de logging e métricas integrado
- **🎛️ CLI Unificada**: Interface simplificada e intuitiva
- **🔍 Validação Robusta**: Sistema de validação com 26+ testes automatizados
- **🔄 Sistema de Recovery**: Checkpoints automáticos e recuperação de falhas

### 📦 Componentes Principais

- **Core**: Configuração, exceções, pipeline principal e sistema de recovery
- **Services**: Serviços de FTP, extração e conversão
- **Entities**: Modelos de dados do CAGED
- **Utils**: Utilitários, validadores, logging e cache
- **CLI**: Interface de linha de comando unificada
- **Recovery**: Sistema de checkpoints e recuperação automática
- **Tests**: Suíte de testes automatizados

## 📁 Estrutura do Projeto

```
caged/
├── main.py                    # Ponto de entrada principal
├── requirements.txt           # Dependências
├── README.md                 # Este arquivo
├── PLANO_MELHORIAS_CAGED.md  # Plano de desenvolvimento
├── config/                   # Arquivos de configuração
├── examples/                 # Exemplos de uso
│   ├── cache_usage_example.py # Exemplo de uso do cache
│   └── parallel_processing_example.py # Exemplo de processamento paralelo
├── files-zip/                # Arquivos baixados (7z) organizados por ano/mês
│   └── AAAA/                 # Diretório do ano (ex: 2024)
│       └── AAAAMM/           # Diretório do mês (ex: 202401)
│           ├── CAGEDMOV*.7z  # Arquivo de movimentação
│           ├── CAGEDEXC*.7z  # Arquivo de exclusão
│           └── CAGEDFOR*.7z  # Arquivo fora de prazo
├── files-unzip/              # Arquivos descompactados
├── parquet/                  # Dados processados (Parquet)
├── logs/                     # Logs de execução
├── tests/                    # Testes automatizados
│   ├── test_cache.py         # Testes do sistema de cache
│   ├── test_parallel_pipeline.py # Testes do processamento paralelo
│   └── test_validators.py    # Testes dos validadores
└── src/                      # Código fonte
    ├── cli/                  # Interface de linha de comando
    │   ├── __init__.py
    │   └── commands.py       # Comandos CLI unificados
    ├── core/                 # Componentes centrais
    │   ├── __init__.py
    │   ├── config.py         # Sistema de configuração
    │   ├── exceptions.py     # Exceções customizadas
    │   ├── pipeline.py       # Pipeline de processamento
    │   └── recovery.py       # Sistema de recovery
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
        ├── cache.py          # Sistema de cache inteligente
        ├── logger.py         # Sistema de logging
        ├── validators.py     # Validadores
        ├── utilitarios.py    # Utilitários gerais
        └── filtro_caged.py   # Filtros CAGED
```

## ⚡ Sistema de Processamento Paralelo

O sistema implementa dois tipos de processamento paralelo para melhorar performance:

### 🎯 Processamento Paralelo entre Itens
Processa múltiplos itens (meses/anos) em paralelo:
```bash
python main.py processar --ano 2024 --mes 1 --workers 8
```

### 🚀 Processamento Paralelo entre Estágios
Executa estágios (download, extração, conversão) em paralelo para cada item:
```bash
python main.py processar --ano 2024 --mes 1 --parallel-stages --workers 8
```

Com este modo, assim que um download é concluído, a extração começa imediatamente, sem esperar que todos os downloads terminem.

### 🎯 Funcionalidades do Processamento Paralelo

- **Controle Inteligente de Recursos**: Monitoramento em tempo real de CPU, memória e disco
- **Pool de Conexões**: Gerenciamento eficiente de conexões de banco de dados
- **Ajuste Automático**: Número de workers baseado na carga do sistema
- **Sistema de Throttling**: Prevenção de sobrecarga do sistema
- **Estatísticas Detalhadas**: Métricas de performance e uso de recursos
- **Pipeline com Estágios Paralelos**: Download, extração e conversão executados em pipeline

### 📊 Comparação de Modos de Processamento

| Modo | Descrição | Caso de Uso |
|------|-----------|-------------|
| **Sequencial** | Executa tudo em sequência | Recursos limitados |
| **Paralelo entre Itens** | Múltiplos itens em paralelo | Muitos períodos diferentes |
| **Paralelo entre Estágios** | Estágios em pipeline | Reduzir tempo total por item |

### 📈 Benefícios de Performance

- **Processamento Assíncrono**: Execução paralela de tarefas independentes
- **Monitoramento de Recursos**: Ajuste automático baseado na carga do sistema
- **Recovery Automático**: Tratamento robusto de erros e falhas
- **Logging Estruturado**: Acompanhamento detalhado do progresso

### 📈 Benefícios de Performance

- **Redução de Downloads**: Evita re-download de arquivos já processados
- **Gestão de I/O**: Cache de arquivos extraídos e convertidos
- **Economia de Tempo**: Processamento até 80% mais rápido em re-execuções
- **Economia de Banda**: Redução significativa no tráfego de rede

### ⚡ Comandos de Processamento Paralelo

```bash
# Executar exemplo de processamento paralelo
python examples/parallel_processing_example.py

# Usar processamento paralelo em código Python
from src.core.pipeline import create_pipeline

# Criar pipeline com configurações personalizadas
pipeline = create_pipeline(
    max_workers=4,
    resource_check_interval=5,
    cache_enabled=True
)

# Processar itens em paralelo
results = await pipeline.process_items_parallel(items)
```

### ⚙️ Configuração do Sistema

O sistema pode ser configurado através do arquivo de configuração:

```yaml
parallel_processing:
  max_workers: 4
  resource_check_interval: 5  # segundos
  cpu_threshold: 80  # porcentagem
  memory_threshold: 80  # porcentagem
  disk_threshold: 90  # porcentagem
  connection_pool:
    max_connections: 10
    timeout: 30  # segundos
    retry_attempts: 3

recovery:
  enabled: true
  state_file: "state.json"
  auto_cleanup_days: 30
  max_checkpoints: 100
```

## 🔄 Sistema de Recovery

O sistema implementa um robusto sistema de recovery para garantir a continuidade das operações:

### 🎯 Funcionalidades do Recovery

- **Checkpoints Automáticos**: Criação automática de pontos de recuperação durante o processamento
- **Persistência de Estado**: Salvamento contínuo do progresso das operações
- **Recuperação Inteligente**: Detecção e recuperação automática de operações interrompidas
- **Gerenciamento de Falhas**: Tratamento robusto de erros com informações detalhadas
- **Limpeza Automática**: Remoção automática de checkpoints antigos
- **Monitoramento**: Acompanhamento detalhado do status das operações

### 📈 Benefícios do Recovery

- **Continuidade**: Operações podem ser retomadas após falhas ou interrupções
- **Confiabilidade**: Redução significativa de perda de progresso
- **Transparência**: Visibilidade completa do estado das operações
- **Eficiência**: Evita reprocessamento desnecessário de dados
- **Auditoria**: Histórico completo de operações e falhas

## 📊 Sistema de Métricas

O sistema implementa um sistema completo de monitoramento e métricas para acompanhar a performance:

### 🎯 Funcionalidades das Métricas

- **Coleta Automática**: Registro automático de todas as operações do sistema
- **Métricas de Performance**: Tempo de execução, throughput e taxa de sucesso
- **Recursos do Sistema**: Monitoramento de CPU, memória e espaço em disco
- **Estatísticas de Cache**: Hit rate, economia de tempo e eficiência
- **Alertas Inteligentes**: Detecção automática de problemas de performance
- **Múltiplos Formatos**: Saída em texto, tabular e JSON
- **Persistência**: Salvamento automático de métricas em arquivo

### 📈 Benefícios das Métricas

- **Visibilidade**: Acompanhamento completo da performance do sistema
- **Análise de Performance**: Identificação de gargalos e oportunidades de aprimoramento
- **Monitoramento**: Alertas proativos para problemas de performance
- **Análise**: Dados históricos para análise de tendências
- **Relatórios**: Geração automática de relatórios detalhados

## 🧪 Testes Automatizados

O sistema implementa uma suíte completa de testes automatizados para garantir qualidade e confiabilidade:

### 🎯 Estrutura de Testes

```
tests/
├── conftest.py                    # Configurações globais e fixtures
├── pytest.ini                     # Configuração do pytest
├── unit/                          # Testes unitários
│   ├── test_cache.py              # Testes do sistema de cache
│   ├── test_validators.py         # Testes dos validadores
│   ├── test_recovery.py           # Testes do sistema de recovery
│   ├── test_ftp_service.py        # Testes do serviço FTP
│   ├── test_extract_service.py    # Testes do serviço de extração
│   ├── test_convert_service.py    # Testes do serviço de conversão
│   └── test_metrics.py            # Testes do sistema de métricas
├── integration/                   # Testes de integração
│   ├── test_parallel_pipeline.py  # Testes do pipeline paralelo
│   └── test_full_pipeline.py      # Testes do pipeline completo
└── fixtures/                      # Dados e mocks para testes
    ├── sample_data/               # Dados de exemplo
    └── mock_responses/            # Respostas simuladas
```

### 🚀 Executando Testes

#### Execução Básica
```bash
# Executar todos os testes
python run_tests.py --all

# Apenas testes unitários
python run_tests.py --unit

# Apenas testes de integração
python run_tests.py --integration

# Testes rápidos (exclui testes lentos)
python run_tests.py --quick
```

#### Execução com Cobertura
```bash
# Todos os testes com relatório de cobertura
python run_tests.py --all --coverage

# Testes unitários com cobertura
python run_tests.py --unit --coverage
```

#### Execução Paralela
```bash
# Executar testes em paralelo
python run_tests.py --all --parallel

# Pipeline completo de CI/CD
python run_tests.py --ci
```

#### Testes por Categoria
```bash
# Testes de cache
python run_tests.py --marker cache

# Testes de validação
python run_tests.py --marker validators

# Testes de métricas
python run_tests.py --marker metrics

# Testes de serviços
python run_tests.py --marker services
```

### 📊 Relatórios de Qualidade

```bash
# Gerar relatório completo de qualidade
python run_tests.py --quality

# Executar com profiling de performance
python run_tests.py --profile

# Testes de performance
python run_tests.py --performance
```

### 🎯 Cobertura de Testes

O sistema mantém alta cobertura de testes em todos os componentes:

- **Testes Unitários**: 80+ testes cobrindo todas as classes e funções principais
- **Testes de Integração**: 20+ testes verificando a integração entre componentes
- **Testes de Sistema**: Pipeline completo end-to-end
- **Mocks e Fixtures**: Dados de teste realistas e mocks configuráveis
- **Validação de Dados**: Testes com dados reais e casos extremos

### 🔧 Configuração de Testes

O arquivo `pytest.ini` configura:

- **Marcadores Personalizados**: `slow`, `integration`, `unit`, `ftp`, `cache`, `recovery`, `metrics`
- **Cobertura Automática**: Relatórios HTML, XML e terminal
- **Timeout**: Proteção contra testes infinitos
- **Logging**: Saída estruturada para debugging
- **Filtros**: Supressão de warnings desnecessários

### 🚀 Pipeline de CI/CD

O script `run_tests.py` oferece um pipeline completo:

1. **Testes Unitários**: Verificação de componentes individuais
2. **Testes de Integração**: Verificação de interações entre componentes
3. **Análise de Cobertura**: Relatórios detalhados de cobertura
4. **Análise Estática**: Verificação de qualidade de código (flake8)
5. **Análise de Complexidade**: Métricas de complexidade (radon)
6. **Relatório Consolidado**: Resumo executivo em Markdown

### 📈 Métricas de Qualidade

- **Cobertura de Código**: Meta de 80%+ (configurável)
- **Taxa de Sucesso**: 100% dos testes devem passar
- **Performance**: Testes executam em < 5 minutos
- **Qualidade**: Análise estática sem erros críticos
- **Manutenibilidade**: Complexidade ciclomática controlada

### 🔧 Comandos de Recovery

```bash
# Listar todos os checkpoints
python main.py recovery-list

# Filtrar checkpoints por status
python main.py recovery-list --status failed
python main.py recovery-list --status completed

# Filtrar por tipo de operação
python main.py recovery-list --operation-type download
python main.py recovery-list --operation-type processing

# Ver detalhes de um checkpoint
python main.py recovery-info d6c8aea015f4

# Limpar checkpoints específicos
python main.py recovery-clear --status completed --force
python main.py recovery-clear --all --force
python main.py recovery-clear d6c8aea015f4 --force
```

### ⚡ Uso Programático do Recovery

```python
from src.core.recovery import RecoveryManager

# Criar gerenciador de recovery
recovery = RecoveryManager()

# Criar checkpoint para operação
checkpoint_id = recovery.create_checkpoint(
    operation_type="processing",
    metadata={"ano": 2024, "mes": 1}
)

# Atualizar progresso
recovery.update_checkpoint(
    checkpoint_id,
    progress=0.5,
    current_step="Processando dados"
)

# Marcar como concluído
recovery.complete_checkpoint(checkpoint_id)

# Listar checkpoints ativos
active = recovery.list_active_checkpoints()
print(f"Operações ativas: {len(active)}")
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
- **Performance Eficiente**: Cálculos realizados com expressões Polars eficientes
- **Flexibilidade**: Indicadores adaptam-se aos campos disponíveis nos dados

## 💡 Exemplos de Uso

### 🚀 Processamento Básico
```bash
# Processar dados de janeiro de 2024
python main.py processar --ano 2024 --mes 1

# Processar ano completo (todos os meses) com cache
python main.py processar --ano 2024 --use-cache

# Processar com validação rigorosa
python main.py processar --ano 2024 --mes 1 --validacao-rigorosa

# Verificar processamento com dry-run
python main.py processar --ano 2024 --dry-run
```

### ⚡ Processamento Eficiente
```bash
# Usar cache para acelerar reprocessamento
python main.py processar --ano 2024 --mes 1 --use-cache

# Processamento paralelo com 8 workers
python main.py processar --ano 2024 --mes 1 --workers 8
```

### 📊 Uso Programático
```python
from src.utils.cache import cache_manager
from src.utils.validators import DataValidator
from src.core.pipeline import create_pipeline
from src.utils.metrics import get_metrics_collector

# Usar o cache em código Python
if cache_manager.get_cached_file("dados_2024_01"):
    print("Dados já em cache!")

# Validar dados antes do processamento
validator = DataValidator()
if validator.validate_disk_space("/path/to/data", required_gb=5):
    print("Espaço suficiente para processamento")

# Usar processamento paralelo
pipeline = create_pipeline(max_workers=8, cache_enabled=True)
results = await pipeline.process_items_parallel(items)
stats = pipeline.get_parallel_stats()
print(f"Processados {stats['completed_tasks']} itens")

# Usar sistema de métricas
metrics = get_metrics_collector()
metrics.record_operation("download", success=True, duration=2.5)
report = metrics.generate_report()
print(f"Taxa de sucesso: {report['summary']['overall_success_rate']:.1%}")
```

## 🎯 Status do Desenvolvimento

### ✅ Fase 1: Estrutura e Arquitetura (Concluída)
- [x] ✅ Estrutura de arquivos organizada
- [x] ✅ Sistema de configuração centralizado
- [x] ✅ Hierarquia de exceções customizadas
- [x] ✅ Pipeline de processamento avançado
- [x] ✅ CLI unificada e intuitiva
- [x] ✅ Módulos organizados em estrutura modular
- [x] ✅ Sistema de logging completo
- [x] ✅ Sistema de validação robusto (26+ testes)

### ⚡ Fase 2: Pipeline Eficiente (Concluída)
- [x] ✅ Processamento paralelo (19 testes aprovados)
- [x] ✅ Comando processar unificado (integração completa)
- [x] ✅ Sistema de configuração YAML

### 🚀 Fase 3: Funcionalidades Avançadas (Em Andamento)
- [x] ✅ Sistema de recovery e checkpoints (19 testes aprovados)
- [x] ✅ Sistema de métricas e monitoramento (CLI completa)
- [ ] 📋 Interface web interativa
- [ ] 📊 Dashboard de monitoramento
- [ ] 🔔 Sistema de notificações
- [ ] 📈 Relatórios automatizados
- [ ] 🌐 API REST

### 🧪 Qualidade e Testes
- [x] ✅ Testes de validação (26 testes)
- [x] ✅ Testes de processamento paralelo (19 testes)
- [x] ✅ Testes de recovery (19 testes)
- [x] ✅ Testes de integração CLI
- [x] ✅ Sistema de métricas funcional
- [x] ✅ Testes automatizados completos (100+ testes)
- [x] ✅ Pipeline de CI/CD configurado
- [x] ✅ Relatórios de cobertura automatizados
- [ ] 🔄 Cobertura de código 90%+
- [ ] 🔄 Documentação técnica completa

## 📋 TODO - Próximas Implementações

### 🔧 FASE 3: Funcionalidades Avançadas (Em Andamento)

#### 3.2 Sistema de Métricas - Funcionalidades Pendentes
- [ ] **Dashboard Web de Monitoramento**: Interface web interativa para visualização de métricas
- [ ] **Notificações em Tempo Real**: Sistema de alertas e notificações automáticas

#### 3.3 Testes Automatizados - Pendentes
- [ ] **Testes de Performance**: Benchmarks e testes de stress com grandes volumes
- [ ] **Testes de Stress**: Validação com cargas extremas do sistema

#### 3.4 Documentação e Exemplos
- [x] ✅ **README Completo**: Documentação das funcionalidades de processamento anual automático e estrutura de diretórios
- [ ] **Documentação de Configurações**: Documentação completa de todas as opções de configuração
- [ ] **Exemplos Práticos**: Casos de uso reais e exemplos avançados
- [ ] **FAQ**: Perguntas frequentes e solução de problemas comuns
- [ ] **USAGE.md**: Exemplos detalhados de uso
- [ ] **API.md**: Documentação da API interna
- [ ] **TROUBLESHOOTING.md**: Guia de solução de problemas
- [ ] **CHANGELOG.md**: Histórico detalhado de mudanças

### 📈 FASE 4: Performance e Polimento

#### 4.1 Análise de Performance
- [ ] **Profile de Performance**: Análise completa de performance do sistema
- [ ] **Operações Críticas**: Aprimoramentos em gargalos identificados
- [ ] **Lazy Loading**: Implementação de carregamento sob demanda
- [ ] **Compressão Inteligente**: Gestão de armazenamento e transferência
- [ ] **Uso de Memória**: Redução do footprint de memória
- [ ] **Memory Management**: Processamento em chunks
- [ ] **I/O Operations**: Buffering para operações de arquivo
- [ ] **Network Management**: Pool de conexões
- [ ] **CPU Operations**: Algoritmos eficientes

#### 4.2 Interface de Usuário
- [ ] **Progress Bars Avançadas**: Indicadores visuais mais detalhados e informativos
- [ ] **Mensagens de Erro Aprimoradas**: Mensagens mais claras e acionáveis
- [ ] **Sistema de Ajuda Interativo**: Help contextual e interativo
- [ ] **Auto-completion**: Suporte para Bash/Zsh completion
- [ ] **Modo Verbose/Quiet**: Controle granular de verbosidade

#### 4.3 Validação e Testes Finais
- [ ] **Teste com Dados Reais**: Validação com dados de produção
- [ ] **Teste de Stress**: Validação com grandes volumes de dados
- [ ] **Teste de Recuperação**: Validação do sistema de recovery
- [ ] **Teste de Compatibilidade**: Verificação de compatibilidade entre versões
- [ ] **Teste de Performance**: Benchmarks e métricas de performance

### 🎯 Critérios de Sucesso Pendentes

#### Métricas Quantitativas
- [ ] **Tempo de Setup**: Reduzir para <5 minutos para usuário
- [ ] **Cobertura de Código**: Atingir 90%+ de cobertura

#### Métricas Qualitativas
- [ ] **Facilidade de Uso**: Coletar feedback positivo de usuários
- [ ] **Manutenibilidade**: Garantir código mais limpo e organizado
- [ ] **Robustez**: Reduzir falhas em produção
- [ ] **Documentação**: Completar documentação técnica
- [ ] **Compatibilidade**: Garantir migração suave da versão anterior

### 🔄 Estratégia de Migração
- [ ] **Manter Comandos Antigos**: Com warnings de deprecação por 6 meses
- [ ] **Documentação de Migração**: Guia detalhado de migração
- [ ] **Scripts de Migração**: Automação quando possível
- [ ] **Backup da Versão Atual**: Antes de implementar mudanças
- [ ] **Testes em Ambiente Isolado**: Validação antes de produção
- [ ] **Deployment Gradual**: Por funcionalidade
- [ ] **Monitoramento Ativo**: Durante período de transição

### 🚀 Funcionalidades Futuras
- [ ] **Interface Web Completa**: Dashboard interativo para gerenciamento
- [ ] **API REST**: Endpoints para integração com outros sistemas
- [ ] **Sistema de Plugins**: Arquitetura extensível para funcionalidades customizadas
- [ ] **Integração com Cloud**: Suporte para AWS, Azure, GCP
- [ ] **Machine Learning**: Análises preditivas e insights automáticos
- [ ] **Exportação Avançada**: Múltiplos formatos (Excel, CSV, JSON, XML)
- [ ] **Agendamento de Tarefas**: Processamento automático agendado
- [ ] **Monitoramento em Tempo Real**: Dashboard live de operações

### 🤖 Agente IA Especializado em Dash para Trae

#### 💡 Conceito do Agente
Criação de um agente IA especializado para gerar aplicações Dash de forma automática e inteligente, integrado ao ecossistema Trae. Este agente seria capaz de:
- Analisar dados do CAGED automaticamente
- Gerar dashboards interativos personalizados
- Criar visualizações eficientes para análise de mercado de trabalho
- Implementar componentes Dash reutilizáveis

#### 🔧 Implementação no Trae
```python
# Estrutura proposta para o agente Dash
class DashAgent:
    def __init__(self):
        self.name = "solo_dash"
        self.description = "Agente especializado em criação de aplicações Dash"
        self.capabilities = [
            "data_analysis",
            "dash_layout_generation", 
            "plotly_components",
            "interactive_callbacks",
            "deployment_optimization"
        ]
    
    def generate_dashboard(self, data_source, requirements):
        # Lógica de geração automática de dashboard
        pass
```

#### 🎯 Capacidades Específicas
- **Análise Automática de Dados**: Interpretação inteligente de estruturas de dados CAGED
- **Geração de Layouts**: Criação automática de layouts Dash responsivos e intuitivos
- **Componentes Plotly**: Implementação de gráficos eficientes para dados de emprego
- **Callbacks Interativos**: Geração automática de interatividade entre componentes
- **Performance**: Implementação de best practices para aplicações Dash
- **Deployment Automático**: Configuração para deploy em diferentes ambientes

#### 🚀 Funcionalidades
1. **Geração Automática**: Criação completa de dashboards a partir de especificações
2. **Integração CAGED**: Conectores nativos para dados do sistema CAGED
3. **Templates Inteligentes**: Biblioteca de templates para diferentes tipos de análise
4. **Performance**: Implementação automática de cache, lazy loading e eficiência
5. **Responsividade**: Dashboards adaptáveis para desktop e mobile
6. **Exportação**: Funcionalidades de export para PDF, PNG e dados

#### 💪 Vantagens para o Projeto CAGED
- **Especialização**: Agente focado especificamente em visualização de dados de emprego
- **Reutilização**: Templates e componentes reutilizáveis para diferentes análises
- **Velocidade**: Geração rápida de dashboards complexos
- **Consistência**: Padrões visuais e de UX consistentes
- **Manutenibilidade**: Código Dash limpo e bem estruturado
- **Integração Nativa**: Aproveitamento total da infraestrutura Python existente

#### 📋 Próximos Passos
1. **Definir Especificações**: Documentar requisitos detalhados do agente
2. **Criar Templates**: Desenvolver templates base para dashboards CAGED
3. **Implementar Agente**: Desenvolver o agente seguindo padrões Trae
4. **Testar Integração**: Validar integração com dados reais do CAGED
5. **Documentar Uso**: Criar documentação e exemplos de uso
6. **Deploy e Feedback**: Implementar em produção e coletar feedback

#### 🎯 Exemplo de Uso
```bash
# Comando proposto para o agente Dash
trae dash create-dashboard \
  --data-source "caged_2024_01.parquet" \
  --dashboard-type "employment-analysis" \
  --features "geographic,sector,temporal" \
  --output "dashboard_emprego_2024.py"

# Geração de componentes específicos
trae dash create-component \
  --type "time-series-chart" \
  --data-field "admissoes" \
  --groupby "uf,mes" \
  --interactive

# Deploy automático
trae dash deploy \
  --app "dashboard_emprego_2024.py" \
  --platform "heroku" \
  --config "production"
```

#### 🔗 Recursos Relacionados
- **Dash Documentation**: https://dash.plotly.com/
- **Plotly Python**: https://plotly.com/python/
- **CAGED Data Structure**: Consultar `src/entities/` para estruturas de dados
- **Performance Best Practices**: Implementar cache e eficiência automática

---

## 📞 Suporte

Para dúvidas, problemas ou sugestões, consulte a documentação completa ou abra uma issue no repositório.

---

**Desenvolvido para facilitar o acesso aos dados do mercado de trabalho brasileiro.**