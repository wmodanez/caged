# 🎯 Processador de Dados CAGED

Sistema automatizado para download, processamento e consolidação de dados mensais do **Cadastro Geral de Empregados e Desempregados (CAGED)**.

## 🚀 Características

- **📊 Dados Mensais**: Processamento de dados de movimentação do mercado de trabalho formal
- **🔄 Pipeline Automatizado**: Download, descompactação, conversão e consolidação
- **⚡ Processamento Paralelo**: Otimizado para grandes volumes de dados
- **📦 Formato Parquet**: Arquivos compactos e otimizados para análise
- **🎛️ Interface CLI**: Comandos simples e intuitivos

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

## 🎯 Comandos Disponíveis

### 📥 Download de Dados
```bash
# Apenas baixar dados de janeiro/2024
python main.py baixar --ano 2024 --mes 1

# Baixar primeiro semestre
python main.py baixar --ano 2024 --mes-inicio 1 --mes-fim 6

# Baixar todos os meses de um ano específico
python main.py baixar --ano 2024 --todos-meses

# Baixar todos os anos disponíveis no FTP
python main.py baixar --todos-anos
```

> **📝 Nota**: Os arquivos CAGED contêm dados de todas as UFs em um único arquivo por competência. A filtragem por UF deve ser feita após o download e descompactação dos dados.

### 📦 Descompactação
```bash
# Apenas descompactar arquivos já baixados
python main.py apenas-descompactar --ano 2024 --mes 1

# Descompactar todos os arquivos de um ano
python main.py apenas-descompactar --ano 2024 --todos

# Baixar e descompactar
python main.py descompactar --ano 2024 --mes 1
```

### 🔄 Conversão
```bash
# Apenas converter arquivos já baixados
python main.py apenas-converter --ano 2024 --mes 1

# Converter ano completo
python main.py apenas-converter --ano 2024 --consolidacao-anual

# Baixar e converter
python main.py converter --ano 2024 --mes 1
```

### 🚀 Processamento Completo
```bash
# Processamento completo: baixar, descompactar e converter
python main.py completo --ano 2024 --mes 1

# Processamento completo do ano
python main.py completo --ano 2024 --consolidacao-anual
```

### 📊 Status do Projeto
```bash
# Verificar arquivos processados
python main.py status
```

## 🚀 Comandos Otimizados - Fase 5.1

### ⚡ Conversão Otimizada
```bash
# Conversão mensal com cache e paralelismo
python main.py converter-otimizado --ano 2024 --mes 1

# Conversão sem cache
python main.py converter-otimizado --ano 2024 --mes 1 --sem-cache

# Conversão sequencial (sem paralelismo)
python main.py converter-otimizado --ano 2024 --mes 1 --sem-paralelismo

# Conversão com campos específicos
python main.py converter-otimizado --ano 2024 --mes 1 --campos ADMITIDOS DESLIGADOS SALDO
```

### 🚀 Consolidação Otimizada
```bash
# Consolidação anual com cache e paralelismo
python main.py consolidar-otimizado --ano 2024

# Consolidação sem cache
python main.py consolidar-otimizado --ano 2024 --sem-cache

# Consolidação sequencial (sem paralelismo)
python main.py consolidar-otimizado --ano 2024 --sem-paralelismo
```

### 📊 Gerenciamento de Cache
```bash
# Exibir estatísticas detalhadas do cache
python main.py estatisticas-cache

# Limpar arquivos de cache expirados
python main.py limpar-cache
```

### 🎯 Benefícios da Fase 5.1

- **⚡ Performance**: Processamento paralelo otimizado
- **💾 Cache Inteligente**: Reutilização de dados processados
- **📊 Estatísticas**: Monitoramento detalhado de performance
- **🧹 Limpeza Automática**: Gerenciamento automático de cache
- **🎛️ Controle Granular**: Opções para desabilitar cache/paralelismo

## 📁 Estrutura do Projeto

```
caged/
├── main.py                    # Script principal
├── requirements.txt           # Dependências
├── README.md                 # Este arquivo
├── db/                       # Classificações auxiliares
├── files-zip/                # Arquivos baixados (7z)
├── files-unzip/              # Arquivos descompactados
├── parquet/                  # Dados processados (Parquet)
├── logs/                     # Logs de execução
└── src/util/                 # Módulos do sistema
    ├── gerenciador_ftp.py    # Download FTP
    ├── conversor_parquet.py  # Conversão de dados
    └── ...
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

- [x] ✅ Estrutura inicial do projeto
- [ ] 🔄 Módulo de download FTP
- [ ] 🔄 Conversor Parquet
- [ ] 🔄 Pipeline de processamento
- [ ] 🔄 Interface de comandos
- [ ] 🔄 Testes e validação

## 📞 Suporte

Para dúvidas, problemas ou sugestões, consulte a documentação completa ou abra uma issue no repositório.

---

**Desenvolvido com ❤️ para facilitar o acesso aos dados do mercado de trabalho brasileiro.**