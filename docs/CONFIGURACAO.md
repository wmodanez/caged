# Sistema de Configuração CAGED

Este documento descreve o sistema de configuração do projeto CAGED, incluindo como usar perfis, configurar diferentes ambientes e gerenciar configurações via CLI.

## 📋 Visão Geral

O sistema de configuração do CAGED utiliza arquivos YAML para definir configurações organizadas em perfis. Cada perfil pode ser otimizado para diferentes ambientes (desenvolvimento, produção, testes, etc.).

## 🗂️ Estrutura de Configuração

### Arquivo Principal: `config.yaml`

O arquivo de configuração principal está localizado na raiz do projeto e contém múltiplos perfis:

```yaml
profiles:
  default:
    ftp: { ... }
    processing: { ... }
    output: { ... }
    logging: { ... }
  
  dev:
    # Configurações para desenvolvimento
  
  prod:
    # Configurações para produção
```

### Seções de Configuração

#### 🌐 FTP
Configurações para conexão com o servidor FTP do Ministério do Trabalho:

```yaml
ftp:
  server: "ftp.mtps.gov.br"           # Servidor FTP
  directory: "/pdet/microdados/NOVO CAGED"  # Diretório dos arquivos
  timeout: 30                          # Timeout em segundos
  max_retries: 3                       # Número máximo de tentativas
```

#### ⚙️ Processamento
Configurações de processamento e paralelismo:

```yaml
processing:
  max_workers: 4                       # Número de workers paralelos
  chunk_size: 10000                    # Tamanho dos chunks
  memory_limit_gb: 4.0                 # Limite de memória em GB
  enable_parallel: true                # Habilitar processamento paralelo
```

#### 📤 Saída
Configurações de formato e saída de dados:

```yaml
output:
  format: "parquet"                    # Formato: parquet, csv, json
  compression: "snappy"                # Compressão: snappy, gzip, none
  directory: "output"                  # Diretório de saída
```

#### 📝 Logging
Configurações de logging e debug:

```yaml
logging:
  level: "INFO"                        # Nível: DEBUG, INFO, WARNING, ERROR
  enable_file: true                    # Habilitar log em arquivo
  enable_console: true                 # Habilitar log no console
  use_emojis: true                     # Usar emojis nos logs
```

## 🎯 Perfis Disponíveis

### `default` - Perfil Padrão
- Configuração balanceada para uso geral
- 4 workers paralelos
- Formato Parquet com compressão Snappy
- Logs em nível INFO

### `dev` - Desenvolvimento
- Configuração otimizada para desenvolvimento
- Processamento sequencial para debug
- Logs em nível DEBUG
- Debug mode habilitado

### `prod` - Produção
- Configuração otimizada para performance
- 8 workers paralelos
- Compressão GZIP para economia de espaço
- Logs apenas de WARNING e ERROR
- Sem emojis nos logs

### `test` - Testes
- Configuração mínima para testes
- Processamento sequencial
- Formato CSV simples
- Logs apenas de ERROR

### `ci` - Integração Contínua
- Configuração otimizada para CI/CD
- 2 workers paralelos
- Sem emojis nos logs
- Timeouts reduzidos

## 🛠️ Comandos CLI

### Criar Configuração Padrão
```bash
# Criar arquivo config.yaml com perfil padrão
python main.py config-create

# Criar com perfil específico
python main.py config-create --profile dev
```

### Visualizar Configuração
```bash
# Mostrar configuração atual
python main.py config-show

# Mostrar configuração de perfil específico
python main.py --profile prod config-show
```

### Listar Perfis
```bash
# Listar todos os perfis disponíveis
python main.py config-list
```

### Validar Configuração
```bash
# Validar configuração atual
python main.py config-validate

# Validar perfil específico
python main.py --profile prod config-validate
```

### Modificar Configuração
```bash
# Alterar número de workers
python main.py config-set --section processing --key max_workers --value 8

# Alterar timeout FTP
python main.py config-set --section ftp --key timeout --value 60

# Alterar nível de log
python main.py config-set --section logging --key level --value DEBUG
```

## 🔧 Uso com Diferentes Perfis

### Especificar Perfil na Linha de Comando
```bash
# Usar perfil de desenvolvimento
python main.py --profile dev processar --ano 2024 --mes 3

# Usar perfil de produção
python main.py --profile prod processar --ano 2024 --todos-meses

# Usar perfil de teste
python main.py --profile test processar --ano 2024 --mes 1 --dry-run
```

### Arquivo de Configuração Personalizado
```bash
# Usar arquivo de configuração específico
python main.py --config minha_config.yaml processar --ano 2024 --mes 3

# Combinar arquivo e perfil personalizados
python main.py --config prod_config.yaml --profile prod processar --ano 2024 --todos-meses
```

## 🌍 Variáveis de Ambiente

O sistema também suporta sobrescrita via variáveis de ambiente com prefixo `CAGED_`:

```bash
# Configurações FTP
export CAGED_FTP_SERVER="meu.servidor.ftp"
export CAGED_FTP_TIMEOUT="60"

# Configurações de processamento
export CAGED_MAX_WORKERS="8"

# Configurações de saída
export CAGED_OUTPUT_FORMAT="csv"

# Configurações de log
export CAGED_LOG_LEVEL="DEBUG"
export CAGED_DEBUG_MODE="true"
```

## 📝 Exemplos Práticos

### Desenvolvimento Local
```bash
# Configurar para desenvolvimento
python main.py --profile dev config-show
python main.py --profile dev processar --ano 2024 --mes 3 --dry-run
```

### Produção
```bash
# Configurar para produção
python main.py --profile prod config-validate
python main.py --profile prod processar --ano 2024 --todos-meses --workers 8
```

### Testes Automatizados
```bash
# Configurar para testes
python main.py --profile test processar --ano 2024 --mes 1 --dry-run
```

### CI/CD
```bash
# Configurar para integração contínua
python main.py --profile ci processar --ano 2024 --mes 3
```

## 🔍 Validações

O sistema realiza validações automáticas:

- **FTP**: Servidor não pode estar vazio, timeout deve ser positivo
- **Processamento**: Número de workers deve ser positivo, limite de memória deve ser positivo
- **Saída**: Formato deve ser válido (parquet, csv, json)

## 🚨 Solução de Problemas

### Erro de Configuração Inválida
```bash
# Validar configuração
python main.py config-validate

# Recriar configuração padrão
python main.py config-create --profile default
```

### Perfil Não Encontrado
```bash
# Listar perfis disponíveis
python main.py config-list

# Criar novo perfil baseado no padrão
python main.py config-create --profile meu_perfil
```

### Problemas de Permissão
```bash
# Verificar permissões do arquivo
ls -la config.yaml

# Recriar com permissões corretas
python main.py config-create
```

## 📚 Referências

- [Documentação PyYAML](https://pyyaml.org/wiki/PyYAMLDocumentation)
- [Click Documentation](https://click.palletsprojects.com/)
- [Dataclasses Python](https://docs.python.org/3/library/dataclasses.html)