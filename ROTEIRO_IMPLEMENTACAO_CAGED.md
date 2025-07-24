# 📊 Roteiro de Implementação - Processador de Dados CAGED

## 🎯 1. ANÁLISE DAS DIFERENÇAS ENTRE RAIS E CAGED

### 1.1 Características dos Dados CAGED
- **Periodicidade**: Dados mensais (vs. anuais da RAIS)
- **Escopo**: Movimentação do mercado de trabalho formal (admissões/demissões)
- **Fonte**: Ministério do Trabalho e Previdência (mesmo que RAIS)
- **Formato**: Arquivos CSV/TXT compactados
- **Estrutura**: Dados por UF e período mensal
- **Tamanho**: 8-22 MB por arquivo mensal (menor que RAIS)

### 1.2 Servidor de Dados
- **FTP Oficial**: ftp.mtps.gov.br (mesmo servidor da RAIS)
- **Diretório**: /pdet/microdados/CAGED ou /pdet/microdados/NOVO_CAGED
- **Estrutura**: Organização por ano/mês
- **Formato de Compactação**: .7z (similar à RAIS)

## 🚀 2. PLANEJAMENTO DA ARQUITETURA

### 2.1 Estrutura de Diretórios Proposta
```
caged/
├── main.py                    # Script principal
├── requirements.txt           # Dependências
├── README.md                 # Documentação
├── db/                       # Classificações auxiliares
│   └── cnae_classe_emprego_verde.csv
├── files-zip/                # Arquivos baixados
│   └── 2024/
│       ├── 01_CAGED_UF.7z
│       └── 02_CAGED_UF.7z
├── files-unzip/              # Arquivos descompactados
│   └── 2024/
│       ├── 01_CAGED_SP.txt
│       └── 01_CAGED_RJ.txt
├── parquet/                  # Arquivos convertidos
│   └── 2024/
│       ├── CAGED_2024_01.parquet
│       └── CAGED_2024.parquet (consolidado)
├── logs/                     # Logs de execução
└── src/
    └── util/
        ├── gerenciador_ftp.py      # Adaptado para CAGED
        ├── descompactador.py       # Reutilizado
        ├── conversor_parquet.py    # Adaptado para CAGED
        ├── filtro_cnae.py          # Reutilizado
        ├── pipeline_paralelo.py    # Adaptado
        └── utilitarios.py          # Reutilizado
```

### 2.2 Dependências Técnicas
- **Python**: 3.8+
- **Bibliotecas**: py7zr, polars, pyarrow, tqdm, chardet, psutil
- **Recursos**: Mesmo stack tecnológico do projeto RAIS

## 🔧 3. IMPLEMENTAÇÃO POR MÓDULOS

### 3.1 Módulo Gerenciador FTP (gerenciador_ftp.py)

#### Adaptações Necessárias:
```python
class GerenciadorArquivosCaged:
    def __init__(self, servidor_ftp="ftp.mtps.gov.br", 
                 diretorio_remoto="/pdet/microdados/NOVO_CAGED"):
        # Adaptar para estrutura mensal do CAGED
        
    def listar_arquivos_mensais(self, ano, mes=None):
        # Listar arquivos por mês específico ou todos do ano
        
    def baixar_dados_mensais(self, ano, mes, ufs=None):
        # Download seletivo por UF e período
```

#### Funcionalidades Específicas:
- Download por mês/ano
- Seleção de UFs específicas
- Verificação incremental mensal
- Nomenclatura adaptada para CAGED

### 3.2 Módulo Conversor Parquet (conversor_parquet.py)

#### Mapeamento de Campos CAGED:
```python
MAPEAMENTO_CAMPOS_CAGED = {
    'competencia': 'COMPETENCIA',
    'regiao': 'REGIAO', 
    'uf': 'UF',
    'municipio': 'MUNICIPIO',
    'cnae_2_0_classe': 'CNAE_2_0_CLASSE',
    'cnae_2_0_subclasse': 'CNAE_2_0_SUBCLASSE',
    'admitidos': 'ADMITIDOS',
    'desligados': 'DESLIGADOS',
    'saldo': 'SALDO',
    'sexo': 'SEXO',
    'faixa_etaria': 'FAIXA_ETARIA',
    'escolaridade': 'ESCOLARIDADE',
    'cbo_2002': 'CBO_2002',
    'tipo_movimentacao': 'TIPO_MOVIMENTACAO',
    'tipo_deficiencia': 'TIPO_DEFICIENCIA'
}
```

#### Campos Principais CAGED:
- **Identificação**: Competência, Região, UF, Município
- **Classificação**: CNAE 2.0, CBO 2002
- **Movimentação**: Admitidos, Desligados, Saldo
- **Demografia**: Sexo, Faixa Etária, Escolaridade
- **Especiais**: Tipo de Deficiência, Tipo de Movimentação

### 3.3 Pipeline Paralelo (pipeline_paralelo.py)

#### Adaptações para Periodicidade Mensal:
```python
class PipelineParaleloCaged:
    def processar_periodo(self, ano, mes_inicio=1, mes_fim=12):
        # Processamento mensal em paralelo
        
    def consolidar_mensal(self, ano, mes):
        # Consolidação por mês
        
    def consolidar_anual(self, ano):
        # Consolidação de todos os meses do ano
```

## 📋 4. CRONOGRAMA DE IMPLEMENTAÇÃO

### Fase 1: Preparação (1-2 semanas)
- [ ] Setup do ambiente de desenvolvimento
- [ ] Análise detalhada da estrutura FTP do CAGED
- [ ] Mapeamento completo dos campos
- [ ] Criação da estrutura base do projeto

### Fase 2: Módulos Core (2-3 semanas)
- [ ] Adaptação do GerenciadorFTP para CAGED
- [ ] Modificação do ConversorParquet
- [ ] Ajuste dos utilitários para periodicidade mensal
- [ ] Implementação do mapeamento de campos

### Fase 3: Pipeline e Funcionalidades (2 semanas)
- [ ] Adaptação do Pipeline Paralelo
- [ ] Implementação da consolidação mensal/anual
- [ ] Sistema de filtros CNAE (reutilizado)
- [ ] Funcionalidades de limpeza automática

### Fase 4: Interface e Documentação (1 semana)
- [ ] Adaptação do main.py para comandos CAGED
- [ ] Criação da documentação completa
- [ ] Testes de integração
- [ ] Guia de uso específico

### Fase 5: Testes e Otimização (1 semana)
- [ ] Testes com dados reais
- [ ] Otimização de performance
- [ ] Validação de resultados
- [ ] Correções finais

## 🎛️ 5. COMANDOS DE USO PROPOSTOS

### Download de Dados
```bash
# Baixar dados de um mês específico
python main.py --baixar --ano 2024 --mes 01

# Baixar todos os meses de um ano
python main.py --baixar --ano 2024

# Baixar faixa de meses
python main.py --baixar --ano 2024 --mes-inicio 01 --mes-fim 06

# Baixar UFs específicas
python main.py --baixar --ano 2024 --mes 01 --ufs SP RJ MG
```

### Processamento
```bash
# Pipeline completo mensal
python main.py --converter --ano 2024 --mes 01

# Processamento anual completo
python main.py --converter --ano 2024 --consolidacao-anual

# Campos específicos
python main.py --converter --ano 2024 --campos CNAE_2_0_CLASSE ADMITIDOS DESLIGADOS SALDO
```

### Consolidação
```bash
# Consolidar mês específico
python main.py --consolidar-mensal --ano 2024 --mes 01

# Consolidar ano completo
python main.py --consolidar-anual --ano 2024

# Consolidar múltiplos anos
python main.py --consolidar-historico --faixa-anos 2020 2024
```

## 📊 6. ESTRUTURA DE DADOS ESPERADA

### 6.1 Arquivo Mensal Consolidado
- **Nome**: `CAGED_2024_01.parquet`
- **Tamanho estimado**: 50-150 MB
- **Registros**: ~500K-2M por mês
- **Colunas**: ~15-25 campos padronizados

### 6.2 Arquivo Anual Consolidado
- **Nome**: `CAGED_2024.parquet`
- **Tamanho estimado**: 600MB-1.5GB
- **Registros**: ~6M-20M por ano
- **Performance**: Otimizado para análises anuais

## 🔍 7. FUNCIONALIDADES ESPECÍFICAS DO CAGED

### 7.1 Análise de Movimentação
- Separação de admissões e demissões
- Cálculo automático de saldo líquido
- Análise por tipo de movimentação
- Tendências mensais automatizadas

### 7.2 Filtros Especializados
- **Por região geográfica**: Norte, Nordeste, etc.
- **Por porte de município**: Pequeno, médio, grande
- **Por setor econômico**: Indústria, comércio, serviços
- **Por perfil demográfico**: Idade, sexo, escolaridade

### 7.3 Indicadores Automáticos
- Taxa de crescimento mensal
- Sazonalidade por setor
- Rotatividade por região
- Índices de formalização

## 🚨 8. CONSIDERAÇÕES TÉCNICAS

### 8.1 Desafios Específicos
- **Volume maior**: Dados mensais geram mais arquivos
- **Sincronização**: Dados chegam com atraso variável
- **Consistência**: Validação entre meses consecutivos
- **Performance**: Otimização para queries temporais

### 8.2 Soluções Propostas
- Cache inteligente por período
- Validação cruzada automática
- Indexação temporal otimizada
- Particionamento por ano/mês

## 📈 9. BENEFÍCIOS ESPERADOS

### 9.1 Para Usuários
- Acesso facilitado a dados mensais do CAGED
- Análises temporais automatizadas
- Dados padronizados e limpos
- Múltiplos formatos de exportação

### 9.2 Para Pesquisadores
- Séries históricas consolidadas
- Filtros avançados por critérios específicos
- Integração com dados da RAIS
- APIs para análises automatizadas

## 🎯 10. PRÓXIMOS PASSOS

1. **Validar acesso ao FTP**: Confirmar estrutura atual dos dados CAGED
2. **Criar projeto piloto**: Implementar versão mínima para 1 mês
3. **Testar com dados reais**: Validar toda a pipeline
4. **Documentar diferenças**: Criar guia de migração RAIS→CAGED
5. **Lançar versão beta**: Coletar feedback da comunidade

---

## 📝 Observações Adicionais

### Comparação com Projeto RAIS Atual

| Aspecto | RAIS | CAGED |
|---------|------|-------|
| Periodicidade | Anual | Mensal |
| Tamanho por arquivo | ~500MB-2.5GB | ~8-22MB |
| Complexidade | Alta (muitos campos) | Média (foco em movimentação) |
| Frequência de atualização | 1x/ano | 12x/ano |
| Casos de uso | Análise estrutural | Análise conjuntural |

### Integração com Projeto Existente

- **Reutilização de código**: ~70% do código atual pode ser aproveitado
- **Bibliotecas**: Mesmas dependências técnicas
- **Infraestrutura**: Mesma estrutura de FTP e processamento
- **Experiência**: Conhecimento adquirido acelera desenvolvimento

Este roteiro fornece uma base sólida para implementar um processador de dados CAGED com as mesmas características técnicas do projeto RAIS, adaptado para as especificidades dos dados mensais de movimentação do mercado de trabalho formal. 