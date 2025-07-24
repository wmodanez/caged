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

## 🎯 Uso Básico

### Download de Dados
```bash
# Baixar dados de janeiro/2024
python main.py baixar --ano 2024 --mes 1

# Baixar primeiro semestre
python main.py baixar --ano 2024 --mes-inicio 1 --mes-fim 6

# Baixar UFs específicas
python main.py baixar --ano 2024 --mes 1 --ufs SP RJ MG
```

### Processamento
```bash
# Converter dados mensais
python main.py converter --ano 2024 --mes 1

# Consolidação anual
python main.py converter --ano 2024 --consolidacao-anual
```

### Status do Projeto
```bash
# Verificar arquivos processados
python main.py status
```

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