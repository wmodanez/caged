#!/usr/bin/env python3
"""
Processador de Dados CAGED - Versão Refatorada
Script principal para download, processamento e consolidação de dados mensais

Versão: 3.0.0 - Implementação do Item 1.4 do Plano de Melhorias
Data: 2024

Melhorias implementadas:
- Estrutura de arquivos refatorada
- Nova arquitetura CLI modular
- Sistema de configuração YAML
- Pipeline de processamento otimizado
- Validações centralizadas aprimoradas
"""

import sys
from pathlib import Path

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Importar CLI refatorada
from src.cli.commands import cli


# ============================================================================
# PONTO DE ENTRADA PRINCIPAL
# ============================================================================

if __name__ == '__main__':
    # Criar diretórios necessários se não existirem
    for dir_name in ['files-zip', 'files-unzip', 'files-parquet', 'logs', 'cache', 'config']:
        Path(dir_name).mkdir(exist_ok=True)
    
    # Executar CLI
    cli()