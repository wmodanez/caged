#!/usr/bin/env python3
"""
Entidades do Sistema CAGED
Implementação do Item 1.4 do Plano de Melhorias

Este módulo contém as classes de entidade para representar
os dados do sistema CAGED.
"""

__version__ = "3.0.0"
__author__ = "Sistema CAGED"

# Importações das entidades principais
from .movimentacao import Movimentacao
from .saldo_mensal import SaldoMensal
from .exclusao import Exclusao
from .movimentacao_fora_prazo import MovimentacaoForaPrazo
from .indicador import Indicador

__all__ = [
    'Movimentacao',
    'SaldoMensal',
    'Exclusao',
    'MovimentacaoForaPrazo',
    'Indicador'
]