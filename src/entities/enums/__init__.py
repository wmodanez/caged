"""Enums para códigos oficiais do CAGED

Este módulo contém todos os enums baseados no layout oficial do CAGED,
organizados em arquivos separados para melhor manutenibilidade.
"""

from .tipo_movimentacao import TipoMovimentacao
from .sexo import Sexo
from .regiao import Regiao
from .grau_instrucao import GrauInstrucao
from .raca_cor import RacaCor
from .categoria import Categoria
from .tipo_empregador import TipoEmpregador
from .tipo_estabelecimento import TipoEstabelecimento
from .indicador_aprendiz import IndicadorAprendiz
from .indicador_exclusao import IndicadorExclusao
from .indicador_fora_prazo import IndicadorForaPrazo
from .indicador_trabalho_intermitente import IndicadorTrabalhoIntermitente
from .indicador_trabalho_parcial import IndicadorTrabalhoParcial
from .tamanho_estabelecimento import TamanhoEstabelecimento
from .tipo_deficiencia import TipoDeficiencia
from .origem_informacao import OrigemInformacao
from .unidade_salario import UnidadeSalario
from .uf import UF

__all__ = [
    'TipoMovimentacao',
    'Sexo',
    'Regiao',
    'GrauInstrucao',
    'RacaCor',
    'Categoria',
    'TipoEmpregador',
    'TipoEstabelecimento',
    'IndicadorAprendiz',
    'IndicadorExclusao',
    'IndicadorForaPrazo',
    'IndicadorTrabalhoIntermitente',
    'IndicadorTrabalhoParcial',
    'TamanhoEstabelecimento',
    'TipoDeficiencia',
    'OrigemInformacao',
    'UnidadeSalario',
    'UF'
]