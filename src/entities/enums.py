"""Enums para códigos oficiais do CAGED

Este módulo mantém compatibilidade com importações existentes,
mas agora os enums estão organizados em arquivos separados.

Para novos desenvolvimentos, recomenda-se importar diretamente
dos módulos específicos em src.entities.enums.*
"""

# Importações dos enums organizados em arquivos separados
from .enums.tipo_movimentacao import TipoMovimentacao
from .enums.sexo import Sexo
from .enums.regiao import Regiao
from .enums.grau_instrucao import GrauInstrucao
from .enums.raca_cor import RacaCor
from .enums.categoria import Categoria
from .enums.tipo_empregador import TipoEmpregador
from .enums.tipo_estabelecimento import TipoEstabelecimento
from .enums.indicador_aprendiz import IndicadorAprendiz
from .enums.indicador_exclusao import IndicadorExclusao
from .enums.indicador_fora_prazo import IndicadorForaPrazo
from .enums.indicador_trabalho_intermitente import IndicadorTrabalhoIntermitente
from .enums.indicador_trabalho_parcial import IndicadorTrabalhoParcial
from .enums.tamanho_estabelecimento import TamanhoEstabelecimento
from .enums.tipo_deficiencia import TipoDeficiencia
from .enums.origem_informacao import OrigemInformacao
from .enums.unidade_salario import UnidadeSalario
from .enums.uf import UF

# Mantém compatibilidade com código existente
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