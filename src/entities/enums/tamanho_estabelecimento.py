from enum import Enum


class TamanhoEstabelecimento(Enum):
    """Códigos de tamanho do estabelecimento conforme layout oficial do CAGED"""
    ZERO = 0
    DE_1_A_4 = 1
    DE_5_A_9 = 2
    DE_10_A_19 = 3
    DE_20_A_49 = 4
    DE_50_A_99 = 5
    DE_100_A_249 = 6
    DE_250_A_499 = 7
    DE_500_A_999 = 8
    MIL_OU_MAIS = 9
    NAO_IDENTIFICADO = 99