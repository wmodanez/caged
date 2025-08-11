from enum import Enum


class GrauInstrucao(Enum):
    """Códigos de grau de instrução conforme layout oficial do CAGED"""
    ANALFABETO = 1
    ATE_5_INCOMPLETO = 2
    ATE_5_COMPLETO = 3
    DE_6_A_9_INCOMPLETO = 4
    FUNDAMENTAL_COMPLETO = 5
    MEDIO_INCOMPLETO = 6
    MEDIO_COMPLETO = 7
    SUPERIOR_INCOMPLETO = 8
    SUPERIOR_COMPLETO = 9
    NAO_IDENTIFICADO = 99