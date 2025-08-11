from enum import Enum


class IndicadorTrabalhoParcial(Enum):
    """Códigos de indicador de trabalho parcial conforme layout oficial do CAGED"""
    SIM = 1
    NAO = 2
    NAO_IDENTIFICADO = 9