from enum import Enum


class Regiao(Enum):
    """Códigos de região conforme layout oficial do CAGED"""
    NORTE = 1
    NORDESTE = 2
    SUDESTE = 3
    SUL = 4
    CENTRO_OESTE = 5