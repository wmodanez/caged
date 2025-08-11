from enum import Enum


class Sexo(Enum):
    """Códigos de sexo conforme layout oficial do CAGED"""
    HOMEM = 1
    MULHER = 2
    NAO_IDENTIFICADO = 9