from enum import Enum


class Categoria(Enum):
    """Códigos de categoria conforme layout oficial do CAGED"""
    EMPREGADO_GERAL = 101
    EMPREGADO_DOMESTICO = 102
    EMPREGADO_RURAL = 103
    APRENDIZ = 104
    TEMPORARIO = 106
    INTERMITENTE = 111
    NAO_IDENTIFICADO = 999