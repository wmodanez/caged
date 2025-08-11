from enum import Enum


class TipoEstabelecimento(Enum):
    """Códigos de tipo de estabelecimento conforme layout oficial do CAGED"""
    CNPJ = 1
    CAEPF = 2
    CNO = 3
    CEI = 4
    NAO_IDENTIFICADO = 9