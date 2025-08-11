from enum import Enum


class TipoEmpregador(Enum):
    """Códigos de tipo de empregador conforme layout oficial do CAGED"""
    CNPJ_RAIZ = 1
    CPF = 2
    NAO_IDENTIFICADO = 9