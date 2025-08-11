from enum import Enum


class OrigemInformacao(Enum):
    """Códigos de origem da informação conforme layout oficial do CAGED"""
    ESOCIAL = 1
    CAGED = 2
    EMPREGADOR_WEB = 3