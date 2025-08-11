from enum import Enum


class TipoDeficiencia(Enum):
    """Códigos de tipo de deficiência conforme layout oficial do CAGED"""
    FISICA = 1
    AUDITIVA = 2
    VISUAL = 3
    INTELECTUAL = 4
    MULTIPLA = 5
    REABILITADO = 6
    NAO_DEFICIENTE = 0
    NAO_IDENTIFICADO = 9