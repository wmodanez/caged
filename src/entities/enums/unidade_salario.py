from enum import Enum


class UnidadeSalario(Enum):
    """Códigos de unidade de salário conforme layout oficial do CAGED"""
    HORA = 1
    DIA = 2
    SEMANA = 3
    QUINZENA = 4
    MES = 5
    TAREFA = 6
    NAO_IDENTIFICADO = 9