from enum import Enum


class TipoMovimentacao(Enum):
    """Códigos de tipo de movimentação conforme layout oficial do CAGED"""
    ADMISSAO_PRIMEIRO_EMPREGO = 10
    ADMISSAO_REEMPREGO = 20
    ADMISSAO_CONTRATO_PRAZO_DETERMINADO = 25
    DESLIGAMENTO_DEMISSAO_SEM_JUSTA_CAUSA = 31
    DESLIGAMENTO_DEMISSAO_COM_JUSTA_CAUSA = 32
    CULPA_RECIPROCA = 33
    ADMISSAO_REINTEGRACAO = 35
    DESLIGAMENTO_A_PEDIDO = 40
    TERMINO_CONTRATO_PRAZO_DETERMINADO = 43
    DESLIGAMENTO_TERMINO_CONTRATO = 45
    DESLIGAMENTO_APOSENTADORIA = 50
    DESLIGAMENTO_MORTE = 60
    ADMISSAO_TRANSFERENCIA = 70
    DESLIGAMENTO_TRANSFERENCIA = 80
    DESLIGAMENTO_ACORDO_EMPREGADO_EMPREGADOR = 90
    ADMISSAO_TIPO_IGNORADO = 97
    DESLIGAMENTO_TIPO_IGNORADO = 98
    NAO_IDENTIFICADO = 99
    
    @classmethod
    def get_admissoes(cls):
        """Retorna lista de códigos de admissão"""
        return [10, 20, 25, 35, 70, 97]
    
    @classmethod
    def get_desligamentos(cls):
        """Retorna lista de códigos de desligamento"""
        return [31, 32, 33, 40, 43, 45, 50, 60, 80, 90, 98]