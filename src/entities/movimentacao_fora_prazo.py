from dataclasses import dataclass
from typing import Optional
from datetime import date

@dataclass
class MovimentacaoForaPrazo:
    id: int
    cnpj: str
    cpf: str
    competencia_original: str  # formato AAAA-MM
    tipo_movimentacao: str  # 'admissao' ou 'desligamento'
    data_envio: Optional[date] = None
    # Adicione outros campos relevantes conforme o layout 