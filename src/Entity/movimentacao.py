from dataclasses import dataclass
from typing import Optional
from datetime import date

@dataclass
class Movimentacao:
    id: int
    cnpj: str
    cpf: str
    competencia: str  # formato AAAA-MM
    tipo_movimentacao: str  # 'admissao' ou 'desligamento'
    data_movimentacao: Optional[date] = None
    # Adicione outros campos relevantes conforme o layout 