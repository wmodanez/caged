from dataclasses import dataclass
from typing import Optional
from datetime import date

@dataclass
class Exclusao:
    id: int
    cnpj: str
    cpf: str
    competencia: str  # formato AAAA-MM
    tipo_exclusao: str  # 'admissao' ou 'desligamento'
    data_exclusao: Optional[date] = None
    # Adicione outros campos relevantes conforme o layout 