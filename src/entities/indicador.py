from dataclasses import dataclass

@dataclass
class Indicador:
    id: int
    cnpj: str
    competencia: str  # formato AAAA-MM
    nome_indicador: str
    valor: float
    # Adicione outros campos relevantes se necessário 