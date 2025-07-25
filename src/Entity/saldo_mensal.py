from dataclasses import dataclass

@dataclass
class SaldoMensal:
    id: int
    cnpj: str
    competencia: str  # formato AAAA-MM
    saldo: int
    admissoes: int
    desligamentos: int
    exc_admissoes: int
    exc_desligamentos: int
    # Adicione outros campos relevantes se necessário 