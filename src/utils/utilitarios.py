import re
import unicodedata
import time
from typing import List, Dict, Any
import logging
from contextlib import contextmanager

# Importação do sistema de logging centralizado
from src.utils.logger import LoggerConfig, setup_logger, log_structured
import logging
import shutil
from pathlib import Path

# Usar o logger centralizado configurado no main.py
logger = logging.getLogger("caged")





def formatar_tempo(segundos: float) -> str:
    """
    Formata um tempo em segundos para um formato mais legível (H:M:S).
    
    Args:
        segundos: Tempo em segundos
        
    Returns:
        String formatada no formato H:M:S ou M:S para tempos menores que 1 hora
        
    Exemplos:
        >>> formatar_tempo(3661)
        '1:01:01'
        >>> formatar_tempo(125)
        '2:05'
        >>> formatar_tempo(45.5)
        '0:45'
    """
    if segundos < 0:
        return "0:00"
    
    horas = int(segundos // 3600)
    minutos = int((segundos % 3600) // 60)
    segs = int(segundos % 60)
    
    if horas > 0:
        return f"{horas}:{minutos:02d}:{segs:02d}"
    else:
        return f"{minutos}:{segs:02d}"


class MedidorTempo:
    """
    Classe para medir o tempo de processamento de diferentes etapas do CAGED.
    Adaptada para monitorar conversões mensais, consolidações anuais e outras operações.
    """
    
    def __init__(self, nome_processo: str = "Processamento CAGED"):
        """
        Inicializa o medidor de tempo.
        
        Args:
            nome_processo: Nome do processo principal
        """
        self.nome_processo = nome_processo
        self.tempo_inicio = None
        self.tempo_fim = None
        self.etapas = {}
        self.etapa_atual = None
        self.tempo_etapa_inicio = None
        
        logger.info(f"Iniciando medição de tempo para: {nome_processo}")
    
    def iniciar_processo(self):
        """Inicia a medição do tempo total do processo."""
        self.tempo_inicio = time.time()
        logger.info(f"Processo '{self.nome_processo}' iniciado")
    
    def finalizar_processo(self):
        """Finaliza a medição do tempo total do processo."""
        self.tempo_fim = time.time()
        tempo_total = self.tempo_fim - self.tempo_inicio
        tempo_formatado = formatar_tempo(tempo_total)
        logger.info(f"Processo '{self.nome_processo}' finalizado em {tempo_formatado}")
        return tempo_total
    
    def iniciar_etapa(self, nome_etapa: str):
        """
        Inicia a medição de uma etapa específica.
        
        Args:
            nome_etapa: Nome da etapa (ex: 'Conversão Mensal', 'Consolidação Anual')
        """
        if self.etapa_atual:
            self.finalizar_etapa()
        
        self.etapa_atual = nome_etapa
        self.tempo_etapa_inicio = time.time()
        logger.info(f"Etapa '{nome_etapa}' iniciada")
    
    def finalizar_etapa(self):
        """Finaliza a medição da etapa atual."""
        if self.etapa_atual and self.tempo_etapa_inicio:
            tempo_etapa = time.time() - self.tempo_etapa_inicio
            self.etapas[self.etapa_atual] = tempo_etapa
            tempo_formatado = formatar_tempo(tempo_etapa)
            logger.info(f"Etapa '{self.etapa_atual}' finalizada em {tempo_formatado}")
            self.etapa_atual = None
            self.tempo_etapa_inicio = None
    
    @contextmanager
    def etapa(self, nome_etapa: str):
        """
        Context manager para medir o tempo de uma etapa.
        
        Args:
            nome_etapa: Nome da etapa
            
        Usage:
            with medidor.etapa("Conversão Mensal Janeiro"):
                # código da etapa
        """
        self.iniciar_etapa(nome_etapa)
        try:
            yield
        finally:
            self.finalizar_etapa()
    
    def obter_resumo(self) -> Dict[str, Any]:
        """
        Retorna um resumo completo dos tempos medidos.
        
        Returns:
            Dicionário com resumo dos tempos
        """
        # Finalizar etapa atual se houver
        if self.etapa_atual:
            self.finalizar_etapa()
        
        # Calcular tempo total
        tempo_total = 0
        if self.tempo_inicio and self.tempo_fim:
            tempo_total = self.tempo_fim - self.tempo_inicio
        elif self.tempo_inicio:
            tempo_total = time.time() - self.tempo_inicio
        
        # Calcular tempo das etapas
        tempo_etapas = sum(self.etapas.values())
        tempo_nao_medido = tempo_total - tempo_etapas
        
        resumo = {
            "processo": self.nome_processo,
            "tempo_total": tempo_total,
            "etapas": self.etapas.copy(),
            "tempo_etapas": tempo_etapas,
            "tempo_nao_medido": tempo_nao_medido,
            "percentual_etapas": (tempo_etapas / tempo_total * 100) if tempo_total > 0 else 0
        }
        
        return resumo
    
    def imprimir_resumo(self):
        """Imprime um resumo formatado dos tempos medidos."""
        resumo = self.obter_resumo()
        
        tempo_total_formatado = formatar_tempo(resumo['tempo_total'])
        tempo_etapas_formatado = formatar_tempo(resumo['tempo_etapas'])
        tempo_nao_medido_formatado = formatar_tempo(resumo['tempo_nao_medido'])
        
        print(f"\n{'='*60}")
        print(f"RESUMO DE TEMPO - {resumo['processo'].upper()}")
        print(f"{'='*60}")
        print(f"Tempo Total: {tempo_total_formatado}")
        print(f"Tempo das Etapas: {tempo_etapas_formatado} ({resumo['percentual_etapas']:.1f}%)")
        if resumo['tempo_nao_medido'] > 0:
            print(f"Tempo Não Medido: {tempo_nao_medido_formatado}")
        
        if resumo['etapas']:
            print(f"\nDetalhamento por Etapa:")
            print(f"{'Etapa':<35} {'Tempo':<10} {'% do Total':<10}")
            print(f"{'-'*35} {'-'*10} {'-'*10}")
            
            for etapa, tempo in resumo['etapas'].items():
                percentual = (tempo / resumo['tempo_total'] * 100) if resumo['tempo_total'] > 0 else 0
                tempo_formatado = formatar_tempo(tempo)
                print(f"{etapa:<35} {tempo_formatado:<10} {percentual:<10.1f}%")
        
        print(f"{'='*60}")


def padronizar_nome_coluna(nome_coluna: str) -> str:
    """
    Padroniza o nome de uma coluna removendo caracteres especiais, acentos,
    convertendo para maiúsculas e substituindo espaços por underline.
    Adaptado para campos específicos do CAGED.
    
    Args:
        nome_coluna: Nome original da coluna
        
    Returns:
        Nome da coluna padronizado
        
    Exemplos:
        >>> padronizar_nome_coluna("Código do Município")
        'CODIGO_DO_MUNICIPIO'
        >>> padronizar_nome_coluna("Saldo Movimentação")
        'SALDO_MOVIMENTACAO'
        >>> padronizar_nome_coluna("CPF do Trabalhador")
        'CPF_DO_TRABALHADOR'
    """
    if not nome_coluna:
        return ""
    
    # Converter para string se não for
    nome = str(nome_coluna).strip()
    
    # Remover acentos e normalizar caracteres Unicode
    nome = unicodedata.normalize('NFD', nome)
    nome = ''.join(c for c in nome if not unicodedata.combining(c))
    
    # Substituir caracteres especiais por espaços
    # Manter apenas letras, números e espaços
    nome = re.sub(r'[^a-zA-Z0-9\s]', ' ', nome)
    
    # Remover espaços múltiplos e converter para maiúsculas
    nome = re.sub(r'\s+', ' ', nome).strip().upper()
    
    # Substituir espaços por underline
    nome = nome.replace(' ', '_')
    
    # Remover underlines múltiplos
    nome = re.sub(r'_+', '_', nome)
    
    # Remover underlines no início e fim
    nome = nome.strip('_')
    
    # Se ficou vazio após a padronização, retornar um nome padrão
    if not nome:
        nome = "COLUNA_SEM_NOME"
    
    logger.debug(f"Coluna padronizada: '{nome_coluna}' -> '{nome}'")
    
    return nome


def padronizar_nome_coluna_caged(nome_coluna: str) -> str:
    """
    Padroniza nomes de colunas do CAGED, incluindo casos especiais e melhorias de legibilidade.
    """
    nome_padronizado = padronizar_nome_coluna(nome_coluna)
    palavras_para_remover = ['DE', 'DO', 'DA', 'DOS', 'DAS', 'E', 'EM', 'COM', 'PARA', 'POR']
    palavras = nome_padronizado.split('_')
    palavras_filtradas = [p for p in palavras if p not in palavras_para_remover]
    nome_final = '_'.join(palavras_filtradas)
    # Casos especiais
    if 'GRAUINSTRUCAO' in nome_final or 'GRAUDEINSTRUCAO' in nome_final:
        nome_final = 'GRAU_INSTRUCAO'
    if 'SALDOMOVIMENTACAO' in nome_final:
        nome_final = 'SALDO_MOVIMENTACAO'
    if 'COMPETENCIAMOV' in nome_final:
        nome_final = 'COMPETENCIA_MOV'
    if 'CBO2002OCUPACAO' in nome_final:
        nome_final = 'CBO2002_OCUPACAO'
    if 'TIPODEDEFICIENCIA' in nome_final or 'TIPODEFICIENCIA' in nome_final or 'TIPO_DEFICIENCIA' in nome_final:
        nome_final = 'TIPO_DEFICIENCIA'
    if 'HORASCONTRATUAIS' in nome_final:
        nome_final = 'HORAS_CONTRATUAIS'
    if 'RACACOR' in nome_final:
        nome_final = 'RACA_COR'
    if 'TIPOEMPREGADOR' in nome_final:
        nome_final = 'TIPO_EMPREGADOR'
    if 'TIPOESTABELECIMENTO' in nome_final:
        nome_final = 'TIPO_ESTABELECIMENTO'
    if 'TIPOMOVIMENTACAO' in nome_final:
        nome_final = 'TIPO_MOVIMENTACAO'
    if 'INDTRABINTERMITENTE' in nome_final:
        nome_final = 'IND_TRAB_INTERMITENTE'
    if 'INDTRABPARCIAL' in nome_final:
        nome_final = 'IND_TRAB_PARCIAL'
    if 'TAMESTABJAN' in nome_final:
        nome_final = 'TAM_ESTAB_JAN'
    if 'INDICADORAPRENDIZ' in nome_final:
        nome_final = 'IND_APRENDIZ'
    if 'ORIGEMDAINFORMACAO' in nome_final:
        nome_final = 'ORIGEM_INFORMACAO'
    if 'COMPETENCIAEXC' in nome_final:
        nome_final = 'COMPETENCIA_EXC'
    if 'INDICADORDEEXCLUSAO' in nome_final:
        nome_final = 'INDICADOR_EXCLUSAO'
    if 'COMPETENCIADEC' in nome_final:
        nome_final = 'COMPETENCIA_DEC'
    if 'INDICADORDEFORADOPRAZO' in nome_final:
        nome_final = 'INDICADOR_FORA_PRAZO'
    if 'UNIDADESALARIOCODIGO' in nome_final:
        nome_final = 'UNIDADE_SALARIO_CODIGO'
    if 'VALORSALARIOFIXO' in nome_final:
        nome_final = 'VALOR_SALARIO_FIXO'
    logger.debug(f"Coluna padronizada (CAGED): '{nome_coluna}' -> '{nome_final}'")
    return nome_final


def padronizar_colunas_dataframe(df_colunas: List[str], usar_versao_melhorada: bool = False) -> Dict[str, str]:
    """
    Padroniza uma lista de nomes de colunas e retorna um mapeamento
    entre os nomes originais e os padronizados.
    Especialmente útil para arquivos CAGED com variações de nomenclatura.
    """
    mapeamento = {}
    for coluna in df_colunas:
        if usar_versao_melhorada:
            nome_padronizado = padronizar_nome_coluna_caged(coluna)
        else:
            nome_padronizado = padronizar_nome_coluna(coluna)
        mapeamento[coluna] = nome_padronizado
    nomes_padronizados = list(mapeamento.values())
    duplicados = set([nome for nome in nomes_padronizados if nomes_padronizados.count(nome) > 1])
    if duplicados:
        logger.warning(f"Colunas duplicadas após padronização: {duplicados}")
        contadores = {}
        for coluna_original, nome_padronizado in mapeamento.items():
            if nome_padronizado in duplicados:
                contadores[nome_padronizado] = contadores.get(nome_padronizado, 0) + 1
                if contadores[nome_padronizado] > 1:
                    mapeamento[coluna_original] = f"{nome_padronizado}_{contadores[nome_padronizado]}"
                    logger.info(f"Coluna renomeada para evitar duplicação: '{coluna_original}' -> '{mapeamento[coluna_original]}'")
    logger.info(f"Padronização concluída: {len(mapeamento)} colunas processadas")
    return mapeamento


def aplicar_padronizacao_colunas(df, mapeamento_colunas: Dict[str, str] = None) -> Any:
    """
    Aplica a padronização de colunas em um DataFrame.
    Compatível com Polars (usado no CAGED) e Pandas.
    
    Args:
        df: DataFrame a ser processado (Polars ou Pandas)
        mapeamento_colunas: Mapeamento de nomes de colunas (opcional)
        
    Returns:
        DataFrame com colunas renomeadas
    """
    if mapeamento_colunas is None:
        mapeamento_colunas = padronizar_colunas_dataframe(df.columns)
    
    # Verificar se é Polars ou Pandas e aplicar renomeação apropriada
    try:
        # Tentar método Polars primeiro
        df_renomeado = df.rename(mapeamento_colunas)
    except AttributeError:
        # Fallback para Pandas
        df_renomeado = df.rename(columns=mapeamento_colunas)
    
    logger.info(f"Colunas renomeadas: {len(mapeamento_colunas)} colunas processadas")
    return df_renomeado


def validar_nome_coluna(nome_coluna: str) -> bool:
    """
    Valida se um nome de coluna é válido após padronização.
    
    Args:
        nome_coluna: Nome da coluna a ser validado
        
    Returns:
        True se o nome é válido, False caso contrário
    """
    nome_padronizado = padronizar_nome_coluna(nome_coluna)
    
    # Verificar se o nome não está vazio
    if not nome_padronizado:
        return False
    
    # Verificar se não começa com número
    if nome_padronizado[0].isdigit():
        return False
    
    # Verificar se contém apenas caracteres válidos
    if not re.match(r'^[A-Z0-9_]+$', nome_padronizado):
        return False
    
    return True


def obter_colunas_invalidas(colunas: List[str]) -> List[str]:
    """
    Identifica colunas com nomes inválidos.
    Útil para validar arquivos CAGED antes do processamento.
    
    Args:
        colunas: Lista de nomes de colunas
        
    Returns:
        Lista de colunas com nomes inválidos
    """
    colunas_invalidas = []
    
    for coluna in colunas:
        if not validar_nome_coluna(coluna):
            colunas_invalidas.append(coluna)
    
    if colunas_invalidas:
        logger.warning(f"Encontradas {len(colunas_invalidas)} colunas com nomes inválidos: {colunas_invalidas}")
    
    return colunas_invalidas


def validar_campos_caged(df_colunas: List[str]) -> Dict[str, Any]:
    """
    Valida se as colunas essenciais do CAGED estão presentes.
    Função específica para validação de arquivos CAGED.
    
    Args:
        df_colunas: Lista de colunas do DataFrame
        
    Returns:
        Dicionário com resultado da validação
    """
    # Campos essenciais do CAGED (após padronização)
    campos_essenciais = {
        'COMPETENCIA',
        'REGIAO',
        'UF', 
        'MUNICIPIO',
        'ADMISSOES',
        'DESLIGAMENTOS',
        'SALDO_MOVIMENTACAO'
    }
    
    # Padronizar colunas recebidas
    colunas_padronizadas = set(padronizar_nome_coluna(col) for col in df_colunas)
    
    # Verificar campos presentes e ausentes
    campos_presentes = campos_essenciais.intersection(colunas_padronizadas)
    campos_ausentes = campos_essenciais - colunas_padronizadas
    
    resultado = {
        'valido': len(campos_ausentes) == 0,
        'campos_presentes': list(campos_presentes),
        'campos_ausentes': list(campos_ausentes),
        'total_colunas': len(df_colunas),
        'colunas_padronizadas': list(colunas_padronizadas)
    }
    
    if campos_ausentes:
        logger.error(f"Campos essenciais ausentes no arquivo CAGED: {campos_ausentes}")
    else:
        logger.info("Todos os campos essenciais do CAGED estão presentes")
    
    return resultado


def criar_mapeamento_caged_flexivel(df_colunas: List[str]) -> Dict[str, str]:
    """
    Cria um mapeamento flexível para colunas CAGED, tentando identificar
    automaticamente campos mesmo com variações de nomenclatura.
    
    Args:
        df_colunas: Lista de colunas originais
        
    Returns:
        Mapeamento otimizado para CAGED
    """
    # Mapeamento base com variações conhecidas
    mapeamentos_conhecidos = {
        # Competência
        'competencia': 'COMPETENCIA',
        'competência': 'COMPETENCIA', 
        'mes': 'COMPETENCIA',
        'mês': 'COMPETENCIA',
        'periodo': 'COMPETENCIA',
        'período': 'COMPETENCIA',
        
        # Localização
        'regiao': 'REGIAO',
        'região': 'REGIAO',
        'uf': 'UF',
        'estado': 'UF',
        'municipio': 'MUNICIPIO',
        'município': 'MUNICIPIO',
        'cidade': 'MUNICIPIO',
        
        # Movimentações
        'admissoes': 'ADMISSOES',
        'admissões': 'ADMISSOES',
        'admitidos': 'ADMISSOES',
        'contratacoes': 'ADMISSOES',
        'contratações': 'ADMISSOES',
        
        'desligamentos': 'DESLIGAMENTOS',
        'demissoes': 'DESLIGAMENTOS',
        'demissões': 'DESLIGAMENTOS',
        'saidas': 'DESLIGAMENTOS',
        'saídas': 'DESLIGAMENTOS',
        
        'saldo': 'SALDO_MOVIMENTACAO',
        'saldo_movimentacao': 'SALDO_MOVIMENTACAO',
        'saldo_movimentação': 'SALDO_MOVIMENTACAO',
        'variacao': 'SALDO_MOVIMENTACAO',
        'variação': 'SALDO_MOVIMENTACAO'
    }
    
    # Criar mapeamento padrão
    mapeamento = padronizar_colunas_dataframe(df_colunas)
    
    # Aplicar mapeamentos conhecidos
    for coluna_original in df_colunas:
        coluna_lower = coluna_original.lower().strip()
        if coluna_lower in mapeamentos_conhecidos:
            mapeamento[coluna_original] = mapeamentos_conhecidos[coluna_lower]
            logger.info(f"Mapeamento específico CAGED aplicado: '{coluna_original}' -> '{mapeamentos_conhecidos[coluna_lower]}'")
    
    return mapeamento