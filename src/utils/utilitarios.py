import re
import unicodedata
import time
from typing import List, Dict, Any
import logging
from contextlib import contextmanager

import logging
import shutil
from pathlib import Path

# Usar o logger padrão
logger = logging.getLogger(__name__)





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
        
        logger.debug(f"Iniciando medição de tempo para: {nome_processo}")
    
    def iniciar_processo(self):
        """Inicia a medição do tempo total do processo."""
        self.tempo_inicio = time.time()
        logger.debug(f"Processo '{self.nome_processo}' iniciado")
    
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
        logger.debug(f"Etapa '{nome_etapa}' iniciada")
    
    def finalizar_etapa(self):
        """Finaliza a medição da etapa atual."""
        if self.etapa_atual and self.tempo_etapa_inicio:
            tempo_etapa = time.time() - self.tempo_etapa_inicio
            self.etapas[self.etapa_atual] = tempo_etapa
            tempo_formatado = formatar_tempo(tempo_etapa)
            logger.debug(f"Etapa '{self.etapa_atual}' finalizada em {tempo_formatado}")
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
    Baseado no layout oficial do Novo CAGED.
    """
    nome_padronizado = padronizar_nome_coluna(nome_coluna)
    palavras_para_remover = ['DE', 'DO', 'DA', 'DOS', 'DAS', 'E', 'EM', 'COM', 'PARA', 'POR']
    palavras = nome_padronizado.split('_')
    palavras_filtradas = [p for p in palavras if p not in palavras_para_remover]
    nome_final = '_'.join(palavras_filtradas)
    
    # Casos especiais baseados no layout oficial do CAGED
    # Campos temporais
    if 'COMPETENCIAMOV' in nome_final:
        nome_final = 'COMPETENCIA_MOV'
    if 'COMPETENCIAEXC' in nome_final:
        nome_final = 'COMPETENCIA_EXC'
    if 'COMPETENCIADEC' in nome_final:
        nome_final = 'COMPETENCIA_DEC'
    
    # Localização geográfica
    if 'REGIAO' in nome_final:
        nome_final = 'REGIAO'
    if 'MUNICIPIO' in nome_final:
        nome_final = 'MUNICIPIO'
    
    # Atividade econômica
    if 'SECAO' in nome_final:
        nome_final = 'SECAO'
    if 'SUBCLASSE' in nome_final:
        nome_final = 'SUBCLASSE'
    
    # Movimentação
    if 'SALDOMOVIMENTACAO' in nome_final:
        nome_final = 'SALDO_MOVIMENTACAO'
    if 'TIPOMOVIMENTACAO' in nome_final:
        nome_final = 'TIPO_MOVIMENTACAO'
    
    # Características do trabalhador
    if 'CATEGORIA' in nome_final:
        nome_final = 'CATEGORIA'
    if 'CBO2002OCUPACAO' in nome_final:
        nome_final = 'CBO2002_OCUPACAO'
    if 'GRAUINSTRUCAO' in nome_final or 'GRAUDEINSTRUCAO' in nome_final:
        nome_final = 'GRAU_INSTRUCAO'
    if 'IDADE' in nome_final:
        nome_final = 'IDADE'
    if 'HORASCONTRATUAIS' in nome_final:
        nome_final = 'HORAS_CONTRATUAIS'
    if 'RACACOR' in nome_final:
        nome_final = 'RACA_COR'
    if 'SEXO' in nome_final:
        nome_final = 'SEXO'
    
    # Características do empregador/estabelecimento
    if 'CNPJRAIZ' in nome_final or 'CNPJ_RAIZ' in nome_final:
        nome_final = 'CNPJ_RAIZ'
    if 'TIPOEMPREGADOR' in nome_final:
        nome_final = 'TIPO_EMPREGADOR'
    if 'TIPOESTABELECIMENTO' in nome_final:
        nome_final = 'TIPO_ESTABELECIMENTO'
    if 'TAMESTABJAN' in nome_final:
        nome_final = 'TAM_ESTAB_JAN'
    
    # Indicadores especiais
    if 'TIPODEDEFICIENCIA' in nome_final or 'TIPODEFICIENCIA' in nome_final or 'TIPO_DEFICIENCIA' in nome_final:
        nome_final = 'TIPO_DEFICIENCIA'
    if 'INDTRABINTERMITENTE' in nome_final:
        nome_final = 'IND_TRAB_INTERMITENTE'
    if 'INDTRABPARCIAL' in nome_final:
        nome_final = 'IND_TRAB_PARCIAL'
    if 'INDICADORAPRENDIZ' in nome_final:
        nome_final = 'IND_APRENDIZ'
    if 'INDICADORDEEXCLUSAO' in nome_final:
        nome_final = 'INDICADOR_EXCLUSAO'
    if 'INDICADORDEFORADOPRAZO' in nome_final:
        nome_final = 'INDICADOR_FORA_PRAZO'
    
    # Informações salariais
    if 'SALARIO' in nome_final and 'CODIGO' not in nome_final and 'FIXO' not in nome_final:
        nome_final = 'SALARIO'
    if 'UNIDADESALARIOCODIGO' in nome_final:
        nome_final = 'UNIDADE_SALARIO_CODIGO'
    if 'VALORSALARIOFIXO' in nome_final:
        nome_final = 'VALOR_SALARIO_FIXO'
    
    # Origem e controle
    if 'ORIGEMDAINFORMACAO' in nome_final:
        nome_final = 'ORIGEM_INFORMACAO'
    
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
    
    # Resolver duplicatas de forma mais robusta
    mapeamento = resolver_colunas_duplicadas(mapeamento)
    
    logger.debug(f"Padronização concluída: {len(mapeamento)} colunas processadas")
    return mapeamento


def resolver_colunas_duplicadas(mapeamento: Dict[str, str]) -> Dict[str, str]:
    """
    Resolve colunas duplicadas após padronização adicionando sufixos únicos.
    
    Args:
        mapeamento: Mapeamento original com possíveis duplicatas
        
    Returns:
        Mapeamento com duplicatas resolvidas
    """
    nomes_padronizados = list(mapeamento.values())
    duplicados = set([nome for nome in nomes_padronizados if nomes_padronizados.count(nome) > 1])
    
    if duplicados:
        logger.warning(f"Colunas duplicadas detectadas: {duplicados}")
        contadores = {}
        mapeamento_corrigido = {}
        
        for coluna_original, nome_padronizado in mapeamento.items():
            if nome_padronizado in duplicados:
                contadores[nome_padronizado] = contadores.get(nome_padronizado, 0) + 1
                if contadores[nome_padronizado] == 1:
                    # Primeira ocorrência mantém o nome original
                    mapeamento_corrigido[coluna_original] = nome_padronizado
                else:
                    # Ocorrências subsequentes recebem sufixo
                    novo_nome = f"{nome_padronizado}_{contadores[nome_padronizado]}"
                    mapeamento_corrigido[coluna_original] = novo_nome
                    logger.debug(f"Coluna renomeada para evitar duplicação: '{coluna_original}' -> '{novo_nome}'")
            else:
                mapeamento_corrigido[coluna_original] = nome_padronizado
        
        return mapeamento_corrigido
    
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
    
    logger.debug(f"Colunas renomeadas: {len(mapeamento_colunas)} colunas processadas")
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
    Versão expandida com mais flexibilidade para variações de nomes.
    
    Args:
        df_colunas: Lista de colunas do DataFrame
        
    Returns:
        Dicionário com resultado da validação
    """
    # Campos essenciais com suas possíveis variações
    campos_essenciais = {
        'COMPETENCIA_MOV': [
            'COMPETENCIA_MOV', 'COMPETENCIA', 'COMPETÊNCIA', 'ANO_MES', 'PERIODO', 'PERÍODO',
            'DATA_COMPETENCIA', 'MES_ANO', 'COMP', 'ANOCOMP'
        ],
        'MUNICIPIO': [
            'MUNICIPIO', 'MUNICÍPIO', 'COD_MUNICIPIO', 'CODIGO_MUNICIPIO',
            'IBGE', 'COD_IBGE', 'CODIGO_IBGE', 'MUNIC', 'CD_MUNICIPIO'
        ],
        'TIPO_MOVIMENTACAO': [
            'TIPO_MOVIMENTACAO', 'TIPO_MOVIMENTAÇÃO', 'MOVIMENTACAO', 'MOVIMENTO',
            'TIPMOV', 'COD_MOVIMENTACAO', 'CODIGO_MOVIMENTACAO', 'MOV', 'TIPOMOV'
        ],
        'SALDO_MOVIMENTACAO': [
            'SALDO_MOVIMENTACAO', 'SALDO_MOVIMENTAÇÃO', 'SALDO', 'VALOR_MOVIMENTACAO',
            'VL_MOVIMENTACAO', 'SALDOMOV', 'MOVIMENTACAO_VALOR'
        ]
    }
    
    # Campos opcionais mas importantes
    campos_opcionais = {
        'REGIAO': ['REGIAO', 'REGIÃO', 'REGIAO_GEOGRAFICA', 'REG'],
        'UF': ['UF', 'ESTADO', 'UNIDADE_FEDERACAO', 'SIGLA_UF'],
        'SECAO': ['SECAO', 'SEÇÃO', 'SECAO_CNAE', 'SEÇÃO_CNAE'],
        'SUBCLASSE': ['SUBCLASSE', 'SUBCLASSE_CNAE', 'CNAE_SUBCLASSE'],
        'CATEGORIA': ['CATEGORIA', 'CATEGORIA_TRABALHADOR', 'CAT'],
        'CBO2002_OCUPACAO': ['CBO2002_OCUPACAO', 'CBO', 'OCUPACAO', 'OCUPAÇÃO'],
        'IDADE': ['IDADE', 'ID', 'FAIXA_ETARIA', 'FAIXA_ETÁRIA'],
        'SEXO': ['SEXO', 'GENERO', 'GÊNERO', 'SEX'],
        'GRAU_INSTRUCAO': ['GRAU_INSTRUCAO', 'GRAU_INSTRUÇÃO', 'ESCOLARIDADE', 'ESC']
    }
    
    # Padronizar colunas recebidas
    colunas_padronizadas = [padronizar_nome_coluna(col) for col in df_colunas]
    colunas_upper = [col.upper() for col in colunas_padronizadas]
    
    campos_encontrados = {}
    campos_faltantes = []
    campos_opcionais_encontrados = {}
    
    # Validar campos essenciais
    for campo_essencial, variacoes in campos_essenciais.items():
        encontrado = False
        for variacao in variacoes:
            if variacao.upper() in colunas_upper:
                idx = colunas_upper.index(variacao.upper())
                campos_encontrados[campo_essencial] = df_colunas[idx]
                encontrado = True
                break
        
        if not encontrado:
            campos_faltantes.append(campo_essencial)
    
    # Verificar campos opcionais
    for campo_opcional, variacoes in campos_opcionais.items():
        for variacao in variacoes:
            if variacao.upper() in colunas_upper:
                idx = colunas_upper.index(variacao.upper())
                campos_opcionais_encontrados[campo_opcional] = df_colunas[idx]
                break
    
    resultado = {
        'valido': len(campos_faltantes) == 0,
        'campos_encontrados': campos_encontrados,
        'campos_faltantes': campos_faltantes,
        'campos_opcionais_encontrados': campos_opcionais_encontrados,
        'total_colunas': len(df_colunas),
        'cobertura_essencial': (len(campos_encontrados) / len(campos_essenciais)) * 100,
        'cobertura_opcional': (len(campos_opcionais_encontrados) / len(campos_opcionais)) * 100,
        'colunas_padronizadas': colunas_padronizadas
    }
    
    if campos_faltantes:
        logger.error(f"Campos essenciais ausentes no arquivo CAGED: {campos_faltantes}")
    else:
        logger.debug("Todos os campos essenciais do CAGED estão presentes")
    
    logger.debug(f"Cobertura essencial: {resultado['cobertura_essencial']:.1f}%")
    logger.debug(f"Cobertura opcional: {resultado['cobertura_opcional']:.1f}%")
    
    return resultado


def criar_mapeamento_caged_flexivel(df_colunas: List[str]) -> Dict[str, str]:
    """
    Cria um mapeamento flexível para colunas CAGED, tentando identificar
    automaticamente campos mesmo com variações de nomenclatura.
    Baseado no layout oficial do Novo CAGED.
    
    Args:
        df_colunas: Lista de colunas originais
        
    Returns:
        Mapeamento otimizado para CAGED
    """
    # Mapeamento base com variações conhecidas baseado no layout oficial
    mapeamentos_conhecidos = {
        # Campos temporais
        'competencia': 'COMPETENCIA_MOV',
        'competência': 'COMPETENCIA_MOV',
        'competenciamov': 'COMPETENCIA_MOV',
        'competência da movimentação': 'COMPETENCIA_MOV',
        'competenciaexc': 'COMPETENCIA_EXC',
        'competência da exclusão': 'COMPETENCIA_EXC',
        'competenciadec': 'COMPETENCIA_DEC',
        'competência da declaração': 'COMPETENCIA_DEC',
        
        # Localização geográfica
        'regiao': 'REGIAO',
        'região': 'REGIAO',
        'região geográfica': 'REGIAO',
        'uf': 'UF',
        'estado': 'UF',
        'unidade da federação': 'UF',
        'municipio': 'MUNICIPIO',
        'município': 'MUNICIPIO',
        'código do município': 'MUNICIPIO',
        'cidade': 'MUNICIPIO',
        
        # Atividade econômica
        'secao': 'SECAO',
        'seção': 'SECAO',
        'seção cnae': 'SECAO',
        'subclasse': 'SUBCLASSE',
        'subclasse cnae': 'SUBCLASSE',
        'cnae subclasse': 'SUBCLASSE',
        
        # Movimentação
        'saldo': 'SALDO_MOVIMENTACAO',
        'saldo_movimentacao': 'SALDO_MOVIMENTACAO',
        'saldo_movimentação': 'SALDO_MOVIMENTACAO',
        'saldomovimentação': 'SALDO_MOVIMENTACAO',
        'valor da movimentação': 'SALDO_MOVIMENTACAO',
        'tipomovimentacao': 'TIPO_MOVIMENTACAO',
        'tipo_movimentacao': 'TIPO_MOVIMENTACAO',
        'tipo_movimentação': 'TIPO_MOVIMENTACAO',
        'tipo de movimentação': 'TIPO_MOVIMENTACAO',
        
        # Características do trabalhador
        'categoria': 'CATEGORIA',
        'categoria de trabalhador': 'CATEGORIA',
        'cbo2002ocupacao': 'CBO2002_OCUPACAO',
        'cbo2002ocupação': 'CBO2002_OCUPACAO',
        'cbo 2002 ocupação': 'CBO2002_OCUPACAO',
        'ocupacao': 'CBO2002_OCUPACAO',
        'ocupação': 'CBO2002_OCUPACAO',
        'grauinstrucao': 'GRAU_INSTRUCAO',
        'grau_instrucao': 'GRAU_INSTRUCAO',
        'grau_instrução': 'GRAU_INSTRUCAO',
        'grau de instrução': 'GRAU_INSTRUCAO',
        'escolaridade': 'GRAU_INSTRUCAO',
        'idade': 'IDADE',
        'idade do trabalhador': 'IDADE',
        'horascontratuais': 'HORAS_CONTRATUAIS',
        'horas_contratuais': 'HORAS_CONTRATUAIS',
        'horas contratuais': 'HORAS_CONTRATUAIS',
        'racacor': 'RACA_COR',
        'raca_cor': 'RACA_COR',
        'raça_cor': 'RACA_COR',
        'raça ou cor': 'RACA_COR',
        'cor': 'RACA_COR',
        'raca': 'RACA_COR',
        'raça': 'RACA_COR',
        'sexo': 'SEXO',
        'sexo do trabalhador': 'SEXO',
        'genero': 'SEXO',
        'gênero': 'SEXO',
        
        # Características do empregador/estabelecimento
        'cnpjraiz': 'CNPJ_RAIZ',
        'cnpj_raiz': 'CNPJ_RAIZ',
        'cnpj raiz': 'CNPJ_RAIZ',
        'tipoempregador': 'TIPO_EMPREGADOR',
        'tipo_empregador': 'TIPO_EMPREGADOR',
        'tipo de empregador': 'TIPO_EMPREGADOR',
        'tipoestabelecimento': 'TIPO_ESTABELECIMENTO',
        'tipo_estabelecimento': 'TIPO_ESTABELECIMENTO',
        'tipo de estabelecimento': 'TIPO_ESTABELECIMENTO',
        'tamestabjan': 'TAM_ESTAB_JAN',
        'tam_estab_jan': 'TAM_ESTAB_JAN',
        'tamanho estabelecimento janeiro': 'TAM_ESTAB_JAN',
        
        # Indicadores especiais
        'tipodedeficiencia': 'TIPO_DEFICIENCIA',
        'tipo_deficiencia': 'TIPO_DEFICIENCIA',
        'tipo de deficiência': 'TIPO_DEFICIENCIA',
        'deficiencia': 'TIPO_DEFICIENCIA',
        'deficiência': 'TIPO_DEFICIENCIA',
        'indtrabintermitente': 'IND_TRAB_INTERMITENTE',
        'ind_trab_intermitente': 'IND_TRAB_INTERMITENTE',
        'indicador trabalhador intermitente': 'IND_TRAB_INTERMITENTE',
        'trabalho intermitente': 'IND_TRAB_INTERMITENTE',
        'indtrabparcial': 'IND_TRAB_PARCIAL',
        'ind_trab_parcial': 'IND_TRAB_PARCIAL',
        'indicador trabalhador parcial': 'IND_TRAB_PARCIAL',
        'trabalho parcial': 'IND_TRAB_PARCIAL',
        'indicadoraprendiz': 'IND_APRENDIZ',
        'ind_aprendiz': 'IND_APRENDIZ',
        'indicador aprendiz': 'IND_APRENDIZ',
        'aprendiz': 'IND_APRENDIZ',
        'indicadordeexclusao': 'INDICADOR_EXCLUSAO',
        'indicador_exclusao': 'INDICADOR_EXCLUSAO',
        'indicador de exclusão': 'INDICADOR_EXCLUSAO',
        'exclusao': 'INDICADOR_EXCLUSAO',
        'exclusão': 'INDICADOR_EXCLUSAO',
        'indicadordeforadoprazo': 'INDICADOR_FORA_PRAZO',
        'indicador_fora_prazo': 'INDICADOR_FORA_PRAZO',
        'indicador fora do prazo': 'INDICADOR_FORA_PRAZO',
        'fora do prazo': 'INDICADOR_FORA_PRAZO',
        
        # Informações salariais
        'salario': 'SALARIO',
        'salário': 'SALARIO',
        'salário mensal': 'SALARIO',
        'unidadesalariocodigo': 'UNIDADE_SALARIO_CODIGO',
        'unidade_salario_codigo': 'UNIDADE_SALARIO_CODIGO',
        'unidade salário código': 'UNIDADE_SALARIO_CODIGO',
        'valorsalariofixo': 'VALOR_SALARIO_FIXO',
        'valor_salario_fixo': 'VALOR_SALARIO_FIXO',
        'valor salário fixo': 'VALOR_SALARIO_FIXO',
        'salario fixo': 'VALOR_SALARIO_FIXO',
        'salário fixo': 'VALOR_SALARIO_FIXO',
        
        # Origem e controle
        'origemdainformacao': 'ORIGEM_INFORMACAO',
        'origem_informacao': 'ORIGEM_INFORMACAO',
        'origem da informação': 'ORIGEM_INFORMACAO',
        'origem': 'ORIGEM_INFORMACAO'
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