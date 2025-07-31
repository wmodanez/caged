#!/usr/bin/env python3
"""
Conversor de Dados CAGED para formato Parquet
Adaptado para estrutura mensal dos dados CAGED
Versão 2.0 - Arquitetura escalável baseada no projeto RAIS
"""

# Imports padrão
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# Imports de terceiros
import polars as pl
from loguru import logger

# Imports locais - Entidades
from src.Entity.movimentacao import Movimentacao
from src.Entity.saldo_mensal import SaldoMensal
from src.Entity.exclusao import Exclusao
from src.Entity.movimentacao_fora_prazo import MovimentacaoForaPrazo
from src.Entity.indicador import Indicador

# Imports locais - Utilitários
from src.util.utilitarios import (
    MedidorTempo,
    padronizar_colunas_dataframe,
    aplicar_padronizacao_colunas,
    validar_campos_caged,
    criar_mapeamento_caged_flexivel,
    obter_colunas_invalidas
)

# ============================================================================
# CONFIGURAÇÕES GLOBAIS
# ============================================================================

# Configurações de paralelismo
CONFIG_PARALELISMO = {
    'max_workers': min(32, (os.cpu_count() or 1) + 4),  # Otimizado para I/O
    'chunk_size': 50000,  # Tamanho do chunk para processamento
    'chunk_size_min': 10000,  # Tamanho mínimo do chunk
    'chunk_size_max': 200000,  # Tamanho máximo do chunk
    'memoria_limite_mb': 2048,  # Limite de memória por worker
    'timeout_arquivo': 300,  # Timeout por arquivo (segundos)
}

# Configurações de encoding e separadores
CONFIG_ARQUIVO = {
    'encodings_fallback': ['utf-8', 'latin1', 'cp1252', 'iso-8859-1'],
    'separadores': [';', ',', '\t', '|'],
    'confianca_encoding_min': 0.7,
    'max_linhas_deteccao': 10000,
}

# Campos númericos para validação
CAMPOS_NUMERICOS = ['ADMITIDOS', 'DESLIGADOS', 'SALDO']

# Schema flexível - será aplicado dinamicamente
SCHEMA_CAGED_BASE = {
    'COMPETENCIA': pl.Utf8,
    'REGIAO': pl.Utf8,
    'UF': pl.Utf8,
    'MUNICIPIO': pl.Utf8,
    'CNAE_2_0_CLASSE': pl.Utf8,
    'CNAE_2_0_SUBCLASSE': pl.Utf8,
    'ADMITIDOS': pl.Int64,
    'DESLIGADOS': pl.Int64,
    'SALDO': pl.Int64,
    'SEXO': pl.Utf8,
    'FAIXA_ETARIA': pl.Utf8,
    'ESCOLARIDADE': pl.Utf8,
    'CBO_2002': pl.Utf8,
    'TIPO_MOVIMENTACAO': pl.Utf8,
    'TIPO_DEFICIENCIA': pl.Utf8
}

# Configurações de logging estruturado
logger.remove()  # Remove handler padrão
logger.add(
    "logs/conversor_caged_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="30 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}"
)
logger.add(
    lambda msg: print(msg, end=""),
    level="INFO",
    format="{time:HH:mm:ss} | {level} | {message}"
)


class ConversorParquetCaged:
    """
    Conversor especializado para dados CAGED
    Versão 2.0 - Arquitetura escalável com processamento paralelo
    
    Funcionalidades:
    - Processamento paralelo otimizado
    - Sistema de padronização flexível
    - Medição de performance
    - Validação robusta de dados
    - Tratamento avançado de encoding
    """
    
    def __init__(self, 
                 diretorio_origem: str = "files-unzip",
                 diretorio_destino: str = "parquet",
                 max_workers: Optional[int] = None,
                 chunk_size: Optional[int] = None,
                 habilitar_paralelismo: bool = True):
        """
        Inicializa o conversor com configurações otimizadas
        
        Args:
            diretorio_origem: Diretório com arquivos descompactados
            diretorio_destino: Diretório para arquivos Parquet
            max_workers: Número máximo de workers (None = automático)
            chunk_size: Tamanho do chunk (None = automático)
            habilitar_paralelismo: Se deve usar processamento paralelo
        """
        # Configuração de diretórios
        self.diretorio_origem = Path(diretorio_origem)
        self.diretorio_destino = Path(diretorio_destino)
        self.diretorio_destino.mkdir(parents=True, exist_ok=True)
        
        # Configuração de paralelismo
        self.habilitar_paralelismo = habilitar_paralelismo
        self.max_workers = max_workers or CONFIG_PARALELISMO['max_workers']
        self.chunk_size = chunk_size or CONFIG_PARALELISMO['chunk_size']
        
        # Contadores e estatísticas
        self.contador_entidades = 0
        self.estatisticas = {
            'arquivos_processados': 0,
            'arquivos_com_erro': 0,
            'total_registros': 0,
            'tempo_total': 0
        }
        
        # Medidor de tempo
        self.medidor = MedidorTempo("Conversor CAGED v2.0")
        
        # Cache para schemas e mapeamentos
        self._cache_schemas = {}
        self._cache_mapeamentos = {}
        
        logger.info(f"Conversor CAGED v2.0 inicializado")
        logger.info(f"Origem: {self.diretorio_origem}")
        logger.info(f"Destino: {self.diretorio_destino}")
        logger.info(f"Paralelismo: {self.habilitar_paralelismo} (workers: {self.max_workers})")
        logger.info(f"Chunk size: {self.chunk_size}")
        
    def detectar_encoding(self, arquivo: Path) -> str:
        """
        Detecta encoding do arquivo CAGED com múltiplos fallbacks
        
        Args:
            arquivo: Caminho do arquivo
            
        Returns:
            str: Encoding detectado
        """
        try:
            import chardet
            
            # Ler amostra maior para melhor detecção
            with open(arquivo, 'rb') as f:
                raw_data = f.read(CONFIG_ARQUIVO['max_linhas_deteccao'])
            
            result = chardet.detect(raw_data)
            encoding = result.get('encoding', 'latin1')
            confidence = result.get('confidence', 0)
            
            logger.debug(f"Encoding detectado: {encoding} (confiança: {confidence:.2f})")
            
            # Se confiança baixa, testar fallbacks
            if confidence < CONFIG_ARQUIVO['confianca_encoding_min']:
                logger.warning(f"Confiança baixa ({confidence:.2f}), testando fallbacks")
                
                for fallback in CONFIG_ARQUIVO['encodings_fallback']:
                    try:
                        with open(arquivo, 'r', encoding=fallback) as f:
                            # Tentar ler algumas linhas para validar
                            for _ in range(5):
                                linha = f.readline()
                                if not linha:
                                    break
                        
                        logger.info(f"Fallback bem-sucedido: {fallback}")
                        return fallback
                        
                    except (UnicodeDecodeError, UnicodeError) as e:
                        logger.debug(f"Fallback {fallback} falhou: {e}")
                        continue
                    except Exception as e:
                        logger.warning(f"Erro inesperado no fallback {fallback}: {e}")
                        continue
            
            return encoding or 'latin1'
            
        except ImportError:
            logger.warning("chardet não disponível, usando latin1")
            return 'latin1'
        except Exception as e:
            logger.error(f"Erro na detecção de encoding: {e}")
            return 'latin1'
    
    def processar_arquivo_mensal(self, 
                               arquivo: Path, 
                               ano: int, 
                               mes: int,
                               campos_selecionados: Optional[List[str]] = None) -> Tuple[pl.DataFrame, List, List, List]:
        """
        Processa um arquivo mensal CAGED com sistema otimizado
        
        Args:
            arquivo: Caminho do arquivo
            ano: Ano dos dados
            mes: Mês dos dados
            campos_selecionados: Campos específicos a processar
            
        Returns:
            Tuple[DataFrame, List[Movimentacao], List[SaldoMensal], List[Indicador]]: Dados processados
        """
        with self.medidor.etapa(f"Processamento {arquivo.name}"):
            if not arquivo.exists():
                logger.error(f"Arquivo não encontrado: {arquivo}")
                raise FileNotFoundError(f"Arquivo não encontrado: {arquivo}")
            
            logger.debug(f"Processando: {arquivo.name}")
            
            # Detectar encoding com sistema robusto
            encoding = self.detectar_encoding(arquivo)
            logger.debug(f"Encoding: {encoding}")
            
            try:
                # Detectar separador com análise aprimorada
                separador = self._detectar_separador(arquivo, encoding)
                logger.debug(f"Separador: '{separador}'")
                
                # Ler arquivo com configurações otimizadas
                df = pl.read_csv(
                    arquivo,
                    separator=separador,
                    encoding=encoding,
                    has_header=True,
                    ignore_errors=True,
                    truncate_ragged_lines=True,
                    low_memory=False,  # Melhor para arquivos grandes
                    rechunk=True  # Otimizar chunks
                )
                
                logger.info(f"Arquivo lido: {df.shape[0]:,} registros, {df.shape[1]} colunas")
                
                # Verificar se DataFrame não está vazio
                if df.shape[0] == 0:
                    logger.warning(f"Arquivo {arquivo.name} está vazio")
                    return df, [], [], []
                
                # Padronizar colunas com sistema flexível
                df = self._padronizar_colunas(df, arquivo)
                
                # Filtrar campos se especificado
                if campos_selecionados:
                    campos_disponiveis = [c for c in campos_selecionados if c in df.columns]
                    if campos_disponiveis:
                        df = df.select(campos_disponiveis)
                        logger.debug(f"Campos filtrados: {len(campos_disponiveis)} de {len(campos_selecionados)}")
                    else:
                        logger.warning("Nenhum campo selecionado encontrado no arquivo")
                
                # Aplicar tipos de dados com detecção automática
                df = self._aplicar_tipos_dados(df)
                
                # Adicionar colunas de controle
                df = df.with_columns([
                    pl.lit(f"{ano}-{mes:02d}").alias("ANO_MES"),
                    pl.lit(ano).alias("ANO"),
                    pl.lit(mes).alias("MES")
                ])
                
                # Validar integridade dos dados
                if self.validar_integridade_dados(df):
                    logger.info("✅ Dados validados com sucesso")
                else:
                    logger.warning("⚠️  Alertas encontrados na validação")
                
                # Converter para entidades com tratamento de erro
                try:
                    movimentacoes = self._dataframe_para_movimentacoes(df, ano, mes)
                    saldos_mensais = self._calcular_saldos_mensais(df, ano, mes)
                    indicadores = self._gerar_indicadores(df, ano, mes)
                    
                    logger.debug(f"Entidades criadas: {len(movimentacoes)} movimentações, "
                               f"{len(saldos_mensais)} saldos, {len(indicadores)} indicadores")
                    
                except Exception as e:
                    logger.error(f"Erro ao converter entidades: {e}")
                    # Retornar listas vazias em caso de erro
                    movimentacoes, saldos_mensais, indicadores = [], [], []
                
                logger.debug(f"Processamento de {arquivo.name} concluído")
                
                return df, movimentacoes, saldos_mensais, indicadores
                
            except Exception as e:
                logger.error(f"❌ Erro ao processar arquivo: {e}", exc_info=True)
                raise
    
    def _dataframe_para_movimentacoes(self, df: pl.DataFrame, ano: int, mes: int) -> List[Movimentacao]:
        """
        Converte DataFrame em lista de objetos Movimentacao
        
        Args:
            df: DataFrame com dados CAGED
            ano: Ano dos dados
            mes: Mês dos dados
            
        Returns:
            List[Movimentacao]: Lista de movimentações
        """
        movimentacoes = []
        competencia = f"{ano}-{mes:02d}"
        
        # Mapear campos do DataFrame para a entidade
        for row in df.iter_rows(named=True):
            self.contador_entidades += 1
            
            # Determinar tipo de movimentação baseado nos campos disponíveis
            tipo_movimentacao = self._determinar_tipo_movimentacao(row)
            
            # Extrair CPF se disponível
            cpf = row.get('CPF', '') or row.get('CPF_TRABALHADOR', '') or ''
            
            # Extrair CNPJ se disponível
            cnpj = row.get('CNPJ', '') or row.get('CNPJ_CEI', '') or ''
            
            movimentacao = Movimentacao(
                id=self.contador_entidades,
                cnpj=cnpj,
                cpf=cpf,
                competencia=competencia,
                tipo_movimentacao=tipo_movimentacao,
                data_movimentacao=self._extrair_data_movimentacao(row)
            )
            
            movimentacoes.append(movimentacao)
        
        print(f"📊 Criadas {len(movimentacoes)} movimentações")
        return movimentacoes
    
    def _determinar_tipo_movimentacao(self, row: Dict) -> str:
        """
        Determina o tipo de movimentação baseado nos dados da linha
        
        Args:
            row: Linha de dados do DataFrame
            
        Returns:
            str: Tipo de movimentação ('admissao' ou 'desligamento')
        """
        # Verificar campos específicos de tipo de movimentação
        tipo_campo = row.get('TIPO_MOVIMENTACAO', '')
        
        if tipo_campo in ['1', 'ADMISSAO', 'ADMISSÃO']:
            return 'admissao'
        elif tipo_campo in ['2', 'DESLIGAMENTO', 'DEMISSAO', 'DEMISSÃO']:
            return 'desligamento'
        
        # Verificar se há dados de admissão ou desligamento
        admitidos = row.get('ADMITIDOS', 0) or 0
        desligados = row.get('DESLIGADOS', 0) or 0
        
        if admitidos > 0:
            return 'admissao'
        elif desligados > 0:
            return 'desligamento'
        
        # Padrão baseado no nome do arquivo ou outros indicadores
        return 'admissao'  # Padrão
    
    def _extrair_data_movimentacao(self, row: Dict) -> Optional[date]:
        """
        Extrai data de movimentação dos dados
        
        Args:
            row: Linha de dados do DataFrame
            
        Returns:
            Optional[date]: Data de movimentação ou None
        """
        # Tentar extrair data de diferentes campos
        campos_data = ['DATA_ADMISSAO', 'DATA_DESLIGAMENTO', 'DATA_MOVIMENTACAO', 'DATA']
        
        for campo in campos_data:
            if campo in row and row[campo]:
                try:
                    # Tentar diferentes formatos de data
                    data_str = str(row[campo])
                    for formato in ['%Y%m%d', '%d/%m/%Y', '%Y-%m-%d', '%d%m%Y']:
                        try:
                            return datetime.strptime(data_str, formato).date()
                        except:
                            continue
                except:
                    continue
        
        return None
    
    def _calcular_saldos_mensais(self, df: pl.DataFrame, ano: int, mes: int) -> List[SaldoMensal]:
        """
        Calcula saldos mensais por CNPJ
        
        Args:
            df: DataFrame com dados CAGED
            ano: Ano dos dados
            mes: Mês dos dados
            
        Returns:
            List[SaldoMensal]: Lista de saldos mensais
        """
        saldos = []
        competencia = f"{ano}-{mes:02d}"
        
        # Agrupar por CNPJ se disponível
        if 'CNPJ' in df.columns:
            # Agrupar por CNPJ e calcular totais
            df_agrupado = df.group_by('CNPJ').agg([
                pl.sum('ADMITIDOS').alias('total_admissoes'),
                pl.sum('DESLIGADOS').alias('total_desligamentos'),
                pl.sum('SALDO').alias('saldo_total')
            ])
            
            for row in df_agrupado.iter_rows(named=True):
                self.contador_entidades += 1
                
                saldo = SaldoMensal(
                    id=self.contador_entidades,
                    cnpj=row['CNPJ'],
                    competencia=competencia,
                    saldo=row['saldo_total'] or 0,
                    admissoes=row['total_admissoes'] or 0,
                    desligamentos=row['total_desligamentos'] or 0,
                    exc_admissoes=0,  # Seria calculado se houvesse dados de exclusão
                    exc_desligamentos=0
                )
                
                saldos.append(saldo)
        else:
            # Calcular totais gerais se não houver CNPJ
            total_admissoes = df.select(pl.sum('ADMITIDOS')).item() or 0
            total_desligamentos = df.select(pl.sum('DESLIGADOS')).item() or 0
            saldo_total = df.select(pl.sum('SALDO')).item() or 0
            
            self.contador_entidades += 1
            saldo = SaldoMensal(
                id=self.contador_entidades,
                cnpj="",  # CNPJ geral
                competencia=competencia,
                saldo=saldo_total,
                admissoes=total_admissoes,
                desligamentos=total_desligamentos,
                exc_admissoes=0,
                exc_desligamentos=0
            )
            
            saldos.append(saldo)
        
        print(f"📊 Calculados {len(saldos)} saldos mensais")
        return saldos
    
    def _gerar_indicadores(self, df: pl.DataFrame, ano: int, mes: int) -> List[Indicador]:
        """
        Gera indicadores baseados nos dados processados
        
        Args:
            df: DataFrame com dados CAGED
            ano: Ano dos dados
            mes: Mês dos dados
            
        Returns:
            List[Indicador]: Lista de indicadores
        """
        indicadores = []
        competencia = f"{ano}-{mes:02d}"
        
        # Calcular indicadores básicos
        total_registros = df.shape[0]
        total_admissoes = df.select(pl.sum('ADMITIDOS')).item() or 0
        total_desligamentos = df.select(pl.sum('DESLIGADOS')).item() or 0
        saldo_total = df.select(pl.sum('SALDO')).item() or 0
        
        # Taxa de rotatividade (se houver dados suficientes)
        if total_admissoes + total_desligamentos > 0:
            taxa_rotatividade = ((total_admissoes + total_desligamentos) / 2) / max(saldo_total, 1) * 100
        else:
            taxa_rotatividade = 0
        
        # Criar indicadores
        indicadores_dados = [
            ("total_registros", total_registros),
            ("total_admissoes", total_admissoes),
            ("total_desligamentos", total_desligamentos),
            ("saldo_total", saldo_total),
            ("taxa_rotatividade", taxa_rotatividade)
        ]
        
        for nome, valor in indicadores_dados:
            self.contador_entidades += 1
            indicador = Indicador(
                id=self.contador_entidades,
                cnpj="",  # Indicador geral
                competencia=competencia,
                nome_indicador=nome,
                valor=float(valor)
            )
            indicadores.append(indicador)
        
        print(f"📊 Gerados {len(indicadores)} indicadores")
        return indicadores
    
    def _detectar_separador(self, arquivo: Path, encoding: str) -> str:
        """
        Detecta o separador do arquivo CSV com análise aprimorada
        
        Args:
            arquivo: Caminho do arquivo
            encoding: Encoding do arquivo
            
        Returns:
            str: Separador detectado
        """
        try:
            with open(arquivo, 'r', encoding=encoding) as f:
                # Ler múltiplas linhas para melhor detecção
                linhas = [f.readline().strip() for _ in range(min(5, 1000))]
                linhas = [linha for linha in linhas if linha]  # Remover vazias
            
            if not linhas:
                logger.warning("Arquivo vazio ou sem linhas válidas")
                return ';'
            
            # Analisar cada separador
            resultados = {}
            
            for separador in CONFIG_ARQUIVO['separadores']:
                contagens = [linha.count(separador) for linha in linhas]
                
                if contagens:
                    # Verificar consistência (todas as linhas devem ter contagem similar)
                    contagem_media = sum(contagens) / len(contagens)
                    variacao = max(contagens) - min(contagens)
                    
                    # Penalizar alta variação (inconsistência)
                    score = contagem_media - (variacao * 0.5)
                    resultados[separador] = max(0, score)
                else:
                    resultados[separador] = 0
            
            # Escolher melhor separador
            if resultados:
                separador = max(resultados, key=resultados.get)
                if resultados[separador] > 0:
                    logger.debug(f"Separador detectado: '{separador}' (score: {resultados[separador]:.2f})")
                    return separador
            
            # Fallback para padrão brasileiro
            logger.warning("Nenhum separador consistente encontrado, usando ';'")
            return ';'
            
        except Exception as e:
            logger.error(f"Erro na detecção de separador: {e}")
            return ';'
    
    def _padronizar_colunas(self, df: pl.DataFrame, arquivo_origem: Optional[Path] = None) -> pl.DataFrame:
        """
        Padroniza nomes das colunas usando sistema flexível
        
        Args:
            df: DataFrame original
            arquivo_origem: Arquivo de origem (para cache)
            
        Returns:
            DataFrame com colunas padronizadas
        """
        # Verificar cache se arquivo fornecido
        cache_key = str(arquivo_origem) if arquivo_origem else None
        if cache_key and cache_key in self._cache_mapeamentos:
            mapeamento = self._cache_mapeamentos[cache_key]
            logger.debug(f"Usando mapeamento em cache para {arquivo_origem.name}")
        else:
            # Criar mapeamento flexível
            mapeamento = criar_mapeamento_caged_flexivel(df.columns)
            
            # Validar colunas
            colunas_invalidas = obter_colunas_invalidas(df.columns)
            if colunas_invalidas:
                logger.warning(f"Colunas inválidas encontradas: {len(colunas_invalidas)}")
            
            # Validar campos essenciais
            validacao = validar_campos_caged(df.columns)
            if not validacao['valido']:
                logger.warning(f"Campos essenciais ausentes: {validacao['campos_ausentes']}")
            
            # Salvar no cache
            if cache_key:
                self._cache_mapeamentos[cache_key] = mapeamento
        
        # Aplicar padronização
        try:
            df_padronizado = df.rename(mapeamento)
            logger.debug(f"Colunas padronizadas: {len(mapeamento)} mapeamentos aplicados")
            return df_padronizado
        except Exception as e:
            logger.error(f"Erro na padronização de colunas: {e}")
            # Fallback para padronização básica
            mapeamento_basico = padronizar_colunas_dataframe(df.columns)
            return df.rename(mapeamento_basico)
    
    def _aplicar_tipos_dados(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Aplica tipos de dados corretos às colunas com detecção automática
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame com tipos corretos
        """
        try:
            conversoes = []
            schema_aplicado = {}
            
            for coluna in df.columns:
                # Verificar se coluna está no schema base
                if coluna in SCHEMA_CAGED_BASE:
                    tipo_esperado = SCHEMA_CAGED_BASE[coluna]
                    schema_aplicado[coluna] = tipo_esperado
                    
                    if tipo_esperado == pl.Int64:
                        # Conversão robusta para inteiros
                        conversoes.append(
                            pl.col(coluna)
                            .cast(pl.Utf8, strict=False)  # Primeiro para string
                            .str.replace_all(r'[^\d-]', '')  # Limpar caracteres
                            .str.replace(r'^-+$', '0')  # Tratar apenas sinais
                            .str.replace(r'^$', '0')  # Tratar vazios
                            .cast(pl.Int64, strict=False)
                            .fill_null(0)
                            .alias(coluna)
                        )
                    elif tipo_esperado == pl.Utf8:
                        # Conversão para string
                        conversoes.append(
                            pl.col(coluna)
                            .cast(pl.Utf8, strict=False)
                            .fill_null("")
                            .str.strip()  # Remover espaços
                            .alias(coluna)
                        )
                else:
                    # Detecção automática de tipo para colunas não mapeadas
                    if coluna in CAMPOS_NUMERICOS or any(num_field in coluna.upper() 
                                                        for num_field in ['ADMITIDO', 'DESLIGADO', 'SALDO', 'QUANTIDADE']):
                        # Tratar como numérico
                        conversoes.append(
                            pl.col(coluna)
                            .cast(pl.Utf8, strict=False)
                            .str.replace_all(r'[^\d-]', '')
                            .str.replace(r'^$', '0')
                            .cast(pl.Int64, strict=False)
                            .fill_null(0)
                            .alias(coluna)
                        )
                        schema_aplicado[coluna] = pl.Int64
                    else:
                        # Tratar como string
                        conversoes.append(
                            pl.col(coluna)
                            .cast(pl.Utf8, strict=False)
                            .fill_null("")
                            .str.strip()
                            .alias(coluna)
                        )
                        schema_aplicado[coluna] = pl.Utf8
            
            if conversoes:
                df = df.with_columns(conversoes)
                logger.debug(f"Tipos aplicados: {len(schema_aplicado)} colunas convertidas")
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao aplicar tipos de dados: {e}")
            return df
    
    def validar_integridade_dados(self, df: pl.DataFrame) -> bool:
        """
        Valida integridade dos dados CAGED
        Verifica se: Admitidos - Desligados = Saldo
        
        Args:
            df: DataFrame a validar
            
        Returns:
            bool: True se dados são consistentes
        """
        try:
            if not all(col in df.columns for col in ['ADMITIDOS', 'DESLIGADOS', 'SALDO']):
                print("⚠️  Colunas de movimentação não encontradas para validação")
                return False
            
            # Calcular saldo esperado
            df_validacao = df.with_columns([
                (pl.col('ADMITIDOS') - pl.col('DESLIGADOS')).alias('SALDO_CALCULADO')
            ])
            
            # Verificar inconsistências
            inconsistencias = df_validacao.filter(
                pl.col('SALDO') != pl.col('SALDO_CALCULADO')
            )
            
            total_registros = df.shape[0]
            registros_inconsistentes = inconsistencias.shape[0]
            
            if registros_inconsistentes > 0:
                percentual = (registros_inconsistentes / total_registros) * 100
                print(f"⚠️  {registros_inconsistentes}/{total_registros} registros inconsistentes ({percentual:.2f}%)")
                
                # Mostrar alguns exemplos
                if registros_inconsistentes <= 5:
                    print("📋 Exemplos de inconsistências:")
                    print(inconsistencias.select(['ADMITIDOS', 'DESLIGADOS', 'SALDO', 'SALDO_CALCULADO']))
                
                return percentual < 5  # Aceitar até 5% de inconsistências
            else:
                print("✅ Todos os registros são consistentes (Admitidos - Desligados = Saldo)")
                return True
                
        except Exception as e:
            print(f"❌ Erro na validação: {e}")
            return False
    
    def converter_mensal(self, 
                        ano: int, 
                        mes: int,
                        campos_selecionados: Optional[List[str]] = None,
                        usar_paralelismo: Optional[bool] = None) -> Tuple[bool, List, List, List]:
        """
        Converte dados de um mês específico para Parquet com processamento otimizado
        
        Args:
            ano: Ano dos dados
            mes: Mês dos dados
            campos_selecionados: Campos específicos a processar
            usar_paralelismo: Forçar uso/não uso de paralelismo
            
        Returns:
            Tuple[bool, List[Movimentacao], List[SaldoMensal], List[Indicador]]: Resultado da conversão
        """
        with self.medidor.etapa(f"Conversão Mensal {ano}/{mes:02d}"):
            # Configurar paralelismo
            usar_paralelo = usar_paralelismo if usar_paralelismo is not None else self.habilitar_paralelismo
            
            # Encontrar arquivos do período
            padrao = f"*{ano}*{mes:02d}*"
            arquivos_origem = list(self.diretorio_origem.rglob(f"{padrao}.txt")) + \
                             list(self.diretorio_origem.rglob(f"{padrao}.csv"))
            
            if not arquivos_origem:
                logger.error(f"Nenhum arquivo encontrado para {ano}/{mes:02d}")
                return False, [], [], []
            
            logger.info(f"Convertendo {len(arquivos_origem)} arquivos de {ano}/{mes:02d}")
            logger.info(f"Paralelismo: {'Habilitado' if usar_paralelo else 'Desabilitado'}")
            
            # Processar arquivos
            if usar_paralelo and len(arquivos_origem) > 1:
                resultados = self._processar_arquivos_paralelo(arquivos_origem, ano, mes, campos_selecionados)
            else:
                resultados = self._processar_arquivos_sequencial(arquivos_origem, ano, mes, campos_selecionados)
            
            # Consolidar resultados
            dataframes, movimentacoes_totais, saldos_totais, indicadores_totais = self._consolidar_resultados(resultados)
            
            if not dataframes:
                logger.error("Nenhum arquivo foi processado com sucesso")
                return False, [], [], []
            
            # Salvar resultado consolidado
            sucesso = self._salvar_resultado_mensal(dataframes, ano, mes)
            
            # Atualizar estatísticas
            self.estatisticas['arquivos_processados'] += len([r for r in resultados if r['sucesso']])
            self.estatisticas['arquivos_com_erro'] += len([r for r in resultados if not r['sucesso']])
            self.estatisticas['total_registros'] += sum(df.shape[0] for df in dataframes)
            
            return sucesso, movimentacoes_totais, saldos_totais, indicadores_totais
    
    def consolidar_anual(self, ano: int) -> Tuple[bool, List, List, List]:
        """
        Consolida todos os meses de um ano em arquivo único
        
        Args:
            ano: Ano a consolidar
            
        Returns:
            Tuple[bool, List[Movimentacao], List[SaldoMensal], List[Indicador]]: Resultado da consolidação
        """
        # Encontrar arquivos mensais do ano
        padrao = f"CAGED_{ano}_*.parquet"
        arquivos_mensais = list(self.diretorio_destino.glob(padrao))
        
        if not arquivos_mensais:
            print(f"❌ Nenhum arquivo mensal encontrado para {ano}")
            return False, [], [], []
        
        print(f"🎯 Consolidando {len(arquivos_mensais)} arquivos mensais de {ano}")
        
        # Ler e consolidar todos os arquivos mensais
        dataframes = []
        movimentacoes_totais = []
        saldos_totais = []
        indicadores_totais = []
        
        for arquivo in sorted(arquivos_mensais):
            print(f"📊 Carregando: {arquivo.name}")
            df = pl.read_parquet(arquivo)
            dataframes.append(df)
            
            # TODO: Carregar entidades dos arquivos se persistidas
        
        # Consolidar
        df_anual = pl.concat(dataframes, how="vertical")
        
        # Salvar consolidado anual
        nome_arquivo = f"CAGED_{ano}.parquet"
        caminho_saida = self.diretorio_destino / nome_arquivo
        
        df_anual.write_parquet(caminho_saida)
        
        total_registros = df_anual.shape[0]
        tamanho_arquivo = caminho_saida.stat().st_size / (1024 * 1024)  # MB
        
        print(f"✅ Consolidação anual concluída!")
        print(f"   📄 Arquivo: {nome_arquivo}")
        print(f"   📊 Registros: {total_registros:,}")
        print(f"   💾 Tamanho: {tamanho_arquivo:.2f} MB")
        
        return True, movimentacoes_totais, saldos_totais, indicadores_totais
    
    def descompactar_mensal(self, ano: int, mes: int) -> bool:
        """
        Descompacta arquivos mensais de dados CAGED
        
        Args:
            ano: Ano dos dados
            mes: Mês dos dados
            
        Returns:
            bool: True se descompactação bem-sucedida
        """
        diretorio_mes = self.diretorio_origem / f"{ano}" / f"{mes:02d}"
        arquivos_7z = list(diretorio_mes.glob("*.7z"))
        
        print(f"🔄 Descompactando arquivos em {diretorio_mes}")
        
        for arquivo_7z in arquivos_7z:
            diretorio_destino = self.diretorio_destino / f"{ano}" / f"{mes:02d}"
            diretorio_destino.mkdir(parents=True, exist_ok=True)
            self.descompactar_arquivo(arquivo_7z, diretorio_destino)

        return True


    def _processar_arquivos_paralelo(self, arquivos: List[Path], ano: int, mes: int, 
                                    campos_selecionados: Optional[List[str]]) -> List[Dict[str, Any]]:
        """
        Processa arquivos em paralelo usando ThreadPoolExecutor
        """
        resultados = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submeter tarefas
            futures = {
                executor.submit(self._processar_arquivo_seguro, arquivo, ano, mes, campos_selecionados): arquivo
                for arquivo in arquivos
            }
            
            # Coletar resultados
            for future in as_completed(futures, timeout=CONFIG_PARALELISMO['timeout_arquivo']):
                arquivo = futures[future]
                try:
                    resultado = future.result()
                    resultado['arquivo'] = arquivo
                    resultados.append(resultado)
                    
                    if resultado['sucesso']:
                        logger.info(f"✅ {arquivo.name} processado com sucesso")
                    else:
                        logger.error(f"❌ Erro em {arquivo.name}: {resultado['erro']}")
                        
                except Exception as e:
                    logger.error(f"❌ Erro inesperado em {arquivo.name}: {e}")
                    resultados.append({
                        'sucesso': False,
                        'erro': str(e),
                        'arquivo': arquivo,
                        'df': None,
                        'movimentacoes': [],
                        'saldos': [],
                        'indicadores': []
                    })
        
        return resultados
    
    def _processar_arquivos_sequencial(self, arquivos: List[Path], ano: int, mes: int,
                                      campos_selecionados: Optional[List[str]]) -> List[Dict[str, Any]]:
        """
        Processa arquivos sequencialmente
        """
        resultados = []
        
        for arquivo in arquivos:
            resultado = self._processar_arquivo_seguro(arquivo, ano, mes, campos_selecionados)
            resultado['arquivo'] = arquivo
            resultados.append(resultado)
            
            if resultado['sucesso']:
                logger.info(f"✅ {arquivo.name} processado com sucesso")
            else:
                logger.error(f"❌ Erro em {arquivo.name}: {resultado['erro']}")
        
        return resultados
    
    def _processar_arquivo_seguro(self, arquivo: Path, ano: int, mes: int,
                                 campos_selecionados: Optional[List[str]]) -> Dict[str, Any]:
        """
        Processa um arquivo com tratamento seguro de erros
        """
        try:
            df, movimentacoes, saldos, indicadores = self.processar_arquivo_mensal(
                arquivo, ano, mes, campos_selecionados
            )
            
            return {
                'sucesso': True,
                'erro': None,
                'df': df,
                'movimentacoes': movimentacoes,
                'saldos': saldos,
                'indicadores': indicadores
            }
            
        except Exception as e:
            return {
                'sucesso': False,
                'erro': str(e),
                'df': None,
                'movimentacoes': [],
                'saldos': [],
                'indicadores': []
            }
    
    def _consolidar_resultados(self, resultados: List[Dict[str, Any]]) -> Tuple[List, List, List, List]:
        """
        Consolida resultados de múltiplos arquivos
        """
        dataframes = []
        movimentacoes_totais = []
        saldos_totais = []
        indicadores_totais = []
        
        for resultado in resultados:
            if resultado['sucesso'] and resultado['df'] is not None:
                dataframes.append(resultado['df'])
                movimentacoes_totais.extend(resultado['movimentacoes'])
                saldos_totais.extend(resultado['saldos'])
                indicadores_totais.extend(resultado['indicadores'])
        
        return dataframes, movimentacoes_totais, saldos_totais, indicadores_totais
    
    def _salvar_resultado_mensal(self, dataframes: List[pl.DataFrame], ano: int, mes: int) -> bool:
        """
        Salva resultado consolidado mensal
        """
        try:
            with self.medidor.etapa("Consolidação e Salvamento"):
                # Consolidar DataFrames
                df_consolidado = pl.concat(dataframes, how="vertical")
                
                # Salvar como Parquet
                nome_arquivo = f"CAGED_{ano}_{mes:02d}.parquet"
                caminho_saida = self.diretorio_destino / nome_arquivo
                
                df_consolidado.write_parquet(caminho_saida)
                
                # Estatísticas
                total_registros = df_consolidado.shape[0]
                tamanho_arquivo = caminho_saida.stat().st_size / (1024 * 1024)  # MB
                
                logger.info(f"✅ Conversão concluída!")
                logger.info(f"   📄 Arquivo: {nome_arquivo}")
                logger.info(f"   📊 Registros: {total_registros:,}")
                logger.info(f"   💾 Tamanho: {tamanho_arquivo:.2f} MB")
                
                return True
                
        except Exception as e:
            logger.error(f"Erro ao salvar resultado mensal: {e}")
            return False
    
    def obter_estatisticas(self) -> Dict[str, Any]:
        """
        Retorna estatísticas de processamento
        """
        return {
            **self.estatisticas,
            'tempo_medicao': self.medidor.obter_resumo()
        }
    
    def imprimir_relatorio(self):
        """
        Imprime relatório detalhado de processamento
        """
        print("\n" + "="*60)
        print("RELATÓRIO DE PROCESSAMENTO - CONVERSOR CAGED v2.0")
        print("="*60)
        
        stats = self.estatisticas
        print(f"Arquivos processados: {stats['arquivos_processados']}")
        print(f"Arquivos com erro: {stats['arquivos_com_erro']}")
        print(f"Total de registros: {stats['total_registros']:,}")
        
        if stats['arquivos_processados'] > 0:
            taxa_sucesso = (stats['arquivos_processados'] / 
                           (stats['arquivos_processados'] + stats['arquivos_com_erro'])) * 100
            print(f"Taxa de sucesso: {taxa_sucesso:.1f}%")
        
        # Imprimir resumo de tempo
        self.medidor.imprimir_resumo()


# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def testar_conversor():
    """
    Teste rápido do conversor v2.0
    """
    logger.info("🧪 Testando conversor CAGED v2.0...")
    
    conversor = ConversorParquetCaged()
    
    # Verificar diretórios
    logger.info(f"📁 Diretório origem: {conversor.diretorio_origem}")
    logger.info(f"📁 Diretório destino: {conversor.diretorio_destino}")
    
    # Listar arquivos disponíveis
    arquivos_txt = list(conversor.diretorio_origem.rglob("*.txt"))
    arquivos_csv = list(conversor.diretorio_origem.rglob("*.csv"))
    arquivos_parquet = list(conversor.diretorio_destino.glob("*.parquet"))
    
    logger.info(f"📋 Arquivos .txt encontrados: {len(arquivos_txt)}")
    logger.info(f"📋 Arquivos .csv encontrados: {len(arquivos_csv)}")
    logger.info(f"📦 Arquivos .parquet existentes: {len(arquivos_parquet)}")
    
    # Teste de configuração
    logger.info(f"⚙️  Configuração:")
    logger.info(f"   Workers: {conversor.max_workers}")
    logger.info(f"   Chunk size: {conversor.chunk_size}")
    logger.info(f"   Paralelismo: {conversor.habilitar_paralelismo}")
    
    return (len(arquivos_txt) + len(arquivos_csv)) > 0


if __name__ == "__main__":
    # Configurar logging para execução direta
    import sys
    logger.add(sys.stdout, level="INFO")
    
    # Executar teste
    testar_conversor()