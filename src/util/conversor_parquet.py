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
from src.util.filtro_caged import (
    FiltroCNAE,
    FiltroPeriodo,
    FiltroMovimentacao,
    FiltroGeografico,
    GerenciadorFiltros
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
    'fator_memoria_chunk': 0.8,  # Fator de segurança para memória
    'bytes_por_linha_estimado': 150,  # Estimativa de bytes por linha
    'memoria_disponivel_mb': 4096,  # Memória disponível estimada
    # Configurações avançadas de balanceamento
    'fator_cpu_workers': 1.5,  # Multiplicador baseado em CPU
    'workers_min': 2,  # Mínimo de workers
    'workers_max': 16,  # Máximo de workers
    'balanceamento_carga': True,  # Habilitar balanceamento dinâmico
    'prioridade_arquivos': 'tamanho',  # 'tamanho', 'nome', 'aleatorio'
    'timeout_worker': 600,  # Timeout por worker (segundos)
    'retry_max': 3,  # Máximo de tentativas por arquivo
}

# Configurações de encoding e separadores
CONFIG_ARQUIVO = {
    'encodings_fallback': ['utf-8', 'latin1', 'cp1252', 'iso-8859-1'],
    'separadores': [';', ',', '\t', '|'],
    'confianca_encoding_min': 0.7,
    'max_linhas_deteccao': 10000,
}

# ============================================================================
# CONFIGURAÇÕES DE DADOS CAGED
# ============================================================================

# Campos essenciais do CAGED (para validação)
CAMPOS_ESSENCIAIS_CAGED = {
    'competencia': ['COMPETENCIA', 'COMP', 'ANO_MES', 'PERIODO'],
    'regiao': ['REGIAO', 'REG', 'REGION'],
    'uf': ['UF', 'ESTADO', 'SIGLA_UF'],
    'municipio': ['MUNICIPIO', 'MUNIC', 'CIDADE'],
    'cnae_classe': ['CNAE_2_0_CLASSE', 'CNAE_CLASSE', 'CLASSE_CNAE'],
    'cnae_subclasse': ['CNAE_2_0_SUBCLASSE', 'CNAE_SUBCLASSE', 'SUBCLASSE_CNAE'],
    'admitidos': ['ADMITIDOS', 'ADMISSOES', 'ADMIT'],
    'desligados': ['DESLIGADOS', 'DESLIG', 'DEMISSOES'],
    'saldo': ['SALDO', 'SALDO_MOVIMENTACAO', 'VARIACAO']
}

# Campos opcionais do CAGED (podem estar presentes)
CAMPOS_OPCIONAIS_CAGED = {
    'sexo': ['SEXO', 'GENERO'],
    'faixa_etaria': ['FAIXA_ETARIA', 'IDADE', 'FAIXA_IDADE'],
    'escolaridade': ['ESCOLARIDADE', 'GRAU_INSTRUCAO', 'EDUCACAO'],
    'cbo': ['CBO_2002', 'CBO', 'OCUPACAO'],
    'tipo_movimentacao': ['TIPO_MOVIMENTACAO', 'TIPO_MOV', 'MOVIMENTACAO'],
    'tipo_deficiencia': ['TIPO_DEFICIENCIA', 'DEFICIENCIA', 'PCD']
}

# Campos númericos para validação e conversão
CAMPOS_NUMERICOS = ['ADMITIDOS', 'DESLIGADOS', 'SALDO']
PADROES_NUMERICOS = ['ADMITIDO', 'DESLIGADO', 'SALDO', 'QUANTIDADE', 'TOTAL', 'VALOR']

# Schema base flexível - expandido dinamicamente
SCHEMA_TIPOS_BASE = {
    # Campos de texto
    'texto': pl.Utf8,
    # Campos numéricos inteiros
    'numerico_int': pl.Int64,
    # Campos numéricos decimais
    'numerico_float': pl.Float64,
    # Campos de data/tempo
    'data': pl.Utf8,  # Mantemos como string para flexibilidade
    'temporal': pl.Utf8,
    # Campos categóricos
    'categorico': pl.Categorical,
    # Campos booleanos
    'booleano': pl.Boolean,
    # Campos de identificação (grandes números)
    'identificacao': pl.Utf8
}

# Mapeamento avançado de tipos por padrão de nome
MAPEAMENTO_TIPOS_AUTOMATICO = {
    # Campos numéricos inteiros
    'numerico_int': [
        'ADMITIDOS', 'DESLIGADOS', 'SALDO', 'QUANTIDADE', 'TOTAL', 'COUNT',
        'ADMIT', 'DESLIG', 'QTD', 'NUM', 'ANO', 'MES', 'IDADE'
    ],
    # Campos numéricos decimais
    'numerico_float': [
        'VALOR', 'SALARIO', 'REMUNERACAO', 'PERCENTUAL', 'TAXA', 'INDICE',
        'MEDIA', 'VARIACAO', 'PROPORCAO'
    ],
    # Campos de identificação (texto)
    'identificacao': [
        'CNPJ', 'CPF', 'CODIGO', 'ID', 'CHAVE', 'REGISTRO', 'DOCUMENTO',
        'MATRICULA', 'PIS', 'PASEP'
    ],
    # Campos de data/tempo
    'temporal': [
        'DATA', 'COMPETENCIA', 'PERIODO', 'TIMESTAMP', 'INICIO', 'FIM',
        'ADMISSAO', 'DEMISSAO', 'NASCIMENTO'
    ],
    # Campos geográficos
    'geografico': [
        'UF', 'ESTADO', 'MUNICIPIO', 'CIDADE', 'REGIAO', 'CEP', 'ENDERECO',
        'BAIRRO', 'LOGRADOURO', 'PAIS'
    ],
    # Campos de classificação categórica
    'categorico': [
        'CNAE', 'CBO', 'TIPO', 'CATEGORIA', 'CLASSE', 'SUBCLASSE', 'GRUPO',
        'SEXO', 'GENERO', 'ESCOLARIDADE', 'RACA', 'COR', 'DEFICIENCIA',
        'FAIXA_ETARIA', 'FAIXA_SALARIAL', 'PORTE_EMPRESA'
    ],
    # Campos booleanos
    'booleano': [
        'ATIVO', 'INATIVO', 'VALIDO', 'INVALIDO', 'PRINCIPAL', 'SECUNDARIO',
        'PUBLICO', 'PRIVADO', 'FORMAL', 'INFORMAL'
    ]
}

# Configurações de validação de integridade
VALIDACOES_INTEGRIDADE = {
    # Validações matemáticas
    'saldo_movimentacao': {
        'formula': 'ADMITIDOS - DESLIGADOS = SALDO',
        'tolerancia_percentual': 5.0,
        'obrigatorio': True
    },
    # Validações de consistência temporal
    'consistencia_temporal': {
        'campos_data': ['COMPETENCIA', 'DATA_ADMISSAO', 'DATA_DEMISSAO'],
        'formato_esperado': 'YYYY-MM',
        'obrigatorio': False
    },
    # Validações de domínio
    'dominios_validos': {
        'UF': ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'],
        'SEXO': ['M', 'F', 'MASCULINO', 'FEMININO', '1', '2'],
        'obrigatorio': False
    }
}

# ============================================================================
# CONFIGURAÇÕES DE FILTROS
# ============================================================================

# Configurações de filtros CAGED
CONFIG_FILTROS = {
    'habilitar_filtros': False,  # Habilitar sistema de filtros
    'aplicar_filtros_paralelo': True,  # Aplicar filtros em paralelo
    'log_estatisticas_filtros': True,  # Log detalhado dos filtros
    'validar_filtros_inicializacao': True,  # Validar filtros na inicialização
    
    # Configurações específicas de filtros
    'filtro_cnae': {
        'arquivo_padrao': 'db/cnae_classe_emprego_verde.csv',
        'situacao_padrao': 1,
        'coluna_cnae_padrao': 'CNAESUBCLASSE'
    },
    'filtro_periodo': {
        'coluna_competencia_padrao': 'COMPETENCIA',
        'formato_competencia': 'YYYYMM'
    },
    'filtro_movimentacao': {
        'coluna_tipo_padrao': 'TIPOMOVIMENTACAO',
        'coluna_saldo_padrao': 'SALDOMOVIMENTACAO'
    },
    'filtro_geografico': {
        'coluna_uf_padrao': 'UF',
        'coluna_regiao_padrao': 'REGIAO',
        'coluna_municipio_padrao': 'MUNICIPIO',
        'coluna_cep_padrao': 'CEP'
    }
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
                 habilitar_paralelismo: bool = True,
                 habilitar_filtros: bool = False,
                 configuracao_filtros: Optional[Dict[str, Any]] = None):
        """
        Inicializa o conversor com configurações otimizadas
        
        Args:
            diretorio_origem: Diretório com arquivos descompactados
            diretorio_destino: Diretório para arquivos Parquet
            max_workers: Número máximo de workers (None = automático)
            chunk_size: Tamanho do chunk (None = automático)
            habilitar_paralelismo: Se deve usar processamento paralelo
            habilitar_filtros: Se deve habilitar sistema de filtros
            configuracao_filtros: Configurações específicas dos filtros
        """
        # Configuração de diretórios
        self.diretorio_origem = Path(diretorio_origem)
        self.diretorio_destino = Path(diretorio_destino)
        self.diretorio_destino.mkdir(parents=True, exist_ok=True)
        
        # Configuração de paralelismo
        self.habilitar_paralelismo = habilitar_paralelismo
        self.max_workers = max_workers or CONFIG_PARALELISMO['max_workers']
        self.chunk_size = chunk_size or CONFIG_PARALELISMO['chunk_size']
        
        # Configuração de filtros
        self.habilitar_filtros = habilitar_filtros
        self.configuracao_filtros = configuracao_filtros or {}
        self.gerenciador_filtros = None
        
        # Inicializar sistema de filtros se habilitado
        if self.habilitar_filtros:
            self._inicializar_sistema_filtros()
        
        # Contadores e estatísticas
        self.contador_entidades = 0
        self.estatisticas = {
            'arquivos_processados': 0,
            'arquivos_com_erro': 0,
            'total_registros': 0,
            'tempo_total': 0,
            'registros_filtrados': 0,
            'filtros_aplicados': 0
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
        logger.info(f"Filtros: {self.habilitar_filtros}")
    
    def _calcular_chunk_size_dinamico(self, tamanho_arquivo_bytes: int, 
                                     numero_colunas: int = 50) -> int:
        """
        Calcula o tamanho do chunk dinamicamente baseado no tamanho do arquivo,
        número de colunas e memória disponível
        
        Args:
            tamanho_arquivo_bytes: Tamanho do arquivo em bytes
            numero_colunas: Número de colunas no arquivo
            
        Returns:
            Tamanho otimizado do chunk em linhas
        """
        # Obter configurações
        config = CONFIG_PARALELISMO
        
        # Calcular estimativa de linhas no arquivo
        bytes_por_linha = config['bytes_por_linha_estimado'] * (numero_colunas / 50)
        linhas_estimadas = max(1, tamanho_arquivo_bytes // bytes_por_linha)
        
        # Calcular chunk size baseado na memória disponível
        memoria_por_worker_mb = config['memoria_limite_mb']
        memoria_chunk_mb = memoria_por_worker_mb * config['fator_memoria_chunk']
        
        # Converter para bytes e calcular linhas por chunk
        memoria_chunk_bytes = memoria_chunk_mb * 1024 * 1024
        chunk_size_memoria = max(1, memoria_chunk_bytes // bytes_por_linha)
        
        # Calcular chunk size baseado no tamanho do arquivo
        tamanho_arquivo_gb = tamanho_arquivo_bytes / (1024**3)
        
        if tamanho_arquivo_gb < 0.1:  # Arquivos muito pequenos
            chunk_size_arquivo = min(linhas_estimadas, config['chunk_size_min'])
        elif tamanho_arquivo_gb < 1:  # Arquivos pequenos
            chunk_size_arquivo = config['chunk_size'] // 2
        elif tamanho_arquivo_gb < 5:  # Arquivos médios
            chunk_size_arquivo = config['chunk_size']
        else:  # Arquivos grandes
            chunk_size_arquivo = config['chunk_size_max']
        
        # Usar o menor entre os dois cálculos para garantir que caiba na memória
        chunk_size_otimo = min(chunk_size_memoria, chunk_size_arquivo)
        
        # Aplicar limites mínimo e máximo
        chunk_size_final = max(
            config['chunk_size_min'],
            min(chunk_size_otimo, config['chunk_size_max'])
        )
        
        logger.debug(f"Chunk size calculado: {chunk_size_final:,} linhas ")
        logger.debug(f"  - Arquivo: {tamanho_arquivo_gb:.2f} GB, {linhas_estimadas:,} linhas estimadas")
        logger.debug(f"  - Memória: {memoria_chunk_mb:.1f} MB por chunk")
        logger.debug(f"  - Colunas: {numero_colunas}, {bytes_por_linha:.0f} bytes/linha")
        
        return int(chunk_size_final)
    
    def _detectar_workers_otimizado(self, num_arquivos: int, tamanho_total_mb: float) -> int:
        """
        Detecta o número otimizado de workers baseado no sistema e carga de trabalho
        
        Args:
            num_arquivos: Número de arquivos a processar
            tamanho_total_mb: Tamanho total dos arquivos em MB
            
        Returns:
            Número otimizado de workers
        """
        config = CONFIG_PARALELISMO
        
        # Obter informações do sistema
        cpu_count = os.cpu_count() or 1
        memoria_disponivel = self._obter_memoria_disponivel()
        
        # Calcular workers baseado na CPU
        workers_cpu = int(cpu_count * config['fator_cpu_workers'])
        
        # Calcular workers baseado na memória
        memoria_por_worker = config['memoria_limite_mb']
        workers_memoria = max(1, memoria_disponivel // memoria_por_worker)
        
        # Calcular workers baseado no número de arquivos
        workers_arquivos = min(num_arquivos, config['workers_max'])
        
        # Calcular workers baseado no tamanho dos dados
        if tamanho_total_mb < 100:  # Dados pequenos
            workers_dados = min(2, workers_cpu)
        elif tamanho_total_mb < 1000:  # Dados médios
            workers_dados = min(4, workers_cpu)
        else:  # Dados grandes
            workers_dados = workers_cpu
        
        # Usar o menor valor para evitar sobrecarga
        workers_otimo = min(
            workers_cpu,
            workers_memoria,
            workers_arquivos,
            workers_dados,
            config['workers_max']
        )
        
        # Aplicar limites mínimo e máximo
        workers_final = max(config['workers_min'], workers_otimo)
        
        logger.info(f"Workers otimizados: {workers_final}")
        logger.debug(f"  - CPU: {cpu_count} cores → {workers_cpu} workers")
        logger.debug(f"  - Memória: {memoria_disponivel:,} MB → {workers_memoria} workers")
        logger.debug(f"  - Arquivos: {num_arquivos} → {workers_arquivos} workers")
        logger.debug(f"  - Dados: {tamanho_total_mb:.1f} MB → {workers_dados} workers")
        
        return workers_final
    
    def _ordenar_arquivos_por_prioridade(self, arquivos: List[Path]) -> List[Path]:
        """
        Ordena arquivos por prioridade para balanceamento de carga
        
        Args:
            arquivos: Lista de arquivos
            
        Returns:
            Lista de arquivos ordenada por prioridade
        """
        config = CONFIG_PARALELISMO
        prioridade = config['prioridade_arquivos']
        
        if prioridade == 'tamanho':
            # Ordenar por tamanho (maiores primeiro para melhor balanceamento)
            return sorted(arquivos, key=lambda x: x.stat().st_size, reverse=True)
        elif prioridade == 'nome':
            # Ordenar por nome
            return sorted(arquivos, key=lambda x: x.name)
        elif prioridade == 'aleatorio':
            # Ordem aleatória
            import random
            arquivos_copia = arquivos.copy()
            random.shuffle(arquivos_copia)
            return arquivos_copia
        else:
            # Manter ordem original
            return arquivos
    
    def _calcular_balanceamento_carga(self, arquivos: List[Path], num_workers: int) -> List[List[Path]]:
        """
        Calcula balanceamento de carga otimizado para distribuir arquivos entre workers
        
        Args:
            arquivos: Lista de arquivos
            num_workers: Número de workers
            
        Returns:
            Lista de listas, cada uma contendo arquivos para um worker
        """
        if not CONFIG_PARALELISMO['balanceamento_carga']:
            # Distribuição simples
            chunk_size = len(arquivos) // num_workers + 1
            return [arquivos[i:i + chunk_size] for i in range(0, len(arquivos), chunk_size)]
        
        # Ordenar arquivos por prioridade
        arquivos_ordenados = self._ordenar_arquivos_por_prioridade(arquivos)
        
        # Obter tamanhos dos arquivos
        arquivos_com_tamanho = [(arquivo, arquivo.stat().st_size) for arquivo in arquivos_ordenados]
        
        # Inicializar workers com carga zero
        workers_carga = [[] for _ in range(num_workers)]
        workers_tamanho = [0] * num_workers
        
        # Distribuir arquivos usando algoritmo de menor carga
        for arquivo, tamanho in arquivos_com_tamanho:
            # Encontrar worker com menor carga
            worker_menor_carga = min(range(num_workers), key=lambda i: workers_tamanho[i])
            
            # Atribuir arquivo ao worker
            workers_carga[worker_menor_carga].append(arquivo)
            workers_tamanho[worker_menor_carga] += tamanho
        
        # Log do balanceamento
        for i, (arquivos_worker, tamanho_worker) in enumerate(zip(workers_carga, workers_tamanho)):
            tamanho_mb = tamanho_worker / (1024 * 1024)
            logger.debug(f"Worker {i+1}: {len(arquivos_worker)} arquivos, {tamanho_mb:.1f} MB")
        
        return workers_carga
    
    def _obter_memoria_disponivel(self) -> int:
        """
        Obtém a quantidade de memória disponível no sistema
        
        Returns:
            Memória disponível em MB
        """
        try:
            import psutil
            memoria = psutil.virtual_memory()
            memoria_disponivel_mb = memoria.available // (1024 * 1024)
            
            # Usar no máximo 80% da memória disponível
            memoria_utilizavel = int(memoria_disponivel_mb * 0.8)
            
            logger.debug(f"Memória disponível: {memoria_disponivel_mb:,} MB ")
            logger.debug(f"Memória utilizável: {memoria_utilizavel:,} MB")
            
            return memoria_utilizavel
            
        except ImportError:
            logger.warning("psutil não disponível, usando valor padrão")
            return CONFIG_PARALELISMO['memoria_disponivel_mb']
        except Exception as e:
            logger.warning(f"Erro ao obter memória disponível: {e}")
            return CONFIG_PARALELISMO['memoria_disponivel_mb']
    
    def _ajustar_chunk_size_para_memoria(self, chunk_size_inicial: int, 
                                        numero_colunas: int) -> int:
        """
        Ajusta o chunk size baseado na memória disponível do sistema
        
        Args:
            chunk_size_inicial: Chunk size inicial calculado
            numero_colunas: Número de colunas no arquivo
            
        Returns:
            Chunk size ajustado para a memória disponível
        """
        memoria_disponivel_mb = self._obter_memoria_disponivel()
        
        # Calcular memória necessária por linha (estimativa)
        bytes_por_linha = CONFIG_PARALELISMO['bytes_por_linha_estimado'] * numero_colunas
        memoria_por_chunk_mb = (chunk_size_inicial * bytes_por_linha) / (1024 * 1024)
        
        # Se o chunk usar mais que 50% da memória disponível, reduzir
        limite_memoria_chunk = memoria_disponivel_mb * 0.5
        
        if memoria_por_chunk_mb > limite_memoria_chunk:
            fator_reducao = limite_memoria_chunk / memoria_por_chunk_mb
            chunk_size_ajustado = int(chunk_size_inicial * fator_reducao)
            
            # Garantir que não fique abaixo do mínimo
            chunk_size_ajustado = max(
                chunk_size_ajustado, 
                CONFIG_PARALELISMO['chunk_size_min']
            )
            
            logger.info(f"Chunk size ajustado para memória: {chunk_size_inicial:,} → {chunk_size_ajustado:,}")
            return chunk_size_ajustado
        
        return chunk_size_inicial
    
    def _criar_mapeamento_dinamico(self, colunas: List[str]) -> Dict[str, str]:
        """
        Cria mapeamento dinâmico de colunas baseado nos padrões CAGED
        
        Args:
            colunas: Lista de nomes de colunas originais
            
        Returns:
            Dicionário de mapeamento {coluna_original: coluna_padronizada}
        """
        from src.util.utilitarios import padronizar_nome_coluna
        
        mapeamento = {}
        colunas_padronizadas = set()
        
        for coluna_original in colunas:
            # Padronizar nome básico
            coluna_limpa = padronizar_nome_coluna(coluna_original)
            
            # Verificar se corresponde a algum campo essencial
            campo_mapeado = None
            for campo_essencial, variacoes in CAMPOS_ESSENCIAIS_CAGED.items():
                if self._colunas_similares(coluna_limpa, variacoes):
                    campo_mapeado = campo_essencial.upper()
                    break
            
            # Se não encontrou nos essenciais, verificar opcionais
            if not campo_mapeado:
                for campo_opcional, variacoes in CAMPOS_OPCIONAIS_CAGED.items():
                    if self._colunas_similares(coluna_limpa, variacoes):
                        campo_mapeado = campo_opcional.upper()
                        break
            
            # Se ainda não encontrou, usar padronização básica
            if not campo_mapeado:
                campo_mapeado = coluna_limpa
            
            # Evitar duplicatas
            if campo_mapeado in colunas_padronizadas:
                contador = 1
                while f"{campo_mapeado}_{contador}" in colunas_padronizadas:
                    contador += 1
                campo_mapeado = f"{campo_mapeado}_{contador}"
            
            mapeamento[coluna_original] = campo_mapeado
            colunas_padronizadas.add(campo_mapeado)
        
        return mapeamento
    
    def _colunas_similares(self, coluna: str, variacoes: List[str]) -> bool:
        """
        Verifica se uma coluna é similar a alguma das variações
        
        Args:
            coluna: Nome da coluna
            variacoes: Lista de variações possíveis
            
        Returns:
            True se a coluna é similar a alguma variação
        """
        coluna_norm = coluna.upper().replace('_', '').replace(' ', '')
        
        for variacao in variacoes:
            variacao_norm = variacao.upper().replace('_', '').replace(' ', '')
            
            # Verificação exata
            if coluna_norm == variacao_norm:
                return True
            
            # Verificação de contenção
            if coluna_norm in variacao_norm or variacao_norm in coluna_norm:
                return True
        
        return False
    
    def _validar_campos_essenciais(self, colunas: List[str]) -> Dict[str, Any]:
        """
        Valida se os campos essenciais estão presentes nas colunas
        
        Args:
            colunas: Lista de nomes de colunas
            
        Returns:
            Dicionário com resultado da validação
        """
        campos_encontrados = []
        campos_ausentes = []
        
        for campo_essencial, variacoes in CAMPOS_ESSENCIAIS_CAGED.items():
            encontrado = False
            for coluna in colunas:
                if self._colunas_similares(coluna, variacoes):
                    campos_encontrados.append(campo_essencial)
                    encontrado = True
                    break
            
            if not encontrado:
                campos_ausentes.append(campo_essencial)
        
        return {
            'valido': len(campos_ausentes) == 0,
            'campos_encontrados': campos_encontrados,
            'campos_ausentes': campos_ausentes,
            'total_essenciais': len(CAMPOS_ESSENCIAIS_CAGED),
            'percentual_encontrado': len(campos_encontrados) / len(CAMPOS_ESSENCIAIS_CAGED) * 100
        }
    
    def _inicializar_sistema_filtros(self) -> None:
        """
        Inicializa o sistema de filtros baseado na configuração fornecida
        """
        try:
            self.gerenciador_filtros = GerenciadorFiltros()
            config_filtros = {**CONFIG_FILTROS, **self.configuracao_filtros}
            
            logger.info("Inicializando sistema de filtros...")
            
            # Configurar filtro CNAE se especificado
            if 'filtro_cnae' in self.configuracao_filtros:
                config_cnae = self.configuracao_filtros['filtro_cnae']
                if 'arquivo' in config_cnae:
                    nome_filtro = config_cnae.get('nome', 'CNAE Personalizado')
                    situacao = config_cnae.get('situacao', 1)
                    
                    self.gerenciador_filtros.adicionar_filtro_cnae(
                        caminho_arquivo=config_cnae['arquivo'],
                        nome_filtro=nome_filtro,
                        situacao=situacao
                    )
                    logger.info(f"Filtro CNAE configurado: {nome_filtro}")
            
            # Configurar filtro de período se especificado
            if 'filtro_periodo' in self.configuracao_filtros:
                config_periodo = self.configuracao_filtros['filtro_periodo']
                self.gerenciador_filtros.adicionar_filtro_periodo(**config_periodo)
                logger.info("Filtro de período configurado")
            
            # Configurar filtro de movimentação se especificado
            if 'filtro_movimentacao' in self.configuracao_filtros:
                config_movimentacao = self.configuracao_filtros['filtro_movimentacao']
                self.gerenciador_filtros.adicionar_filtro_movimentacao(**config_movimentacao)
                logger.info("Filtro de movimentação configurado")
            
            # Configurar filtro geográfico se especificado
            if 'filtro_geografico' in self.configuracao_filtros:
                config_geografico = self.configuracao_filtros['filtro_geografico']
                self.gerenciador_filtros.adicionar_filtro_geografico(**config_geografico)
                logger.info("Filtro geográfico configurado")
            
            # Validar filtros se configurado
            if config_filtros.get('validar_filtros_inicializacao', True):
                resumo = self.gerenciador_filtros.obter_resumo_filtros()
                logger.info(f"Sistema de filtros inicializado: {resumo['total_filtros']} filtros ativos")
                
                if config_filtros.get('log_estatisticas_filtros', True):
                    for tipo, quantidade in resumo['tipos_filtros'].items():
                        logger.info(f"  - {tipo}: {quantidade} filtro(s)")
            
        except Exception as e:
            logger.error(f"Erro ao inicializar sistema de filtros: {e}")
            self.gerenciador_filtros = None
            self.habilitar_filtros = False
    
    def _aplicar_filtros_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Aplica os filtros configurados ao DataFrame
        
        Args:
            df: DataFrame a ser filtrado
            
        Returns:
            DataFrame filtrado
        """
        if not self.habilitar_filtros or not self.gerenciador_filtros:
            return df
        
        try:
            total_antes = df.height
            
            # Aplicar filtros
            df_filtrado = self.gerenciador_filtros.aplicar_filtros(df)
            
            total_depois = df_filtrado.height
            registros_removidos = total_antes - total_depois
            
            # Atualizar estatísticas
            self.estatisticas['registros_filtrados'] += registros_removidos
            self.estatisticas['filtros_aplicados'] += 1
            
            if registros_removidos > 0:
                percentual_removido = (registros_removidos / total_antes) * 100
                logger.info(f"Filtros aplicados: {registros_removidos:,} registros removidos ({percentual_removido:.2f}%)")
            
            return df_filtrado
            
        except Exception as e:
            logger.error(f"Erro ao aplicar filtros: {e}")
            return df
    
    def configurar_filtro_cnae(self, arquivo_cnae: str, nome_filtro: str = "CNAE", situacao: int = 1) -> 'ConversorParquetCaged':
        """
        Configura um filtro CNAE (method chaining)
        
        Args:
            arquivo_cnae: Caminho para o arquivo CSV com classificação CNAE
            nome_filtro: Nome descritivo do filtro
            situacao: Valor da coluna SITUACAO para filtrar
            
        Returns:
            Self para permitir method chaining
        """
        if not self.habilitar_filtros:
            self.habilitar_filtros = True
            self._inicializar_sistema_filtros()
        
        if self.gerenciador_filtros:
            self.gerenciador_filtros.adicionar_filtro_cnae(arquivo_cnae, nome_filtro, situacao)
        
        return self
    
    def configurar_filtro_periodo(self, **kwargs) -> 'ConversorParquetCaged':
        """
        Configura um filtro de período (method chaining)
        
        Args:
            **kwargs: Argumentos para FiltroPeriodo
            
        Returns:
            Self para permitir method chaining
        """
        if not self.habilitar_filtros:
            self.habilitar_filtros = True
            self._inicializar_sistema_filtros()
        
        if self.gerenciador_filtros:
            self.gerenciador_filtros.adicionar_filtro_periodo(**kwargs)
        
        return self
    
    def configurar_filtro_movimentacao(self, **kwargs) -> 'ConversorParquetCaged':
        """
        Configura um filtro de movimentação (method chaining)
        
        Args:
            **kwargs: Argumentos para FiltroMovimentacao
            
        Returns:
            Self para permitir method chaining
        """
        if not self.habilitar_filtros:
            self.habilitar_filtros = True
            self._inicializar_sistema_filtros()
        
        if self.gerenciador_filtros:
            self.gerenciador_filtros.adicionar_filtro_movimentacao(**kwargs)
        
        return self
    
    def configurar_filtro_geografico(self, **kwargs) -> 'ConversorParquetCaged':
        """
        Configura um filtro geográfico (method chaining)
        
        Args:
            **kwargs: Argumentos para FiltroGeografico
            
        Returns:
            Self para permitir method chaining
        """
        if not self.habilitar_filtros:
            self.habilitar_filtros = True
            self._inicializar_sistema_filtros()
        
        if self.gerenciador_filtros:
            self.gerenciador_filtros.adicionar_filtro_geografico(**kwargs)
        
        return self
    
    def obter_resumo_filtros(self) -> Dict[str, Any]:
        """
        Obtém um resumo dos filtros configurados
        
        Returns:
            Dicionário com resumo dos filtros
        """
        if not self.habilitar_filtros or not self.gerenciador_filtros:
            return {'filtros_habilitados': False, 'total_filtros': 0}
        
        resumo = self.gerenciador_filtros.obter_resumo_filtros()
        resumo['filtros_habilitados'] = True
        resumo['registros_filtrados'] = self.estatisticas.get('registros_filtrados', 0)
        resumo['filtros_aplicados'] = self.estatisticas.get('filtros_aplicados', 0)
        
        return resumo
    
    def _configurar_filtros_dinamicos(self,
                                     filtros_cnae: Optional[List[str]] = None,
                                     filtros_periodo: Optional[Dict[str, Any]] = None,
                                     filtros_movimentacao: Optional[List[str]] = None,
                                     filtros_geograficos: Optional[Dict[str, Any]] = None) -> None:
        """
        Configura filtros dinamicamente em tempo de execução
        
        Args:
            filtros_cnae: Lista de códigos CNAE para filtrar
            filtros_periodo: Configurações de filtro por período
            filtros_movimentacao: Tipos de movimentação para filtrar
            filtros_geograficos: Configurações de filtro geográfico
        """
        # Habilitar sistema de filtros
        self.habilitar_filtros = True
        
        # Inicializar gerenciador se não existir
        if not self.gerenciador_filtros:
            self._inicializar_sistema_filtros()
        
        # Configurar filtro CNAE
        if filtros_cnae:
            self.configurar_filtro_cnae(codigos_cnae=filtros_cnae)
        
        # Configurar filtro de período
        if filtros_periodo:
            anos = filtros_periodo.get('anos')
            meses = filtros_periodo.get('meses')
            data_inicio = filtros_periodo.get('data_inicio')
            data_fim = filtros_periodo.get('data_fim')
            
            self.configurar_filtro_periodo(
                anos=anos,
                meses=meses,
                data_inicio=data_inicio,
                data_fim=data_fim
            )
        
        # Configurar filtro de movimentação
        if filtros_movimentacao:
            self.configurar_filtro_movimentacao(tipos_movimentacao=filtros_movimentacao)
        
        # Configurar filtro geográfico
        if filtros_geograficos:
            ufs = filtros_geograficos.get('ufs')
            regioes = filtros_geograficos.get('regioes')
            municipios = filtros_geograficos.get('municipios')
            ceps = filtros_geograficos.get('ceps')
            
            self.configurar_filtro_geografico(
                ufs=ufs,
                regioes=regioes,
                municipios=municipios,
                ceps=ceps
            )
        
        logger.info(f"Sistema de filtros configurado dinamicamente com {len(self.gerenciador_filtros.filtros)} filtros ativos")
    
    def _detectar_tipo_coluna(self, nome_coluna: str, amostra_dados: Optional[pl.Series] = None) -> pl.DataType:
        """
        Detecta automaticamente o tipo de dados de uma coluna baseado no nome e amostra
        
        Args:
            nome_coluna: Nome da coluna
            amostra_dados: Amostra dos dados da coluna para análise
            
        Returns:
            Tipo de dados Polars apropriado
        """
        nome_upper = nome_coluna.upper()
        
        # Verificar padrões numéricos específicos primeiro
        for padrao in PADROES_NUMERICOS:
            if padrao in nome_upper:
                tipo_sugerido = pl.Int64
                if amostra_dados is not None:
                    return self._validar_tipo_com_amostra(tipo_sugerido, amostra_dados, nome_coluna)
                return tipo_sugerido
        
        # Verificar mapeamento automático expandido de tipos
        for categoria, padroes in MAPEAMENTO_TIPOS_AUTOMATICO.items():
            for padrao in padroes:
                if padrao in nome_upper:
                    tipo_base = SCHEMA_TIPOS_BASE.get(categoria, pl.Utf8)
                    
                    # Se temos amostra de dados, validar o tipo
                    if amostra_dados is not None:
                        return self._validar_tipo_com_amostra(tipo_base, amostra_dados, nome_coluna)
                    
                    return tipo_base
        
        # Padrões específicos por regex com análise de amostra
        if re.search(r'(CNPJ|CPF|CEP|CODIGO|PIS|PASEP)', nome_upper):
            return pl.Utf8  # IDs sempre como string
        elif re.search(r'(VALOR|SALARIO|REMUNERACAO|PERCENTUAL|TAXA)', nome_upper):
            tipo_sugerido = pl.Float64
        elif re.search(r'(DATA|PERIODO|COMPETENCIA)', nome_upper):
            tipo_sugerido = pl.Utf8
        elif re.search(r'(QUANTIDADE|TOTAL|COUNT|SALDO)', nome_upper):
            tipo_sugerido = pl.Int64
        else:
            tipo_sugerido = pl.Utf8
            
        # Validar com amostra se disponível
        if amostra_dados is not None:
            return self._validar_tipo_com_amostra(tipo_sugerido, amostra_dados, nome_coluna)
            
        return tipo_sugerido
    
    def _validar_tipo_com_amostra(self, tipo_sugerido: pl.DataType, 
                                  amostra: pl.Series, nome_coluna: str) -> pl.DataType:
        """
        Valida o tipo sugerido com uma amostra dos dados
        
        Args:
            tipo_sugerido: Tipo inicialmente sugerido
            amostra: Amostra dos dados
            nome_coluna: Nome da coluna
            
        Returns:
            Tipo validado
        """
        try:
            # Remover valores nulos para análise
            amostra_limpa = amostra.drop_nulls()
            
            if len(amostra_limpa) == 0:
                return pl.Utf8  # Se só tem nulos, usar string
            
            # Tentar conversão para o tipo sugerido
            try:
                amostra_limpa.cast(tipo_sugerido, strict=True)
                return tipo_sugerido
            except:
                # Se falhou, tentar tipos alternativos
                if tipo_sugerido == pl.Int64:
                    # Tentar Float64 se Int64 falhou
                    try:
                        amostra_limpa.cast(pl.Float64, strict=True)
                        logger.debug(f"Coluna {nome_coluna}: Int64 → Float64 (valores decimais detectados)")
                        return pl.Float64
                    except:
                        pass
                elif tipo_sugerido == pl.Float64:
                    # Tentar Int64 se Float64 falhou mas parece numérico
                    try:
                        amostra_limpa.cast(pl.Int64, strict=True)
                        logger.debug(f"Coluna {nome_coluna}: Float64 → Int64 (valores inteiros detectados)")
                        return pl.Int64
                    except:
                        pass
                
                # Se nada funcionou, usar string
                logger.debug(f"Coluna {nome_coluna}: {tipo_sugerido} → Utf8 (conversão falhou)")
                return pl.Utf8
                
        except Exception as e:
            logger.warning(f"Erro na validação de tipo para {nome_coluna}: {e}")
            return pl.Utf8
        
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
                               campos_selecionados: Optional[List[str]] = None,
                               usar_chunks: bool = True) -> Tuple[pl.DataFrame, List, List, List]:
        """
        Processa um arquivo mensal CAGED com sistema otimizado e processamento em chunks
        
        Args:
            arquivo: Caminho do arquivo
            ano: Ano dos dados
            mes: Mês dos dados
            campos_selecionados: Campos específicos a processar
            usar_chunks: Se deve usar processamento em chunks para arquivos grandes
            
        Returns:
            Tuple[DataFrame, List[Movimentacao], List[SaldoMensal], List[Indicador]]: Dados processados
        """
        with self.medidor.etapa(f"Processamento {arquivo.name}"):
            if not arquivo.exists():
                logger.error(f"Arquivo não encontrado: {arquivo}")
                raise FileNotFoundError(f"Arquivo não encontrado: {arquivo}")
            
            logger.debug(f"Processando: {arquivo.name}")
            
            # Obter informações do arquivo
            tamanho_arquivo = arquivo.stat().st_size
            tamanho_mb = tamanho_arquivo / (1024 * 1024)
            
            logger.info(f"Arquivo: {arquivo.name} ({tamanho_mb:.1f} MB)")
            
            # Detectar encoding com sistema robusto
            encoding = self.detectar_encoding(arquivo)
            logger.debug(f"Encoding: {encoding}")
            
            # Detectar separador com análise aprimorada
            separador = self._detectar_separador(arquivo, encoding)
            logger.debug(f"Separador: '{separador}'")
            
            # Decidir se usar processamento em chunks baseado no tamanho do arquivo
            limite_chunk_mb = 100  # Arquivos maiores que 100MB usam chunks
            
            if usar_chunks and tamanho_mb > limite_chunk_mb:
                logger.info(f"Arquivo grande ({tamanho_mb:.1f} MB), usando processamento em chunks")
                return self._processar_arquivo_em_chunks(arquivo, ano, mes, encoding, separador, campos_selecionados)
            else:
                logger.info(f"Arquivo pequeno/médio ({tamanho_mb:.1f} MB), processamento direto")
                return self._processar_arquivo_direto(arquivo, ano, mes, encoding, separador, campos_selecionados)
    
    def _processar_arquivo_direto(self, arquivo: Path, ano: int, mes: int, 
                                 encoding: str, separador: str,
                                 campos_selecionados: Optional[List[str]] = None) -> Tuple[pl.DataFrame, List, List, List]:
        """
        Processa arquivo diretamente na memória (para arquivos pequenos/médios)
        """
        try:
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
            
            # Aplicar filtros se habilitados
            if self.habilitar_filtros and self.gerenciador_filtros:
                registros_antes = df.shape[0]
                df = self._aplicar_filtros_dataframe(df)
                registros_depois = df.shape[0]
                registros_filtrados = registros_antes - registros_depois
                
                self.estatisticas['registros_filtrados'] += registros_filtrados
                
                if registros_filtrados > 0:
                    logger.info(f"Filtros aplicados: {registros_filtrados:,} registros removidos ({registros_antes:,} → {registros_depois:,})")
            
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
    
    def _processar_arquivo_em_chunks(self, arquivo: Path, ano: int, mes: int,
                                   encoding: str, separador: str,
                                   campos_selecionados: Optional[List[str]] = None) -> Tuple[pl.DataFrame, List, List, List]:
        """
        Processa arquivo grande em chunks para otimizar uso de memória
        """
        try:
            # Primeiro, ler apenas o cabeçalho para determinar as colunas
            df_header = pl.read_csv(
                arquivo,
                separator=separador,
                encoding=encoding,
                has_header=True,
                n_rows=1
            )
            
            numero_colunas = df_header.shape[1]
            tamanho_arquivo = arquivo.stat().st_size
            
            # Calcular chunk size dinâmico
            chunk_size = self._calcular_chunk_size_dinamico(tamanho_arquivo, numero_colunas)
            chunk_size = self._ajustar_chunk_size_para_memoria(chunk_size, numero_colunas)
            
            logger.info(f"Processamento em chunks: {chunk_size:,} linhas por chunk")
            
            # Listas para acumular resultados
            dataframes_chunks = []
            movimentacoes_total = []
            saldos_mensais_total = []
            indicadores_total = []
            
            chunk_num = 0
            linhas_processadas = 0
            
            # Ler arquivo em chunks usando scan_csv para eficiência
            try:
                # Usar lazy frame para leitura eficiente
                lazy_df = pl.scan_csv(
                    arquivo,
                    separator=separador,
                    encoding=encoding,
                    has_header=True,
                    ignore_errors=True,
                    truncate_ragged_lines=True
                )
                
                # Obter total de linhas para barra de progresso
                total_linhas = lazy_df.select(pl.count()).collect().item()
                num_chunks = (total_linhas + chunk_size - 1) // chunk_size
                
                logger.info(f"Total de linhas: {total_linhas:,}, chunks: {num_chunks}")
                
                # Processar cada chunk
                from tqdm import tqdm
                
                with tqdm(total=num_chunks, desc="Processando chunks", unit="chunk") as pbar:
                    
                    for chunk_start in range(0, total_linhas, chunk_size):
                        chunk_end = min(chunk_start + chunk_size, total_linhas)
                        chunk_num += 1
                        
                        # Ler chunk específico
                        df_chunk = lazy_df.slice(chunk_start, chunk_size).collect()
                        
                        if df_chunk.shape[0] == 0:
                            continue
                        
                        # Processar chunk
                        df_processado = self._processar_chunk_individual(
                            df_chunk, arquivo, ano, mes, campos_selecionados
                        )
                        
                        if df_processado.shape[0] > 0:
                            dataframes_chunks.append(df_processado)
                            
                            # Converter chunk para entidades
                            try:
                                mov_chunk = self._dataframe_para_movimentacoes(df_processado, ano, mes)
                                saldos_chunk = self._calcular_saldos_mensais(df_processado, ano, mes)
                                ind_chunk = self._gerar_indicadores(df_processado, ano, mes)
                                
                                movimentacoes_total.extend(mov_chunk)
                                saldos_mensais_total.extend(saldos_chunk)
                                indicadores_total.extend(ind_chunk)
                                
                            except Exception as e:
                                logger.warning(f"Erro ao processar entidades do chunk {chunk_num}: {e}")
                        
                        linhas_processadas += df_chunk.shape[0]
                        pbar.update(1)
                        pbar.set_postfix({
                            'linhas': f"{linhas_processadas:,}",
                            'chunk': f"{chunk_num}/{num_chunks}"
                        })
                
                # Consolidar todos os chunks em um DataFrame final
                if dataframes_chunks:
                    logger.info(f"Consolidando {len(dataframes_chunks)} chunks...")
                    df_final = pl.concat(dataframes_chunks, how="vertical")
                    
                    # Validar integridade dos dados consolidados
                    if self.validar_integridade_dados(df_final):
                        logger.info("✅ Dados consolidados validados com sucesso")
                    else:
                        logger.warning("⚠️  Alertas encontrados na validação dos dados consolidados")
                    
                    logger.info(f"Processamento em chunks concluído: {df_final.shape[0]:,} registros")
                    
                    return df_final, movimentacoes_total, saldos_mensais_total, indicadores_total
                else:
                    logger.warning("Nenhum chunk válido processado")
                    return pl.DataFrame(), [], [], []
                    
            except Exception as e:
                logger.error(f"Erro no processamento em chunks: {e}")
                # Fallback para processamento direto
                logger.info("Tentando processamento direto como fallback...")
                return self._processar_arquivo_direto(arquivo, ano, mes, encoding, separador, campos_selecionados)
                
        except Exception as e:
            logger.error(f"❌ Erro crítico no processamento em chunks: {e}", exc_info=True)
            raise
    
    def _processar_chunk_individual(self, df_chunk: pl.DataFrame, arquivo: Path, 
                                  ano: int, mes: int,
                                  campos_selecionados: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Processa um chunk individual aplicando todas as transformações necessárias
        """
        try:
            # Padronizar colunas
            df_processado = self._padronizar_colunas(df_chunk, arquivo)
            
            # Filtrar campos se especificado
            if campos_selecionados:
                campos_disponiveis = [c for c in campos_selecionados if c in df_processado.columns]
                if campos_disponiveis:
                    df_processado = df_processado.select(campos_disponiveis)
            
            # Aplicar tipos de dados
            df_processado = self._aplicar_tipos_dados(df_processado)
            
            # Adicionar colunas de controle
            df_processado = df_processado.with_columns([
                pl.lit(f"{ano}-{mes:02d}").alias("ANO_MES"),
                pl.lit(ano).alias("ANO"),
                pl.lit(mes).alias("MES")
            ])
            
            # Aplicar filtros se habilitados (para chunks)
            if self.habilitar_filtros and self.gerenciador_filtros:
                registros_antes = df_processado.shape[0]
                df_processado = self._aplicar_filtros_dataframe(df_processado)
                registros_depois = df_processado.shape[0]
                registros_filtrados = registros_antes - registros_depois
                
                self.estatisticas['registros_filtrados'] += registros_filtrados
            
            return df_processado
            
        except Exception as e:
            logger.error(f"Erro ao processar chunk individual: {e}")
            return pl.DataFrame()
    
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
        Padroniza nomes das colunas usando sistema dinâmico flexível
        
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
            # Criar mapeamento dinâmico baseado nos padrões CAGED
            mapeamento = self._criar_mapeamento_dinamico(df.columns)
            
            # Validar colunas usando sistema flexível
            colunas_invalidas = obter_colunas_invalidas(df.columns)
            if colunas_invalidas:
                logger.warning(f"Colunas inválidas encontradas: {len(colunas_invalidas)}")
            
            # Validar campos essenciais usando novo sistema
            validacao = self._validar_campos_essenciais(df.columns)
            if not validacao['valido']:
                logger.warning(f"Campos essenciais ausentes: {validacao['campos_ausentes']}")
                logger.info(f"Campos encontrados: {validacao['campos_encontrados']}")
            
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
        Aplica tipos de dados corretos às colunas com detecção automática avançada
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame com tipos corretos
        """
        try:
            conversoes = []
            schema_aplicado = {}
            
            for coluna in df.columns:
                # Obter amostra dos dados para análise
                amostra = df.select(pl.col(coluna)).to_series().head(1000)
                
                # Detectar tipo automaticamente com amostra
                tipo_detectado = self._detectar_tipo_coluna(coluna, amostra)
                schema_aplicado[coluna] = tipo_detectado
                
                if tipo_detectado == pl.Int64:
                    # Conversão robusta para inteiros
                    conversoes.append(
                        pl.col(coluna)
                        .cast(pl.Utf8, strict=False)  # Primeiro para string
                        .str.replace_all(r'[^\d-]', '')  # Limpar caracteres
                        .str.replace(r'^-+$', '0')  # Tratar apenas sinais
                        .str.replace(r'^$', '0')  # Tratar vazios
                        .str.replace(r'^0+$', '0')  # Normalizar zeros
                        .cast(pl.Int64, strict=False)
                        .fill_null(0)
                        .alias(coluna)
                    )
                elif tipo_detectado == pl.Float64:
                    # Conversão para decimais
                    conversoes.append(
                        pl.col(coluna)
                        .cast(pl.Utf8, strict=False)
                        .str.replace_all(r'[^\d.,-]', '')  # Manter pontos e vírgulas
                        .str.replace(',', '.')  # Padronizar decimal
                        .str.replace(r'^$', '0')
                        .cast(pl.Float64, strict=False)
                        .fill_null(0.0)
                        .alias(coluna)
                    )
                elif tipo_detectado == pl.Categorical:
                    # Conversão para categórico
                    conversoes.append(
                        pl.col(coluna)
                        .cast(pl.Utf8, strict=False)
                        .fill_null("")
                        .str.strip()
                        .cast(pl.Categorical)
                        .alias(coluna)
                    )
                elif tipo_detectado == pl.Boolean:
                    # Conversão para booleano
                    conversoes.append(
                        pl.col(coluna)
                        .cast(pl.Utf8, strict=False)
                        .str.to_lowercase()
                        .str.strip()
                        .map_elements(
                            lambda x: True if x in ['true', '1', 'sim', 's', 'yes', 'y'] 
                                     else False if x in ['false', '0', 'nao', 'n', 'no'] 
                                     else None,
                            return_dtype=pl.Boolean
                        )
                        .fill_null(False)
                        .alias(coluna)
                    )
                else:  # pl.Utf8 (padrão)
                    # Conversão para string
                    conversoes.append(
                        pl.col(coluna)
                        .cast(pl.Utf8, strict=False)
                        .fill_null("")
                        .str.strip()  # Remover espaços
                        .alias(coluna)
                    )
            
            if conversoes:
                df = df.with_columns(conversoes)
                logger.debug(f"Tipos aplicados: {len(schema_aplicado)} colunas convertidas")
                
                # Log de tipos detectados para debug
                tipos_resumo = {}
                for col, tipo in schema_aplicado.items():
                    tipo_nome = str(tipo).split('.')[-1]
                    tipos_resumo[tipo_nome] = tipos_resumo.get(tipo_nome, 0) + 1
                
                logger.info(f"Resumo de tipos aplicados: {tipos_resumo}")
                
                # Log detalhado de algumas colunas importantes
                colunas_importantes = ['ADMITIDOS', 'DESLIGADOS', 'SALDO', 'CNAE', 'UF']
                for col in colunas_importantes:
                    if col in schema_aplicado:
                        logger.debug(f"Coluna {col}: {schema_aplicado[col]}")
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao aplicar tipos de dados: {e}")
            return df
    
    def validar_integridade_dados(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Valida integridade dos dados CAGED com múltiplas verificações
        
        Args:
            df: DataFrame a validar
            
        Returns:
            Dict com resultados detalhados da validação
        """
        resultado_validacao = {
            'valido_geral': True,
            'validacoes': {},
            'alertas': [],
            'erros': []
        }
        
        try:
            # 1. Validação de saldo de movimentação
            resultado_saldo = self._validar_saldo_movimentacao(df)
            resultado_validacao['validacoes']['saldo_movimentacao'] = resultado_saldo
            
            if not resultado_saldo['valido']:
                resultado_validacao['valido_geral'] = False
                resultado_validacao['alertas'].append(resultado_saldo['mensagem'])
            
            # 2. Validação de consistência temporal
            resultado_temporal = self._validar_consistencia_temporal(df)
            resultado_validacao['validacoes']['consistencia_temporal'] = resultado_temporal
            
            if not resultado_temporal['valido']:
                resultado_validacao['alertas'].append(resultado_temporal['mensagem'])
            
            # 3. Validação de domínios válidos
            resultado_dominios = self._validar_dominios_validos(df)
            resultado_validacao['validacoes']['dominios_validos'] = resultado_dominios
            
            if not resultado_dominios['valido']:
                resultado_validacao['alertas'].append(resultado_dominios['mensagem'])
            
            # 4. Validação de completude de dados
            resultado_completude = self._validar_completude_dados(df)
            resultado_validacao['validacoes']['completude_dados'] = resultado_completude
            
            if not resultado_completude['valido']:
                resultado_validacao['alertas'].append(resultado_completude['mensagem'])
            
            # Log dos resultados
            if resultado_validacao['valido_geral']:
                logger.info("✅ Validação de integridade: APROVADA")
            else:
                logger.warning(f"⚠️  Validação de integridade: {len(resultado_validacao['alertas'])} alertas")
                for alerta in resultado_validacao['alertas']:
                    logger.warning(f"   - {alerta}")
            
            return resultado_validacao
            
        except Exception as e:
            erro_msg = f"Erro na validação de integridade: {e}"
            logger.error(erro_msg)
            resultado_validacao['valido_geral'] = False
            resultado_validacao['erros'].append(erro_msg)
            return resultado_validacao
    
    def _validar_saldo_movimentacao(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Valida se Admitidos - Desligados = Saldo"""
        config = VALIDACOES_INTEGRIDADE['saldo_movimentacao']
        
        if not all(col in df.columns for col in ['ADMITIDOS', 'DESLIGADOS', 'SALDO']):
            return {
                'valido': False,
                'mensagem': 'Colunas de movimentação não encontradas',
                'detalhes': 'Campos ADMITIDOS, DESLIGADOS ou SALDO ausentes'
            }
        
        try:
            # Converter para numérico e calcular saldo esperado
            df_validacao = df.with_columns([
                pl.col('ADMITIDOS').cast(pl.Int64, strict=False).alias('ADMITIDOS_NUM'),
                pl.col('DESLIGADOS').cast(pl.Int64, strict=False).alias('DESLIGADOS_NUM'),
                pl.col('SALDO').cast(pl.Int64, strict=False).alias('SALDO_NUM')
            ]).with_columns([
                (pl.col('ADMITIDOS_NUM') - pl.col('DESLIGADOS_NUM')).alias('SALDO_CALCULADO')
            ])
            
            # Verificar inconsistências (ignorar registros com valores nulos)
            inconsistencias = df_validacao.filter(
                pl.col('ADMITIDOS_NUM').is_not_null() &
                pl.col('DESLIGADOS_NUM').is_not_null() &
                pl.col('SALDO_NUM').is_not_null() &
                (pl.col('SALDO_NUM') != pl.col('SALDO_CALCULADO'))
            )
            
            # Contar registros válidos para cálculo
            registros_validos = df_validacao.filter(
                pl.col('ADMITIDOS_NUM').is_not_null() &
                pl.col('DESLIGADOS_NUM').is_not_null() &
                pl.col('SALDO_NUM').is_not_null()
            ).shape[0]
            
            registros_inconsistentes = inconsistencias.shape[0]
            percentual = (registros_inconsistentes / registros_validos) * 100 if registros_validos > 0 else 0
            
            valido = percentual <= config['tolerancia_percentual']
            
            return {
                'valido': valido,
                'percentual_inconsistente': percentual,
                'registros_inconsistentes': registros_inconsistentes,
                'total_registros': registros_validos,
                'mensagem': f"Saldo: {percentual:.2f}% inconsistente (limite: {config['tolerancia_percentual']}%)",
                'exemplos': inconsistencias.head(3).to_dicts() if registros_inconsistentes > 0 else []
            }
            
        except Exception as e:
            return {
                'valido': False,
                'mensagem': f'Erro na validação de saldo: {str(e)}',
                'detalhes': 'Falha na conversão ou cálculo dos valores'
            }
    
    def _validar_consistencia_temporal(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Valida consistência de campos temporais"""
        config = VALIDACOES_INTEGRIDADE['consistencia_temporal']
        campos_data = [c for c in config['campos_data'] if c in df.columns]
        
        if not campos_data:
            return {
                'valido': True,
                'mensagem': 'Nenhum campo temporal encontrado para validação',
                'campos_validados': []
            }
        
        problemas = []
        for campo in campos_data:
            # Verificar formato básico (YYYY-MM ou similar)
            valores_invalidos = df.filter(
                pl.col(campo).is_not_null() & 
                ~pl.col(campo).str.contains(r'^\d{4}-\d{2}$|^\d{6}$|^\d{4}/\d{2}$')
            ).shape[0]
            
            if valores_invalidos > 0:
                problemas.append(f"{campo}: {valores_invalidos} valores com formato inválido")
        
        return {
            'valido': len(problemas) == 0,
            'mensagem': f"Temporal: {len(problemas)} problemas encontrados" if problemas else "Temporal: OK",
            'campos_validados': campos_data,
            'problemas': problemas
        }
    
    def _validar_dominios_validos(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Valida se valores estão dentro de domínios esperados"""
        config = VALIDACOES_INTEGRIDADE['dominios_validos']
        problemas = []
        
        for campo, valores_validos in config.items():
            if campo == 'obrigatorio':
                continue
                
            if campo in df.columns:
                valores_invalidos = df.filter(
                    pl.col(campo).is_not_null() & 
                    ~pl.col(campo).is_in(valores_validos)
                ).shape[0]
                
                if valores_invalidos > 0:
                    problemas.append(f"{campo}: {valores_invalidos} valores fora do domínio")
        
        return {
            'valido': len(problemas) == 0,
            'mensagem': f"Domínios: {len(problemas)} problemas encontrados" if problemas else "Domínios: OK",
            'problemas': problemas
        }
    
    def _validar_completude_dados(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Valida completude dos dados (valores nulos)"""
        total_registros = df.shape[0]
        colunas_com_problemas = []
        
        for coluna in df.columns:
            nulos = df.select(pl.col(coluna).is_null().sum()).item()
            percentual_nulos = (nulos / total_registros) * 100 if total_registros > 0 else 0
            
            # Alertar se mais de 50% de valores nulos
            if percentual_nulos > 50:
                colunas_com_problemas.append(f"{coluna}: {percentual_nulos:.1f}% nulos")
        
        return {
            'valido': len(colunas_com_problemas) == 0,
            'mensagem': f"Completude: {len(colunas_com_problemas)} colunas com muitos nulos" if colunas_com_problemas else "Completude: OK",
            'colunas_problematicas': colunas_com_problemas
        }
    
    def converter_mensal(self, 
                        ano: int, 
                        mes: int,
                        campos_selecionados: Optional[List[str]] = None,
                        usar_paralelismo: Optional[bool] = None,
                        filtros_cnae: Optional[List[str]] = None,
                        filtros_periodo: Optional[Dict[str, Any]] = None,
                        filtros_movimentacao: Optional[List[str]] = None,
                        filtros_geograficos: Optional[Dict[str, Any]] = None) -> Tuple[bool, List, List, List]:
        """
        Converte dados de um mês específico para Parquet com processamento otimizado
        
        Args:
            ano: Ano dos dados
            mes: Mês dos dados
            campos_selecionados: Campos específicos a processar
            usar_paralelismo: Forçar uso/não uso de paralelismo
            filtros_cnae: Lista de códigos CNAE para filtrar
            filtros_periodo: Configurações de filtro por período
            filtros_movimentacao: Tipos de movimentação para filtrar
            filtros_geograficos: Configurações de filtro geográfico
            
        Returns:
            Tuple[bool, List[Movimentacao], List[SaldoMensal], List[Indicador]]: Resultado da conversão
        """
        with self.medidor.etapa(f"Conversão Mensal {ano}/{mes:02d}"):
            # Configurar filtros se fornecidos
            if any([filtros_cnae, filtros_periodo, filtros_movimentacao, filtros_geograficos]):
                self._configurar_filtros_dinamicos(
                    filtros_cnae=filtros_cnae,
                    filtros_periodo=filtros_periodo,
                    filtros_movimentacao=filtros_movimentacao,
                    filtros_geograficos=filtros_geograficos
                )
                logger.info(f"Filtros configurados: {self.obter_resumo_filtros()}")
            
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
        Processa arquivos em paralelo usando ThreadPoolExecutor com balanceamento de carga
        """
        resultados = []
        
        # Calcular tamanho total dos arquivos
        tamanho_total_mb = sum(arquivo.stat().st_size for arquivo in arquivos) / (1024 * 1024)
        
        # Detectar número otimizado de workers
        workers_otimizados = self._detectar_workers_otimizado(len(arquivos), tamanho_total_mb)
        
        # Usar o menor entre o configurado e o otimizado
        num_workers = min(self.max_workers, workers_otimizados)
        
        logger.info(f"Processamento paralelo: {num_workers} workers para {len(arquivos)} arquivos ({tamanho_total_mb:.1f} MB)")
        
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submeter tarefas com retry
            futures = {}
            
            for arquivo in arquivos:
                future = executor.submit(
                    self._processar_arquivo_com_retry, 
                    arquivo, ano, mes, campos_selecionados
                )
                futures[future] = arquivo
            
            # Coletar resultados com timeout por worker
            timeout_worker = CONFIG_PARALELISMO['timeout_worker']
            
            for future in as_completed(futures, timeout=timeout_worker):
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
    
    def _processar_arquivo_com_retry(self, arquivo: Path, ano: int, mes: int,
                                   campos_selecionados: Optional[List[str]]) -> Dict[str, Any]:
        """
        Processa um arquivo com sistema de retry automático
        
        Args:
            arquivo: Caminho do arquivo
            ano: Ano de referência
            mes: Mês de referência
            campos_selecionados: Campos específicos para processar
            
        Returns:
            Resultado do processamento
        """
        config = CONFIG_PARALELISMO
        max_tentativas = config['retry_max']
        
        for tentativa in range(1, max_tentativas + 1):
            try:
                resultado = self._processar_arquivo_seguro(arquivo, ano, mes, campos_selecionados)
                
                if resultado['sucesso'] or tentativa == max_tentativas:
                    if tentativa > 1:
                        logger.info(f"✅ {arquivo.name} processado na tentativa {tentativa}")
                    return resultado
                else:
                    logger.warning(f"⚠️  Tentativa {tentativa} falhou para {arquivo.name}: {resultado['erro']}")
                    time.sleep(tentativa * 0.5)  # Backoff exponencial
                    
            except Exception as e:
                if tentativa == max_tentativas:
                    logger.error(f"❌ Todas as tentativas falharam para {arquivo.name}: {e}")
                    return {
                        'sucesso': False,
                        'erro': f"Falha após {max_tentativas} tentativas: {str(e)}",
                        'df': None,
                        'movimentacoes': [],
                        'saldos': [],
                        'indicadores': []
                    }
                else:
                    logger.warning(f"⚠️  Tentativa {tentativa} com erro para {arquivo.name}: {e}")
                    time.sleep(tentativa * 0.5)
        
        # Nunca deveria chegar aqui, mas por segurança
        return {
            'sucesso': False,
            'erro': 'Erro inesperado no sistema de retry',
            'df': None,
            'movimentacoes': [],
            'saldos': [],
            'indicadores': []
        }
    
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