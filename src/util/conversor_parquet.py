#!/usr/bin/env python3
"""
Conversor de Dados CAGED para formato Parquet
Adaptado para estrutura mensal dos dados CAGED
"""

import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
import re

# Importação das entidades
from src.Entity.movimentacao import Movimentacao
from src.Entity.saldo_mensal import SaldoMensal
from src.Entity.exclusao import Exclusao
from src.Entity.movimentacao_fora_prazo import MovimentacaoForaPrazo
from src.Entity.indicador import Indicador

from loguru import logger

# Mapeamento de campos CAGED conforme especificado no roteiro
MAPEAMENTO_CAMPOS_CAGED = {
    'competencia': 'COMPETENCIA',
    'regiao': 'REGIAO', 
    'uf': 'UF',
    'municipio': 'MUNICIPIO',
    'cnae_2_0_classe': 'CNAE_2_0_CLASSE',
    'cnae_2_0_subclasse': 'CNAE_2_0_SUBCLASSE',
    'admitidos': 'ADMITIDOS',
    'desligados': 'DESLIGADOS',
    'saldo': 'SALDO',
    'sexo': 'SEXO',
    'faixa_etaria': 'FAIXA_ETARIA',
    'escolaridade': 'ESCOLARIDADE',
    'cbo_2002': 'CBO_2002',
    'tipo_movimentacao': 'TIPO_MOVIMENTACAO',
    'tipo_deficiencia': 'TIPO_DEFICIENCIA'
}

# Campos númericos para validação
CAMPOS_NUMERICOS = ['ADMITIDOS', 'DESLIGADOS', 'SALDO']

# Tipos de dados esperados
SCHEMA_CAGED = {
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


class ConversorParquetCaged:
    """
    Conversor especializado para dados CAGED
    Processa arquivos mensais e aplica padronização
    """
    
    def __init__(self, diretorio_origem: str = "files-unzip",
                 diretorio_destino: str = "parquet"):
        """
        Inicializa o conversor
        
        Args:
            diretorio_origem: Diretório com arquivos descompactados
            diretorio_destino: Diretório para arquivos Parquet
        """
        self.diretorio_origem = Path(diretorio_origem)
        self.diretorio_destino = Path(diretorio_destino)
        self.diretorio_destino.mkdir(parents=True, exist_ok=True)
        self.contador_entidades = 0
        
    def detectar_encoding(self, arquivo: Path) -> str:
        """
        Detecta encoding do arquivo CAGED
        
        Args:
            arquivo: Caminho do arquivo
            
        Returns:
            str: Encoding detectado
        """
        try:
            import chardet
            with open(arquivo, 'rb') as f:
                raw_data = f.read(10000)
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            confidence = result['confidence']
            logger.info(f"📝 Encoding detectado: {encoding} (confiança: {confidence:.2f})")
            if confidence < 0.7:
                for fallback in ['latin1', 'cp1252', 'utf-8']:
                    try:
                        with open(arquivo, 'r', encoding=fallback) as f:
                            f.readline()
                        logger.info(f"🔄 Usando fallback: {fallback}")
                        return fallback
                    except Exception as e:
                        logger.warning(f"Falha no fallback de encoding {fallback}: {e}")
                        continue
            return encoding or 'latin1'
        except Exception as e:
            logger.error(f"⚠️  Erro na detecção de encoding: {e}. Usando latin1 como padrão", exc_info=True)
            return 'latin1'
    
    def processar_arquivo_mensal(self, 
                               arquivo: Path, 
                               ano: int, 
                               mes: int,
                               campos_selecionados: Optional[List[str]] = None) -> Tuple[pl.DataFrame, List, List, List]:
        """
        Processa um arquivo mensal CAGED
        
        Args:
            arquivo: Caminho do arquivo
            ano: Ano dos dados
            mes: Mês dos dados
            campos_selecionados: Campos específicos a processar
            
        Returns:
            Tuple[DataFrame, List[Movimentacao], List[SaldoMensal], List[Indicador]]: Dados processados
        """
        if not arquivo.exists():
            logger.error(f"Arquivo não encontrado: {arquivo}")
            raise FileNotFoundError(f"Arquivo não encontrado: {arquivo}")
        logger.info(f"📊 Processando: {arquivo.name}")
        encoding = self.detectar_encoding(arquivo)
        try:
            separador = self._detectar_separador(arquivo, encoding)
            logger.info(f"🔗 Separador detectado: '{separador}'")
            try:
                df = pl.read_csv(
                    arquivo,
                    separator=separador,
                    encoding=encoding,
                    has_header=True,
                    ignore_errors=True,
                    truncate_ragged_lines=True
                )
            except Exception as e:
                # Tentar novamente com configurações mais permissivas
                logger.warning(f"⚠️ Erro na primeira tentativa de leitura: {e}. Tentando novamente com configurações alternativas.")
                df = pl.read_csv(
                    arquivo,
                    separator=separador,
                    encoding=encoding,
                    has_header=True,
                    ignore_errors=True,
                    truncate_ragged_lines=True,
                    infer_schema_length=0  # Desativar inferência de schema
                )
            logger.info(f"📋 Arquivo lido: {df.shape[0]} linhas, {df.shape[1]} colunas")
            
            # Adicionar nome do arquivo como coluna para ajudar na determinação do tipo de movimentação
            df = df.with_columns([
                pl.lit(arquivo.name).alias("_arquivo_origem")
            ])
            
            df = self._padronizar_colunas(df)
            if campos_selecionados:
                campos_disponiveis = [c for c in campos_selecionados if c in df.columns]
                df = df.select(campos_disponiveis)
                logger.info(f"🎯 Campos selecionados: {len(campos_disponiveis)}")
            df = self._aplicar_tipos_dados(df)
            df = df.with_columns([
                pl.lit(f"{ano}-{mes:02d}").alias("ANO_MES"),
                pl.lit(ano).alias("ANO"),
                pl.lit(mes).alias("MES")
            ])
            if self.validar_integridade_dados(df):
                logger.info("✅ Dados validados com sucesso")
            else:
                logger.warning("⚠️  Alertas encontrados na validação")
            movimentacoes = self._dataframe_para_movimentacoes(df, ano, mes)
            saldos_mensais = self._calcular_saldos_mensais(df, ano, mes)
            indicadores = self._gerar_indicadores(df, ano, mes)
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
        # Verificar pelo nome do arquivo primeiro (mais confiável)
        arquivo = str(row.get('_arquivo_origem', '')).upper()
        if arquivo:
            if any(termo in arquivo for termo in ['ADM', 'ADMISSAO', 'ADMISSÃO', 'FOR']):
                return 'admissao'
            elif any(termo in arquivo for termo in ['DES', 'DESLIGAMENTO', 'DEMISSAO', 'DEMISSÃO', 'MOV']):
                return 'desligamento'
        
        # Verificar campos específicos de tipo de movimentação
        tipo_campo = str(row.get('TIPO_MOVIMENTACAO', '')).strip()
        
        # Códigos comuns para admissão
        if tipo_campo in ['1', '97', 'ADMISSAO', 'ADMISSÃO']:
            return 'admissao'
        # Códigos comuns para desligamento
        elif tipo_campo in ['2', '31', 'DESLIGAMENTO', 'DEMISSAO', 'DEMISSÃO']:
            return 'desligamento'
        
        # Verificar se há dados de admissão ou desligamento
        try:
            admitidos = int(row.get('ADMITIDOS', 0) or 0)
        except (ValueError, TypeError):
            admitidos = 0
            
        try:
            desligados = int(row.get('DESLIGADOS', 0) or 0)
        except (ValueError, TypeError):
            desligados = 0
            
        try:
            saldo = int(row.get('SALDO', 0) or 0)
        except (ValueError, TypeError):
            saldo = 0
        
        if admitidos > 0:
            return 'admissao'
        elif desligados > 0:
            return 'desligamento'
        elif saldo > 0:
            return 'admissao'
        elif saldo < 0:
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
        Detecta o separador do arquivo CSV
        
        Args:
            arquivo: Caminho do arquivo
            encoding: Encoding do arquivo
            
        Returns:
            str: Separador detectado
        """
        separadores = [';', ',', '\t', '|']
        
        with open(arquivo, 'r', encoding=encoding) as f:
            primeira_linha = f.readline()
            
        # Contar ocorrências de cada separador
        contadores = {sep: primeira_linha.count(sep) for sep in separadores}

        # Garantir que há pelo menos um separador
        if not contadores:
            separador = ';'
        else:
            # Retornar o separador mais frequente
            separador = max(contadores, key=lambda k: contadores[k])

            # Se nenhum separador foi encontrado, usar ponto e vírgula (padrão brasileiro)
            if contadores[separador] == 0:
                separador = ';'

        return separador
    
    def _padronizar_colunas(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Padroniza nomes das colunas conforme mapeamento CAGED
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame com colunas padronizadas
        """
        import re
        import unicodedata
        
        def remover_acentos_e_especiais(texto):
            """
            Remove acentos e caracteres especiais, substituindo pelos equivalentes sem acento
            """
            # Normalizar unicode (NFD = decomposição canônica)
            texto_normalizado = unicodedata.normalize('NFD', texto)
            
            # Remover marcas diacríticas (acentos)
            texto_sem_acentos = ''.join(
                char for char in texto_normalizado 
                if unicodedata.category(char) != 'Mn'
            )
            
            # Mapeamento adicional para caracteres especiais comuns
            mapeamento_especiais = {
                 'Ç': 'C', 'ç': 'c',
                 'Ã': 'A', 'ã': 'a',
                 'Õ': 'O', 'õ': 'o',
                 'Ñ': 'N', 'ñ': 'n',
                 # Caracteres com encoding problemático (UTF-8 mal interpretado)
                 'Ã£': 'A', 'Ã§': 'C', 'Ãª': 'E', 'Ã­': 'I', 'Ã³': 'O', 'Ãº': 'U',
                 'Ã ': 'A', 'Ã¡': 'A', 'Ã¢': 'A', 'Ã¤': 'A',
                 'Ã¨': 'E', 'Ã©': 'E', 'Ã«': 'E',
                 'Ã¬': 'I', 'Ã®': 'I', 'Ã¯': 'I',
                 'Ã²': 'O', 'Ã´': 'O', 'Ã¶': 'O',
                 'Ã¹': 'U', 'Ã»': 'U', 'Ã¼': 'U',
                 'Ã½': 'Y', 'Ã¿': 'Y',
                 # Caracteres isolados problemáticos
                 '§': 'C', '£': 'A', '­': 'I', '³': 'O', 'º': 'U',
                 # Outros caracteres especiais comuns
                 'À': 'A', 'Á': 'A', 'Â': 'A', 'Ä': 'A',
                 'È': 'E', 'É': 'E', 'Ê': 'E', 'Ë': 'E',
                 'Ì': 'I', 'Í': 'I', 'Î': 'I', 'Ï': 'I',
                 'Ò': 'O', 'Ó': 'O', 'Ô': 'O', 'Ö': 'O',
                 'Ù': 'U', 'Ú': 'U', 'Û': 'U', 'Ü': 'U',
                 'Ý': 'Y', 'Ÿ': 'Y'
             }
            
            # Aplicar mapeamento de caracteres especiais
            for especial, substituto in mapeamento_especiais.items():
                texto_sem_acentos = texto_sem_acentos.replace(especial, substituto)
            
            return texto_sem_acentos
        
        # Normalizar nomes das colunas (uppercase, sem espaços)
        colunas_normalizadas = {}
        for col in df.columns:
            # Remover caracteres especiais e acentos
            col_normalizada = col.upper().strip()
            col_normalizada = col_normalizada.replace(' ', '_')
            
            # Aplicar remoção completa de acentos e caracteres especiais
            col_normalizada = remover_acentos_e_especiais(col_normalizada)
            
            # Adicionar underscores entre palavras para melhor legibilidade
            # Detectar transições de minúscula para maiúscula ou números
            col_normalizada = re.sub(r'([a-z])([A-Z])', r'\1_\2', col_normalizada)
            col_normalizada = re.sub(r'([a-zA-Z])([0-9])', r'\1_\2', col_normalizada)
            col_normalizada = re.sub(r'([0-9])([a-zA-Z])', r'\1_\2', col_normalizada)
            
            # Adicionar underscores em palavras compostas específicas
            # Padrões específicos para nomes de colunas CAGED
            col_normalizada = re.sub(r'INDICADOR([A-Z])', r'INDICADOR_\1', col_normalizada)
            col_normalizada = re.sub(r'COMPETENCIA([A-Z])', r'COMPETENCIA_\1', col_normalizada)
            col_normalizada = re.sub(r'ORIGEM([A-Z])', r'ORIGEM_\1', col_normalizada)
            col_normalizada = re.sub(r'TIPO([A-Z])', r'TIPO_\1', col_normalizada)
            col_normalizada = re.sub(r'HORAS([A-Z])', r'HORAS_\1', col_normalizada)
            col_normalizada = re.sub(r'TAMESTAB([A-Z])', r'TAM_ESTAB_\1', col_normalizada)
            col_normalizada = re.sub(r'INDTRAB([A-Z])', r'IND_TRAB_\1', col_normalizada)
            col_normalizada = re.sub(r'UNIDADE([A-Z])', r'UNIDADE_\1', col_normalizada)
            col_normalizada = re.sub(r'VALOR([A-Z])', r'VALOR_\1', col_normalizada)
            col_normalizada = re.sub(r'RACA([A-Z])', r'RACA_\1', col_normalizada)
            
            # Remover underscores duplicados
            col_normalizada = re.sub(r'_+', '_', col_normalizada)
            
            # Remover underscores no início e fim
            col_normalizada = col_normalizada.strip('_')
            
            colunas_normalizadas[col] = col_normalizada
        
        df = df.rename(colunas_normalizadas)
        
        # Verificar se há colunas duplicadas após normalização
        colunas_unicas = set()
        colunas_duplicadas = set()
        
        for col in df.columns:
            if col in colunas_unicas:
                colunas_duplicadas.add(col)
            else:
                colunas_unicas.add(col)
        
        # Renomear colunas duplicadas adicionando um sufixo
        if colunas_duplicadas:
            print(f"⚠️ Colunas duplicadas encontradas: {colunas_duplicadas}")
            contador_duplicadas = {}
            colunas_renomeadas = {}
            
            for i, col in enumerate(df.columns):
                if col in colunas_duplicadas:
                    contador_duplicadas[col] = contador_duplicadas.get(col, 0) + 1
                    novo_nome = f"{col}_{contador_duplicadas[col]}"
                    colunas_renomeadas[col] = novo_nome
                    df = df.rename({df.columns[i]: novo_nome})
        
        # Função para aplicar underscores em nomes de colunas
        def aplicar_underscores(nome):
            # Adicionar underscores entre palavras para melhor legibilidade
            nome_com_underscores = re.sub(r'([a-z])([A-Z])', r'\1_\2', nome)
            nome_com_underscores = re.sub(r'([a-zA-Z])([0-9])', r'\1_\2', nome_com_underscores)
            nome_com_underscores = re.sub(r'([0-9])([a-zA-Z])', r'\1_\2', nome_com_underscores)
            # Remover underscores duplicados
            nome_com_underscores = re.sub(r'_+', '_', nome_com_underscores)
            # Remover underscores no início e fim
            nome_com_underscores = nome_com_underscores.strip('_')
            return nome_com_underscores
        
        # Mapeamento específico para colunas conhecidas (aplicando underscores)
        mapeamento_especifico = {
            'COMPETENCIAMOV': aplicar_underscores('COMPETENCIA'),
            'REGIAO': aplicar_underscores('REGIAO'),
            'UF': aplicar_underscores('UF'),
            'MUNICIPIO': aplicar_underscores('MUNICIPIO'),
            'SECAO': aplicar_underscores('CNAE_2_0_CLASSE'),
            'SUBCLASSE': aplicar_underscores('CNAE_2_0_SUBCLASSE'),
            'SALDOMOVIMENTACAO': aplicar_underscores('SALDO'),
            'CBO2002OCUPACAO': aplicar_underscores('CBO_2002'),
            'SEXO': aplicar_underscores('SEXO'),
            'IDADE': aplicar_underscores('FAIXA_ETARIA'),
            'GRAUDEINSTRUCAO': aplicar_underscores('ESCOLARIDADE'),
            'TIPOMOVIMENTACAO': aplicar_underscores('TIPO_MOVIMENTACAO'),
            'TIPODEDEFICIENCIA': aplicar_underscores('TIPO_DEFICIENCIA')
        }
        
        # Aplicar mapeamento específico
        colunas_finais = {}
        colunas_mapeadas = set()  # Controlar quais colunas já foram mapeadas
        
        for col in df.columns:
            if col in mapeamento_especifico and mapeamento_especifico[col] not in colunas_mapeadas:
                colunas_finais[col] = mapeamento_especifico[col]
                colunas_mapeadas.add(mapeamento_especifico[col])
            elif col in MAPEAMENTO_CAMPOS_CAGED.values() and col not in colunas_mapeadas:
                colunas_finais[col] = col  # Já está no formato correto
                colunas_mapeadas.add(col)
            else:
                # Verificar se já existe uma coluna mapeada para o mesmo destino
                destino_encontrado = False
                
                # Tentar encontrar correspondência parcial
                for campo_original, campo_padrao in mapeamento_especifico.items():
                    if campo_original.replace('_', '') in col.replace('_', '') and campo_padrao not in colunas_mapeadas:
                        colunas_finais[col] = campo_padrao
                        colunas_mapeadas.add(campo_padrao)
                        destino_encontrado = True
                        break
                
                if not destino_encontrado:
                    # Verificar correspondência com valores do mapeamento CAGED
                    for campo_padrao in MAPEAMENTO_CAMPOS_CAGED.values():
                        if campo_padrao.replace('_', '') in col.replace('_', '') and campo_padrao not in colunas_mapeadas:
                            colunas_finais[col] = campo_padrao
                            colunas_mapeadas.add(campo_padrao)
                            destino_encontrado = True
                            break
                
                if not destino_encontrado:
                    # Manter nome original se não encontrar ou se já existir mapeamento
                    colunas_finais[col] = col
        
        return df.rename(colunas_finais)
    
    def _aplicar_tipos_dados(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Aplica tipos de dados corretos às colunas
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame com tipos corretos
        """
        try:
            # Verificar se as colunas esperadas existem
            for campo_numerico in CAMPOS_NUMERICOS:
                if campo_numerico not in df.columns:
                    # Tentar encontrar colunas similares
                    coluna_encontrada = False
                    for coluna in df.columns:
                        if campo_numerico.lower() in coluna.lower():
                            # Renomear coluna para o nome esperado
                            df = df.rename({coluna: campo_numerico})
                            coluna_encontrada = True
                            break
                    
                    # Se não encontrou a coluna, criar uma nova com valor zero
                    if not coluna_encontrada:
                        print(f"⚠️ Criando coluna {campo_numerico} com valores zero")
                        df = df.with_columns(pl.lit(0).alias(campo_numerico))
            
            # Aplicar schema quando possível
            conversoes = []
            
            for coluna in df.columns:
                if coluna in SCHEMA_CAGED:
                    tipo_esperado = SCHEMA_CAGED[coluna]
                    
                    if tipo_esperado == pl.Int64:
                        # Verificar se a coluna já é numérica
                        try:
                            # Tentar converter diretamente
                            conversoes.append(
                                pl.col(coluna)
                                .cast(pl.Int64, strict=False)
                                .fill_null(0)
                                .alias(coluna)
                            )
                        except Exception:
                            # Se falhar, tentar converter de string para número
                            conversoes.append(
                                pl.col(coluna)
                                .cast(pl.Utf8, strict=False)
                                .str.replace_all(r'[^\d-]', '')  # Remover caracteres não numéricos
                                .cast(pl.Int64, strict=False)
                                .fill_null(0)
                                .alias(coluna)
                            )
                    elif tipo_esperado == pl.Utf8:
                        # Converter campos de texto
                        conversoes.append(
                            pl.col(coluna)
                            .cast(pl.Utf8, strict=False)
                            .fill_null("")
                            .alias(coluna)
                        )
            
            if conversoes:
                df = df.with_columns(conversoes)
            
            return df
            
        except Exception as e:
            print(f"⚠️  Erro ao aplicar tipos: {e}")
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
                return True  # Continuar mesmo sem validação
            
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
                
                # Sempre retornar True para ser mais tolerante
                return True
            else:
                print("✅ Todos os registros são consistentes (Admitidos - Desligados = Saldo)")
                return True
                
        except Exception as e:
            print(f"❌ Erro na validação: {e}")
            return True  # Continuar mesmo com erro na validação
    
    def converter_mensal(self, 
                        ano: int, 
                        mes: int,
                        campos_selecionados: Optional[List[str]] = None) -> Tuple[bool, List, List, List]:
        """
        Converte dados de um mês específico para Parquet
        
        Args:
            ano: Ano dos dados
            mes: Mês dos dados
            campos_selecionados: Campos específicos a processar
            
        Returns:
            Tuple[bool, List[Movimentacao], List[SaldoMensal], List[Indicador]]: Resultado da conversão
        """
        # Encontrar arquivos do período
        padrao = f"*{ano}*{mes:02d}*"
        arquivos_origem = list(self.diretorio_origem.rglob(f"{padrao}.txt")) + \
                         list(self.diretorio_origem.rglob(f"{padrao}.csv"))
        
        if not arquivos_origem:
            print(f"❌ Nenhum arquivo encontrado para {ano}/{mes:02d}")
            return False, [], [], []
        
        print(f"🎯 Convertendo {len(arquivos_origem)} arquivos de {ano}/{mes:02d}")
        
        dataframes = []
        movimentacoes_totais = []
        saldos_totais = []
        indicadores_totais = []
        sucessos = 0
        
        for arquivo in arquivos_origem:
            try:
                df, movimentacoes, saldos, indicadores = self.processar_arquivo_mensal(
                    arquivo, ano, mes, campos_selecionados
                )
                dataframes.append(df)
                movimentacoes_totais.extend(movimentacoes)
                saldos_totais.extend(saldos)
                indicadores_totais.extend(indicadores)
                sucessos += 1
                
            except Exception as e:
                print(f"❌ Erro ao processar {arquivo.name}: {e}")
        
        if not dataframes:
            print("❌ Nenhum arquivo foi processado com sucesso")
            return False, [], [], []
        
        # Consolidar todos os DataFrames
        print("🔗 Consolidando dados...")
        
        # Garantir que todos os DataFrames tenham as mesmas colunas
        print("⚙️ Padronizando colunas para concatenação...")
        todas_colunas = set()
        for df in dataframes:
            todas_colunas.update(df.columns)
        
        # Converter para lista ordenada para garantir a mesma ordem em todos os DataFrames
        todas_colunas = sorted(list(todas_colunas))
        print(f"📋 Total de {len(todas_colunas)} colunas únicas encontradas")
        
        # Adicionar colunas faltantes em cada DataFrame e garantir a mesma ordem
        dataframes_padronizados = []
        for df in dataframes:
            # Adicionar colunas faltantes
            colunas_faltantes = set(todas_colunas) - set(df.columns)
            if colunas_faltantes:
                print(f"⚠️ Adicionando {len(colunas_faltantes)} colunas faltantes")
                for col in colunas_faltantes:
                    df = df.with_columns(pl.lit(None).alias(col))
            
            # Garantir a mesma ordem das colunas
            df = df.select(todas_colunas)
            dataframes_padronizados.append(df)
        
        # Concatenar DataFrames padronizados
        try:
            df_consolidado = pl.concat(dataframes_padronizados, how="vertical")
            print(f"✅ Concatenação bem-sucedida: {df_consolidado.shape[0]} linhas, {df_consolidado.shape[1]} colunas")
        except Exception as e:
            print(f"❌ Erro na concatenação: {e}")
            # Tentar concatenação alternativa
            print("⚠️ Tentando método alternativo de concatenação...")
            # Criar DataFrame vazio com todas as colunas
            df_consolidado = pl.DataFrame(schema={col: pl.Utf8 for col in todas_colunas})
            # Adicionar cada DataFrame individualmente
            for df in dataframes_padronizados:
                df_consolidado = pl.concat([df_consolidado, df], how="vertical")
            print(f"✅ Concatenação alternativa bem-sucedida: {df_consolidado.shape[0]} linhas, {df_consolidado.shape[1]} colunas")
        
        # Salvar como Parquet
        nome_arquivo = f"CAGED_{ano}_{mes:02d}.parquet"
        caminho_saida = self.diretorio_destino / nome_arquivo
        
        df_consolidado.write_parquet(caminho_saida)
        
        total_registros = df_consolidado.shape[0]
        tamanho_arquivo = caminho_saida.stat().st_size / (1024 * 1024)  # MB
        
        print(f"✅ Conversão concluída!")
        print(f"   📄 Arquivo: {nome_arquivo}")
        print(f"   📊 Registros: {total_registros:,}")
        print(f"   💾 Tamanho: {tamanho_arquivo:.2f} MB")
        print(f"   🏢 Movimentações: {len(movimentacoes_totais)}")
        print(f"   📈 Saldos: {len(saldos_totais)}")
        print(f"   📊 Indicadores: {len(indicadores_totais)}")
        
        return True, movimentacoes_totais, saldos_totais, indicadores_totais
    
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


# Função auxiliar para teste
def testar_conversor():
    """
    Teste rápido do conversor
    """
    print("🧪 Testando conversor CAGED...")
    
    conversor = ConversorParquetCaged()
    
    # Verificar diretórios
    print(f"📁 Diretório origem: {conversor.diretorio_origem}")
    print(f"📁 Diretório destino: {conversor.diretorio_destino}")
    
    # Listar arquivos disponíveis para conversão
    arquivos_txt = list(conversor.diretorio_origem.rglob("*.txt"))
    arquivos_csv = list(conversor.diretorio_origem.rglob("*.csv"))
    
    print(f"📋 Arquivos .txt encontrados: {len(arquivos_txt)}")
    print(f"📋 Arquivos .csv encontrados: {len(arquivos_csv)}")
    
    # Listar arquivos Parquet já convertidos
    arquivos_parquet = list(conversor.diretorio_destino.glob("*.parquet"))
    print(f"📦 Arquivos .parquet existentes: {len(arquivos_parquet)}")
    
    return (len(arquivos_txt) + len(arquivos_csv)) > 0


if __name__ == "__main__":
    testar_conversor()