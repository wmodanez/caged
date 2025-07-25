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
            df = pl.read_csv(
                arquivo,
                separator=separador,
                encoding=encoding,
                has_header=True,
                ignore_errors=True,
                truncate_ragged_lines=True
            )
            logger.info(f"📋 Arquivo lido: {df.shape[0]} linhas, {df.shape[1]} colunas")
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
        # Normalizar nomes das colunas (uppercase, sem espaços)
        colunas_normalizadas = {}
        for col in df.columns:
            col_normalizada = col.upper().strip().replace(' ', '_')
            colunas_normalizadas[col] = col_normalizada
        
        df = df.rename(colunas_normalizadas)
        
        # Aplicar mapeamento específico do CAGED
        mapeamento_reverso = {v: k for k, v in MAPEAMENTO_CAMPOS_CAGED.items()}
        
        colunas_finais = {}
        for col in df.columns:
            if col in MAPEAMENTO_CAMPOS_CAGED.values():
                colunas_finais[col] = col  # Já está no formato correto
            else:
                # Tentar encontrar correspondência parcial
                for campo_padrao in MAPEAMENTO_CAMPOS_CAGED.values():
                    if campo_padrao.replace('_', '') in col.replace('_', ''):
                        colunas_finais[col] = campo_padrao
                        break
                else:
                    colunas_finais[col] = col  # Manter nome original se não encontrar
        
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
            # Aplicar schema quando possível
            conversoes = []
            
            for coluna in df.columns:
                if coluna in SCHEMA_CAGED:
                    tipo_esperado = SCHEMA_CAGED[coluna]
                    
                    if tipo_esperado == pl.Int64:
                        # Converter campos numéricos, tratando valores inválidos
                        conversoes.append(
                            pl.col(coluna)
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
        df_consolidado = pl.concat(dataframes, how="vertical")
        
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