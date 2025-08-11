"""Serviço para cálculo e atualização do saldo de empregos."""

import polars as pl
from pathlib import Path
from typing import List
import logging

class SaldoService:
    def __init__(self, parquet_path: str = "files-parquet", logger: logging.Logger = None):
        self.parquet_path = Path(parquet_path)
        self.logger = logger or logging.getLogger(__name__)
        
        # Codigos de movimentacao baseados no arquivo de referencia e layout oficial CAGED
        # Admissoes (expandido para incluir mais códigos válidos)
        self.codigos_admissao = ["10", "20", "25", "35", "70", "97", "1", "2", "3", "5", "7"]
        # Desligamentos (expandido para incluir mais códigos válidos)
        self.codigos_desligamento = ["31", "32", "33", "40", "43", "45", "50", "60", "80", "90", "98", "99", "4", "6", "8", "9"]
        
        # Códigos alternativos para compatibilidade
        self.codigos_admissao_alt = ["ADMISSAO", "ADMISSÃO", "ADMIT", "A"]
        self.codigos_desligamento_alt = ["DESLIGAMENTO", "DEMISSAO", "DEMISSÃO", "DESLIG", "D"]

    def calcular_saldo_mensal(self, anos: List[int], meses: List[int]):
        """
        Calcula o saldo mensal seguindo a abordagem simplificada do arquivo de referencia.
        """
        self.logger.debug(f"Calculando saldo para anos: {anos}, meses: {meses}")
        
        # 1. Carregar e combinar todos os tipos de dados
        df_combined = self._carregar_e_combinar_dados(anos, meses)
        
        if df_combined.is_empty():
            self.logger.warning("Nenhum dado encontrado para o periodo especificado")
            return
        
        # 2. Aplicar transformacoes conforme arquivo de referencia
        df_processed = self._processar_dados(df_combined)
        
        # 3. Calcular saldo por competencia
        df_saldo = self._calcular_saldo_por_competencia(df_processed)
        
        # 4. Salvar resultado
        output_path = self.parquet_path / "SALDOMENSAL.parquet"
        df_saldo.write_parquet(output_path)
        
        self.logger.debug(f"Saldo mensal salvo em: {output_path}")
        print(f"Saldo mensal salvo em: {output_path}")
        
        return df_saldo

    def _carregar_e_combinar_dados(self, anos: List[int], meses: List[int]) -> pl.DataFrame:
        """
        Carrega e combina dados de MOV, EXC e FOR seguindo a abordagem do arquivo de referencia.
        """
        dfs = []
        
        # Carregar dados de movimentacao (MOV)
        df_mov = self._carregar_dados_tipo(anos, meses, "CAGEDMOV")
        if not df_mov.is_empty():
            df_mov = df_mov.with_columns(pl.lit("MOV").alias("Tipo"))
            dfs.append(df_mov)
            self.logger.debug(f"Carregados {df_mov.height} registros MOV")
        
        # Carregar dados de exclusao (EXC)
        df_exc = self._carregar_dados_tipo(anos, meses, "CAGEDEXC")
        if not df_exc.is_empty():
            # Aplicar transformacao nas exclusoes conforme arquivo de referencia
            # Os valores do saldo movimentacao sao multiplicados por (-1) pois exclusoes de admissoes 
            # diminuem o saldo e exclusoes de desligamentos aumentam o saldo.
            df_exc = df_exc.with_columns([
                pl.when(pl.col("SALDO_MOVIMENTACAO") == 1)
                .then(-1)
                .when(pl.col("SALDO_MOVIMENTACAO") == -1)
                .then(1)
                .otherwise(pl.col("SALDO_MOVIMENTACAO") * -1)
                .alias("SALDO_MOVIMENTACAO"),
                pl.lit("EXC").alias("Tipo")
            ])
            dfs.append(df_exc)
            self.logger.debug(f"Carregados {df_exc.height} registros EXC")
        
        # Carregar dados fora do prazo (FOR)
        df_for = self._carregar_dados_tipo(anos, meses, "CAGEDFORA")
        if not df_for.is_empty():
            df_for = df_for.with_columns(pl.lit("FOR").alias("Tipo"))
            dfs.append(df_for)
            self.logger.debug(f"Carregados {df_for.height} registros FOR")
        
        # Combinar todos os DataFrames
        if dfs:
            df_combined = pl.concat(dfs, how="vertical")
            self.logger.info(f"Total de registros combinados: {df_combined.height}")
            return df_combined
        else:
            return pl.DataFrame()

    def _carregar_dados_tipo(self, anos: List[int], meses: List[int], tipo_arquivo: str) -> pl.DataFrame:
        """Carrega arquivos Parquet para um determinado tipo e periodo."""
        arquivos = []
        for ano in anos:
            for mes in meses:
                dir_path = self.parquet_path / str(ano) / f"{ano}{mes:02d}"
                if dir_path.exists():
                    arquivos.extend(dir_path.glob(f"*{tipo_arquivo}*.parquet"))
        
        if not arquivos:
            self.logger.info(f"Nenhum arquivo {tipo_arquivo} encontrado para anos {anos}, meses {meses}")
            return pl.DataFrame()
        
        return pl.read_parquet(arquivos)

    def _tratar_tipos_dados(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Trata os tipos de dados garantindo que campos numéricos sejam processados corretamente.
        """
        # Garantir que TIPO_MOVIMENTACAO seja string
        if "TIPO_MOVIMENTACAO" in df.columns:
            df = df.with_columns([
                pl.col("TIPO_MOVIMENTACAO").cast(pl.Utf8).alias("TIPO_MOVIMENTACAO")
            ])
        
        # Garantir que SALDO_MOVIMENTACAO seja numérico
        if "SALDO_MOVIMENTACAO" in df.columns:
            df = df.with_columns([
                pl.col("SALDO_MOVIMENTACAO").cast(pl.Int32).alias("SALDO_MOVIMENTACAO")
            ])
        
        # Garantir que COMPETENCIA seja string para processamento
        if "COMPETENCIA" in df.columns:
            df = df.with_columns([
                pl.col("COMPETENCIA").cast(pl.Utf8).alias("COMPETENCIA")
            ])
        
        if "COMPETENCIA_MOV" in df.columns:
            df = df.with_columns([
                pl.col("COMPETENCIA_MOV").cast(pl.Utf8).alias("COMPETENCIA_MOV")
            ])
        
        return df

    def _processar_dados(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Processa os dados aplicando as transformacoes do arquivo de referencia.
        """
        # Garantir que as colunas necessarias existam
        colunas_necessarias = ["COMPETENCIA_MOV", "MUNICIPIO", "TIPO_MOVIMENTACAO"]
        for coluna in colunas_necessarias:
            if coluna not in df.columns:
                raise ValueError(f"Coluna necessária '{coluna}' não encontrada no DataFrame")
        
        # Aplicar tratamento de tipos de dados
        df = self._tratar_tipos_dados(df)
        
        # Criar coluna de competencia padronizada
        df = df.with_columns([
            pl.col("COMPETENCIA_MOV").alias("Competencia")
        ])
        
        # Classificar movimentacao conforme arquivo de referencia (versão expandida)
        df = df.with_columns([
            pl.when(
                pl.col("TIPO_MOVIMENTACAO").cast(pl.Utf8).is_in(self.codigos_admissao) |
                pl.col("TIPO_MOVIMENTACAO").cast(pl.Utf8).str.to_uppercase().is_in(self.codigos_admissao_alt)
            )
            .then(pl.lit("Admissoes"))
            .when(
                pl.col("TIPO_MOVIMENTACAO").cast(pl.Utf8).is_in(self.codigos_desligamento) |
                pl.col("TIPO_MOVIMENTACAO").cast(pl.Utf8).str.to_uppercase().is_in(self.codigos_desligamento_alt)
            )
            .then(pl.lit("Desligamentos"))
            .otherwise(None)
            .alias("Movimentacao")
        ])
        
        # Log de tipos não reconhecidos para debug
        tipos_nao_reconhecidos = df.filter(pl.col("Movimentacao").is_null())
        if tipos_nao_reconhecidos.height > 0:
            tipos_unicos = tipos_nao_reconhecidos.select("TIPO_MOVIMENTACAO").unique().to_series().to_list()
            self.logger.warning(f"Tipos de movimentação não reconhecidos ({tipos_nao_reconhecidos.height} registros): {tipos_unicos[:10]}")
        
        # Filtrar apenas registros com movimentacao valida
        df = df.filter(pl.col("Movimentacao").is_not_null())
        
        return df
    
    def _calcular_saldo_por_competencia(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calcula o saldo por competencia seguindo a logica do arquivo de referencia.
        """
        # Agregar por competencia e tipo de movimentacao
        df_agregado = df.group_by(["Competencia", "Movimentacao"]).agg([
            pl.col("SALDO_MOVIMENTACAO").sum().alias("Total_Saldo"),
            pl.len().alias("Quantidade_Registros")
        ])
        
        # Pivotar para ter admissoes e desligamentos como colunas
        df_pivot = df_agregado.pivot(
            index="Competencia",
            columns="Movimentacao",
            values="Total_Saldo"
        ).fill_null(0)
        
        # Corrigir os desligamentos para valores positivos (abs) e calcular saldo mensal corretamente
        df_saldo = df_pivot.with_columns([
            pl.col("Desligamentos").abs().alias("Desligamentos"),  # Converter para positivo
            (pl.col("Admissoes") - pl.col("Desligamentos").abs()).alias("Saldo_Mensal")
        ])
        
        # Ordenar por competencia
        df_saldo = df_saldo.sort("Competencia")
        
        # Implementar estoque inicial para janeiro de 2020 e calcular saldo acumulado
        estoque_inicial_jan_2020 = 39054507
        
        # Calcular saldo acumulado com estoque inicial
        df_saldo = df_saldo.with_columns([
            # Para janeiro de 2020, somar o estoque inicial ao saldo mensal
            pl.when(pl.col("Competencia") == "202001")
            .then(estoque_inicial_jan_2020 + pl.col("Saldo_Mensal"))
            .otherwise(pl.col("Saldo_Mensal"))
            .cum_sum()
            .alias("Saldo_Acumulado")
        ])
        
        # Manter apenas colunas necessárias em formato padronizado
        df_saldo = df_saldo.select([
            pl.col("Competencia").alias("competencia"),
            pl.col("Admissoes").alias("admissoes"),
            pl.col("Desligamentos").alias("desligamentos"),
            pl.col("Saldo_Mensal").alias("saldo_mensal"),
            pl.col("Saldo_Acumulado").alias("estoque"),
            pl.lit("AGREGADO").alias("cnpj")
        ])
        
        return df_saldo

    def exibir_resumo_saldo(self, df_saldo: pl.DataFrame = None):
        """
        Exibe um resumo do saldo calculado.
        """
        if df_saldo is None:
            output_path = self.parquet_path / "SALDOMENSAL.parquet"
            if not output_path.exists():
                print("Arquivo de saldo nao encontrado. Execute o calculo primeiro.")
                return
            df_saldo = pl.read_parquet(output_path)
        
        print("\n=== RESUMO DO SALDO MENSAL ===")
        print(f"Periodo: {df_saldo['competencia'].min()} a {df_saldo['competencia'].max()}")
        print(f"Total de competencias: {df_saldo.height}")
        
        # Exibir primeiras e ultimas competencias
        print("\nPrimeiras 5 competencias:")
        print(df_saldo.head(5).select(["competencia", "admissoes", "desligamentos", "saldo_mensal", "estoque"]))
        
        print("\nUltimas 5 competencias:")
        print(df_saldo.tail(5).select(["competencia", "admissoes", "desligamentos", "saldo_mensal", "estoque"]))
        
        # Estatisticas gerais
        print("\n=== ESTATISTICAS GERAIS ===")
        print(f"Total de admissoes: {df_saldo['admissoes'].sum():,}")
        print(f"Total de desligamentos: {df_saldo['desligamentos'].sum():,}")
        print(f"Saldo total do periodo: {df_saldo['saldo_mensal'].sum():,}")
        print(f"Estoque final: {df_saldo['estoque'].max():,}")

    def calcular_e_atualizar_saldo(self, anos: List[int], meses: List[int]):
        """
        Método para compatibilidade com stage_handlers.
        Chama o método calcular_saldo_mensal existente.
        """
        return self.calcular_saldo_mensal(anos, meses)
    
    def calcular_e_atualizar_saldo_incremental(self, anos: List[int], meses: List[int]):
        """
        Método para cálculo incremental de saldo.
        Por enquanto, chama o método padrão.
        """
        return self.calcular_saldo_mensal(anos, meses)