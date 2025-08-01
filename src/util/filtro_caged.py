#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sistema de Filtros para Dados CAGED

Implementa filtros específicos para dados CAGED:
- Filtros por CNAE (similar aos empregos verdes)
- Filtros por período/data
- Filtros por tipo de movimentação
- Filtros por região/município
"""

import logging
import polars as pl
from pathlib import Path
from typing import Set, Optional, Dict, Any, List, Union
from datetime import datetime, date
import re

logger = logging.getLogger("filtro_caged")

class FiltroCNAE:
    """
    Classe para filtrar dados CAGED por CNAE baseado em arquivo CSV com classificação.
    Permite filtrar empregos verdes, setores específicos, etc.
    """
    
    def __init__(self, caminho_arquivo_cnae: Optional[str] = None, nome_filtro: str = "CNAE"):
        """
        Inicializa o filtro de CNAE.
        
        Args:
            caminho_arquivo_cnae: Caminho para o arquivo CSV com classificação CNAE (opcional)
            nome_filtro: Nome descritivo do filtro (ex: "Empregos Verdes", "Indústria")
        """
        self.caminho_arquivo_cnae = Path(caminho_arquivo_cnae) if caminho_arquivo_cnae else None
        self.nome_filtro = nome_filtro
        self._classes_filtradas: Optional[Set[str]] = None
        
        if caminho_arquivo_cnae and not self.caminho_arquivo_cnae.exists():
            logger.warning(f"Arquivo CNAE não encontrado: {self.caminho_arquivo_cnae}")
            logger.warning(f"Filtro {nome_filtro} será desabilitado")
    
    def adicionar_codigos(self, codigos: List[str]) -> None:
        """
        Adiciona códigos CNAE manualmente ao filtro.
        
        Args:
            codigos: Lista de códigos CNAE para adicionar
        """
        if self._classes_filtradas is None:
            self._classes_filtradas = set()
        
        for codigo in codigos:
            self._classes_filtradas.add(str(codigo))
        
        logger.info(f"Adicionados {len(codigos)} códigos CNAE ao filtro {self.nome_filtro}")
        logger.debug(f"Códigos adicionados: {codigos}")
    
    def limpar_codigos(self) -> None:
        """
        Remove todos os códigos CNAE do filtro.
        """
        self._classes_filtradas = set()
        logger.info(f"Códigos CNAE removidos do filtro {self.nome_filtro}")
    
    def obter_codigos(self) -> Set[str]:
        """
        Retorna os códigos CNAE atualmente configurados.
        
        Returns:
            Conjunto com os códigos CNAE configurados
        """
        if self._classes_filtradas is None:
            return set()
        return self._classes_filtradas.copy()
    
    def carregar_classes_filtradas(self, situacao_desejada: int = 1) -> Set[str]:
        """
        Carrega as classes CNAE com a situação desejada.
        
        Args:
            situacao_desejada: Valor da coluna SITUACAO para filtrar (padrão: 1)
            
        Returns:
            Conjunto com os códigos das classes CNAE filtradas
        """
        if self._classes_filtradas is not None:
            return self._classes_filtradas
        
        if not self.caminho_arquivo_cnae.exists():
            logger.error(f"Arquivo CNAE não encontrado: {self.caminho_arquivo_cnae}")
            return set()
        
        try:
            # Ler arquivo CSV com separador ';'
            df_cnae = pl.read_csv(
                self.caminho_arquivo_cnae,
                separator=';',
                encoding='utf-8'
            )
            
            # Verificar se as colunas necessárias existem
            colunas_necessarias = ["CLASSE_CNAE", "SITUACAO"]
            colunas_faltantes = [col for col in colunas_necessarias if col not in df_cnae.columns]
            
            if colunas_faltantes:
                logger.error(f"Colunas necessárias não encontradas: {colunas_faltantes}")
                logger.info(f"Colunas disponíveis: {df_cnae.columns}")
                return set()
            
            # Filtrar apenas as classes com a situação desejada
            df_filtrado = df_cnae.filter(pl.col("SITUACAO") == situacao_desejada)
            
            # Extrair os códigos das classes filtradas como strings
            classes_filtradas = set(str(codigo) for codigo in df_filtrado["CLASSE_CNAE"].to_list())
            
            logger.info(f"Carregadas {len(classes_filtradas)} classes CNAE para {self.nome_filtro} (situação {situacao_desejada})")
            logger.debug(f"Classes filtradas: {sorted(classes_filtradas)}")
            
            self._classes_filtradas = classes_filtradas
            return classes_filtradas
            
        except Exception as e:
            logger.error(f"Erro ao carregar classes filtradas: {e}")
            return set()
    
    def filtrar_dataframe(self, df: pl.DataFrame, coluna_cnae: str = "CNAESUBCLASSE", situacao_desejada: int = 1) -> pl.DataFrame:
        """
        Filtra um DataFrame mantendo apenas os registros com CNAE da classificação desejada.
        
        Args:
            df: DataFrame a ser filtrado
            coluna_cnae: Nome da coluna que contém o código CNAE (padrão: CNAESUBCLASSE para CAGED)
            situacao_desejada: Valor da coluna SITUACAO para filtrar
            
        Returns:
            DataFrame filtrado apenas com os registros da classificação desejada
        """
        # Usar códigos adicionados manualmente se disponíveis
        if self._classes_filtradas is not None and len(self._classes_filtradas) > 0:
            classes_filtradas = self._classes_filtradas
        elif self.caminho_arquivo_cnae and self.caminho_arquivo_cnae.exists():
            # Carregar classes filtradas do arquivo se ainda não foram carregadas
            classes_filtradas = self.carregar_classes_filtradas(situacao_desejada)
        else:
            logger.warning(f"Nenhum código CNAE configurado para {self.nome_filtro}, retornando DataFrame original")
            return df
        
        if not classes_filtradas:
            logger.warning(f"Nenhuma classe encontrada para {self.nome_filtro}, retornando DataFrame original")
            return df
        
        # Verificar se a coluna CNAE existe no DataFrame
        if coluna_cnae not in df.columns:
            logger.warning(f"Coluna {coluna_cnae} não encontrada no DataFrame")
            logger.info(f"Colunas disponíveis: {df.columns}")
            return df
        
        # Contar registros antes do filtro
        total_antes = df.height
        
        # Filtrar apenas registros com CNAE da classificação desejada
        classes_lista = list(classes_filtradas)
        df_filtrado = df.filter(pl.col(coluna_cnae).is_in(classes_lista))
        
        # Contar registros após o filtro
        total_depois = df_filtrado.height
        
        logger.info(f"Filtro {self.nome_filtro} aplicado:")
        logger.info(f"  Total antes: {total_antes:,} registros")
        logger.info(f"  Total depois: {total_depois:,} registros")
        logger.info(f"  Registros mantidos: {total_depois:,} ({total_depois/total_antes*100:.2f}%)")
        
        return df_filtrado
    
    def verificar_disponibilidade(self) -> bool:
        """
        Verifica se o filtro está disponível.
        
        Returns:
            True se o arquivo CNAE existe e pode ser carregado
        """
        if not self.caminho_arquivo_cnae.exists():
            return False
        
        try:
            classes_filtradas = self.carregar_classes_filtradas()
            return len(classes_filtradas) > 0
        except Exception:
            return False
    
    def obter_estatisticas(self, situacao_desejada: int = 1) -> dict:
        """
        Obtém estatísticas sobre as classes filtradas carregadas.
        
        Args:
            situacao_desejada: Valor da coluna SITUACAO para filtrar
            
        Returns:
            Dicionário com estatísticas
        """
        if not self.caminho_arquivo_cnae.exists():
            return {
                "disponivel": False,
                "nome_filtro": self.nome_filtro,
                "total_classes": 0,
                "classes_filtradas": 0,
                "percentual_filtrado": 0.0,
                "situacao_desejada": situacao_desejada
            }
        
        try:
            # Ler arquivo completo
            df_cnae = pl.read_csv(
                self.caminho_arquivo_cnae,
                separator=';',
                encoding='utf-8'
            )
            
            total_classes = df_cnae.height
            classes_filtradas = len(self.carregar_classes_filtradas(situacao_desejada))
            percentual = (classes_filtradas / total_classes * 100) if total_classes > 0 else 0
            
            return {
                "disponivel": True,
                "nome_filtro": self.nome_filtro,
                "total_classes": total_classes,
                "classes_filtradas": classes_filtradas,
                "percentual_filtrado": percentual,
                "situacao_desejada": situacao_desejada
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            return {
                "disponivel": False,
                "nome_filtro": self.nome_filtro,
                "total_classes": 0,
                "classes_filtradas": 0,
                "percentual_filtrado": 0.0,
                "situacao_desejada": situacao_desejada
            }


class FiltroPeriodo:
    """
    Classe para filtrar dados CAGED por período/data.
    """
    
    def __init__(self, data_inicio: Optional[Union[str, date]] = None, 
                 data_fim: Optional[Union[str, date]] = None,
                 anos: Optional[List[int]] = None,
                 meses: Optional[List[int]] = None):
        """
        Inicializa o filtro de período.
        
        Args:
            data_inicio: Data de início (formato YYYY-MM-DD ou objeto date)
            data_fim: Data de fim (formato YYYY-MM-DD ou objeto date)
            anos: Lista de anos específicos para filtrar
            meses: Lista de meses específicos para filtrar (1-12)
        """
        self.data_inicio = self._converter_data(data_inicio) if data_inicio else None
        self.data_fim = self._converter_data(data_fim) if data_fim else None
        self.anos = anos or []
        self.meses = meses or []
        
        logger.info(f"Filtro de período configurado:")
        if self.data_inicio:
            logger.info(f"  Data início: {self.data_inicio}")
        if self.data_fim:
            logger.info(f"  Data fim: {self.data_fim}")
        if self.anos:
            logger.info(f"  Anos: {self.anos}")
        if self.meses:
            logger.info(f"  Meses: {self.meses}")
    
    def configurar_anos(self, anos: List[int]) -> None:
        """
        Configura os anos para filtrar.
        
        Args:
            anos: Lista de anos para filtrar
        """
        self.anos = anos
        logger.info(f"Anos configurados para filtro: {anos}")
    
    def configurar_meses(self, meses: List[int]) -> None:
        """
        Configura os meses para filtrar.
        
        Args:
            meses: Lista de meses para filtrar (1-12)
        """
        self.meses = meses
        logger.info(f"Meses configurados para filtro: {meses}")
    
    def configurar_intervalo(self, data_inicio: Union[str, date], data_fim: Union[str, date]) -> None:
        """
        Configura um intervalo de datas para filtrar.
        
        Args:
            data_inicio: Data de início
            data_fim: Data de fim
        """
        self.data_inicio = self._converter_data(data_inicio)
        self.data_fim = self._converter_data(data_fim)
        logger.info(f"Intervalo configurado: {self.data_inicio} a {self.data_fim}")
    
    def _converter_data(self, data_input: Union[str, date]) -> date:
        """
        Converte string ou date para objeto date.
        
        Args:
            data_input: Data como string (YYYY-MM-DD) ou objeto date
            
        Returns:
            Objeto date
        """
        if isinstance(data_input, date):
            return data_input
        elif isinstance(data_input, str):
            try:
                return datetime.strptime(data_input, "%Y-%m-%d").date()
            except ValueError:
                logger.error(f"Formato de data inválido: {data_input}. Use YYYY-MM-DD")
                raise
        else:
            raise ValueError(f"Tipo de data não suportado: {type(data_input)}")
    
    def filtrar_dataframe(self, df: pl.DataFrame, 
                         coluna_competencia: str = "COMPETENCIA",
                         coluna_ano: str = "ANO",
                         coluna_mes: str = "MES") -> pl.DataFrame:
        """
        Filtra um DataFrame por período baseado na coluna de competência ou colunas ANO/MES.
        
        Args:
            df: DataFrame a ser filtrado
            coluna_competencia: Nome da coluna que contém a competência (formato YYYYMM)
            coluna_ano: Nome da coluna que contém o ano
            coluna_mes: Nome da coluna que contém o mês
            
        Returns:
            DataFrame filtrado por período
        """
        total_antes = df.height
        df_filtrado = df
        
        # Verificar se temos coluna de competência ou colunas separadas
        tem_competencia = coluna_competencia in df.columns
        tem_ano_mes = coluna_ano in df.columns and coluna_mes in df.columns
        
        if not tem_competencia and not tem_ano_mes:
            logger.warning(f"Nenhuma coluna de período encontrada no DataFrame")
            logger.info(f"Colunas disponíveis: {df.columns}")
            return df
        
        # Filtrar por anos específicos
        if self.anos:
            if tem_competencia:
                # Extrair ano da competência (primeiros 4 dígitos)
                df_filtrado = df_filtrado.filter(
                    pl.col(coluna_competencia).cast(pl.Utf8).str.slice(0, 4).cast(pl.Int32).is_in(self.anos)
                )
            elif tem_ano_mes:
                # Usar coluna ANO diretamente
                df_filtrado = df_filtrado.filter(pl.col(coluna_ano).is_in(self.anos))
        
        # Filtrar por meses específicos
        if self.meses:
            if tem_competencia:
                # Extrair mês da competência (últimos 2 dígitos)
                df_filtrado = df_filtrado.filter(
                    pl.col(coluna_competencia).cast(pl.Utf8).str.slice(4, 2).cast(pl.Int32).is_in(self.meses)
                )
            elif tem_ano_mes:
                # Usar coluna MES diretamente
                df_filtrado = df_filtrado.filter(pl.col(coluna_mes).is_in(self.meses))
        
        # Filtrar por intervalo de datas
        if self.data_inicio or self.data_fim:
            # Converter competência para data (assumindo dia 1 do mês)
            df_com_data = df_filtrado.with_columns([
                pl.col(coluna_competencia).cast(pl.Utf8).str.strptime(pl.Date, "%Y%m", strict=False).alias("data_competencia")
            ])
            
            if self.data_inicio:
                df_com_data = df_com_data.filter(pl.col("data_competencia") >= self.data_inicio)
            
            if self.data_fim:
                df_com_data = df_com_data.filter(pl.col("data_competencia") <= self.data_fim)
            
            # Remover coluna temporária
            df_filtrado = df_com_data.drop("data_competencia")
        
        total_depois = df_filtrado.height
        
        logger.info(f"Filtro de período aplicado:")
        logger.info(f"  Total antes: {total_antes:,} registros")
        logger.info(f"  Total depois: {total_depois:,} registros")
        logger.info(f"  Registros mantidos: {total_depois:,} ({total_depois/total_antes*100:.2f}%)")
        
        return df_filtrado


class FiltroMovimentacao:
    """
    Classe para filtrar dados CAGED por tipo de movimentação.
    """
    
    def __init__(self, tipos_movimentacao: Optional[List[Union[int, str]]] = None,
                 apenas_admissoes: bool = False,
                 apenas_desligamentos: bool = False):
        """
        Inicializa o filtro de movimentação.
        
        Args:
            tipos_movimentacao: Lista de códigos de tipo de movimentação específicos
            apenas_admissoes: Se True, filtra apenas admissões (saldo > 0)
            apenas_desligamentos: Se True, filtra apenas desligamentos (saldo < 0)
        """
        self.tipos_movimentacao = tipos_movimentacao or []
        self.apenas_admissoes = apenas_admissoes
        self.apenas_desligamentos = apenas_desligamentos
        
        if self.apenas_admissoes and self.apenas_desligamentos:
            raise ValueError("Não é possível filtrar apenas admissões E apenas desligamentos")
        
        logger.info(f"Filtro de movimentação configurado:")
        if self.tipos_movimentacao:
            logger.info(f"  Tipos específicos: {self.tipos_movimentacao}")
        if self.apenas_admissoes:
            logger.info(f"  Apenas admissões (saldo > 0)")
        if self.apenas_desligamentos:
            logger.info(f"  Apenas desligamentos (saldo < 0)")
    
    def configurar_tipos(self, tipos: List[Union[int, str]]) -> None:
        """
        Configura os tipos de movimentação para filtrar.
        
        Args:
            tipos: Lista de tipos de movimentação
        """
        self.tipos_movimentacao = tipos
        logger.info(f"Tipos de movimentação configurados: {tipos}")
    
    def configurar_apenas_admissoes(self) -> None:
        """
        Configura o filtro para mostrar apenas admissões.
        """
        self.apenas_admissoes = True
        self.apenas_desligamentos = False
        logger.info("Filtro configurado para apenas admissões")
    
    def configurar_apenas_desligamentos(self) -> None:
        """
        Configura o filtro para mostrar apenas desligamentos.
        """
        self.apenas_admissoes = False
        self.apenas_desligamentos = True
        logger.info("Filtro configurado para apenas desligamentos")
    
    def filtrar_dataframe(self, df: pl.DataFrame, 
                         coluna_tipo: str = "TIPO_MOVIMENTACAO",
                         coluna_saldo: str = "SALDOMOVIMENTACAO") -> pl.DataFrame:
        """
        Filtra um DataFrame por tipo de movimentação.
        
        Args:
            df: DataFrame a ser filtrado
            coluna_tipo: Nome da coluna que contém o tipo de movimentação
            coluna_saldo: Nome da coluna que contém o saldo de movimentação
            
        Returns:
            DataFrame filtrado por tipo de movimentação
        """
        total_antes = df.height
        df_filtrado = df
        
        # Filtrar por tipos específicos de movimentação
        if self.tipos_movimentacao:
            if coluna_tipo not in df.columns:
                logger.warning(f"Coluna {coluna_tipo} não encontrada no DataFrame")
            else:
                df_filtrado = df_filtrado.filter(pl.col(coluna_tipo).is_in(self.tipos_movimentacao))
        
        # Filtrar por saldo (admissões/desligamentos)
        if self.apenas_admissoes or self.apenas_desligamentos:
            if coluna_saldo not in df.columns:
                logger.warning(f"Coluna {coluna_saldo} não encontrada no DataFrame")
            else:
                if self.apenas_admissoes:
                    df_filtrado = df_filtrado.filter(pl.col(coluna_saldo) > 0)
                elif self.apenas_desligamentos:
                    df_filtrado = df_filtrado.filter(pl.col(coluna_saldo) < 0)
        
        total_depois = df_filtrado.height
        
        logger.info(f"Filtro de movimentação aplicado:")
        logger.info(f"  Total antes: {total_antes:,} registros")
        logger.info(f"  Total depois: {total_depois:,} registros")
        logger.info(f"  Registros mantidos: {total_depois:,} ({total_depois/total_antes*100:.2f}%)")
        
        return df_filtrado


class FiltroGeografico:
    """
    Classe para filtrar dados CAGED por região/município.
    """
    
    def __init__(self, ufs: Optional[List[str]] = None,
                 regioes: Optional[List[Union[int, str]]] = None,
                 municipios: Optional[List[Union[int, str]]] = None,
                 ceps: Optional[List[str]] = None):
        """
        Inicializa o filtro geográfico.
        
        Args:
            ufs: Lista de códigos de UF (ex: ['SP', 'RJ', 'MG'])
            regioes: Lista de códigos de região (ex: [1, 2, 3, 4, 5])
            municipios: Lista de códigos de município
            ceps: Lista de CEPs ou padrões de CEP
        """
        self.ufs = [uf.upper() for uf in ufs] if ufs else []
        self.regioes = regioes or []
        self.municipios = municipios or []
        self.ceps = ceps or []
        
        logger.info(f"Filtro geográfico configurado:")
        if self.ufs:
            logger.info(f"  UFs: {self.ufs}")
        if self.regioes:
            logger.info(f"  Regiões: {self.regioes}")
        if self.municipios:
            logger.info(f"  Municípios: {len(self.municipios)} códigos")
        if self.ceps:
            logger.info(f"  CEPs: {len(self.ceps)} padrões")
    
    def configurar_ufs(self, ufs: List[str]) -> None:
        """
        Configura as UFs para filtrar.
        
        Args:
            ufs: Lista de códigos de UF (ex: ['SP', 'RJ', 'MG'])
        """
        self.ufs = [uf.upper() for uf in ufs]
        logger.info(f"UFs configuradas: {self.ufs}")
    
    def configurar_ceps(self, ceps: List[str]) -> None:
        """
        Configura os CEPs ou padrões de CEP para filtrar.
        
        Args:
            ceps: Lista de CEPs ou padrões de CEP (ex: ['01*', '20*'])
        """
        self.ceps = ceps
        logger.info(f"CEPs configurados: {self.ceps}")
    
    def configurar_municipios(self, municipios: List[Union[int, str]]) -> None:
        """
        Configura os municípios para filtrar.
        
        Args:
            municipios: Lista de códigos de município
        """
        self.municipios = municipios
        logger.info(f"Municípios configurados: {len(self.municipios)} códigos")
    
    def configurar_regioes(self, regioes: List[Union[int, str]]) -> None:
        """
        Configura as regiões para filtrar.
        
        Args:
            regioes: Lista de códigos de região (ex: [1, 2, 3, 4, 5])
        """
        self.regioes = regioes
        logger.info(f"Regiões configuradas: {self.regioes}")
    
    def filtrar_dataframe(self, df: pl.DataFrame,
                         coluna_uf: str = "UF",
                         coluna_regiao: str = "REGIAO",
                         coluna_municipio: str = "MUNICIPIO",
                         coluna_cep: str = "CEP") -> pl.DataFrame:
        """
        Filtra um DataFrame por critérios geográficos.
        
        Args:
            df: DataFrame a ser filtrado
            coluna_uf: Nome da coluna que contém a UF
            coluna_regiao: Nome da coluna que contém a região
            coluna_municipio: Nome da coluna que contém o município
            coluna_cep: Nome da coluna que contém o CEP
            
        Returns:
            DataFrame filtrado por critérios geográficos
        """
        total_antes = df.height
        df_filtrado = df
        
        # Filtrar por UFs
        if self.ufs:
            if coluna_uf not in df.columns:
                logger.warning(f"Coluna {coluna_uf} não encontrada no DataFrame")
            else:
                df_filtrado = df_filtrado.filter(pl.col(coluna_uf).is_in(self.ufs))
        
        # Filtrar por regiões
        if self.regioes:
            if coluna_regiao not in df.columns:
                logger.warning(f"Coluna {coluna_regiao} não encontrada no DataFrame")
            else:
                df_filtrado = df_filtrado.filter(pl.col(coluna_regiao).is_in(self.regioes))
        
        # Filtrar por municípios
        if self.municipios:
            if coluna_municipio not in df.columns:
                logger.warning(f"Coluna {coluna_municipio} não encontrada no DataFrame")
            else:
                df_filtrado = df_filtrado.filter(pl.col(coluna_municipio).is_in(self.municipios))
        
        # Filtrar por CEPs
        if self.ceps:
            if coluna_cep not in df.columns:
                logger.warning(f"Coluna {coluna_cep} não encontrada no DataFrame")
            else:
                # Criar condição OR para todos os padrões de CEP
                condicoes_cep = []
                for cep_padrao in self.ceps:
                    if '*' in cep_padrao or '?' in cep_padrao:
                        # Padrão com wildcards - converter para regex
                        regex_padrao = cep_padrao.replace('*', '.*').replace('?', '.')
                        condicoes_cep.append(pl.col(coluna_cep).str.contains(f"^{regex_padrao}$"))
                    else:
                        # CEP exato
                        condicoes_cep.append(pl.col(coluna_cep) == cep_padrao)
                
                if condicoes_cep:
                    # Combinar todas as condições com OR
                    condicao_final = condicoes_cep[0]
                    for condicao in condicoes_cep[1:]:
                        condicao_final = condicao_final | condicao
                    
                    df_filtrado = df_filtrado.filter(condicao_final)
        
        total_depois = df_filtrado.height
        
        logger.info(f"Filtro geográfico aplicado:")
        logger.info(f"  Total antes: {total_antes:,} registros")
        logger.info(f"  Total depois: {total_depois:,} registros")
        logger.info(f"  Registros mantidos: {total_depois:,} ({total_depois/total_antes*100:.2f}%)")
        
        return df_filtrado


class GerenciadorFiltros:
    """
    Classe principal para gerenciar múltiplos filtros aplicados aos dados CAGED.
    """
    
    def __init__(self):
        """
        Inicializa o gerenciador de filtros.
        """
        self.filtros = []
        logger.info("Gerenciador de filtros inicializado")
    
    def adicionar_filtro(self, filtro) -> 'GerenciadorFiltros':
        """
        Adiciona um filtro genérico ao gerenciador.
        
        Args:
            filtro: Instância de qualquer classe de filtro
            
        Returns:
            Self para permitir method chaining
        """
        if isinstance(filtro, FiltroCNAE):
            self.filtros.append(("cnae", filtro, {"situacao": 1}))
            logger.info(f"Filtro CNAE '{filtro.nome_filtro}' adicionado")
        elif isinstance(filtro, FiltroPeriodo):
            self.filtros.append(("periodo", filtro, {}))
            logger.info("Filtro de período adicionado")
        elif isinstance(filtro, FiltroMovimentacao):
            self.filtros.append(("movimentacao", filtro, {}))
            logger.info("Filtro de movimentação adicionado")
        elif isinstance(filtro, FiltroGeografico):
            self.filtros.append(("geografico", filtro, {}))
            logger.info("Filtro geográfico adicionado")
        else:
            logger.warning(f"Tipo de filtro não reconhecido: {type(filtro)}")
        
        return self
    
    def adicionar_filtro_cnae(self, caminho_arquivo: str, nome_filtro: str = "CNAE", situacao: int = 1) -> 'GerenciadorFiltros':
        """
        Adiciona um filtro CNAE.
        
        Args:
            caminho_arquivo: Caminho para o arquivo CSV com classificação CNAE
            nome_filtro: Nome descritivo do filtro
            situacao: Valor da coluna SITUACAO para filtrar
            
        Returns:
            Self para permitir method chaining
        """
        filtro = FiltroCNAE(caminho_arquivo, nome_filtro)
        if filtro.verificar_disponibilidade():
            self.filtros.append(("cnae", filtro, {"situacao": situacao}))
            logger.info(f"Filtro CNAE '{nome_filtro}' adicionado")
        else:
            logger.warning(f"Filtro CNAE '{nome_filtro}' não pôde ser adicionado")
        return self
    
    def adicionar_filtro_periodo(self, **kwargs) -> 'GerenciadorFiltros':
        """
        Adiciona um filtro de período.
        
        Args:
            **kwargs: Argumentos para FiltroPeriodo
            
        Returns:
            Self para permitir method chaining
        """
        filtro = FiltroPeriodo(**kwargs)
        self.filtros.append(("periodo", filtro, {}))
        logger.info("Filtro de período adicionado")
        return self
    
    def adicionar_filtro_movimentacao(self, **kwargs) -> 'GerenciadorFiltros':
        """
        Adiciona um filtro de movimentação.
        
        Args:
            **kwargs: Argumentos para FiltroMovimentacao
            
        Returns:
            Self para permitir method chaining
        """
        filtro = FiltroMovimentacao(**kwargs)
        self.filtros.append(("movimentacao", filtro, {}))
        logger.info("Filtro de movimentação adicionado")
        return self
    
    def adicionar_filtro_geografico(self, **kwargs) -> 'GerenciadorFiltros':
        """
        Adiciona um filtro geográfico.
        
        Args:
            **kwargs: Argumentos para FiltroGeografico
            
        Returns:
            Self para permitir method chaining
        """
        filtro = FiltroGeografico(**kwargs)
        self.filtros.append(("geografico", filtro, {}))
        logger.info("Filtro geográfico adicionado")
        return self
    
    def aplicar_filtros(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Aplica todos os filtros configurados ao DataFrame.
        
        Args:
            df: DataFrame a ser filtrado
            
        Returns:
            DataFrame com todos os filtros aplicados
        """
        if not self.filtros:
            logger.info("Nenhum filtro configurado, retornando DataFrame original")
            return df
        
        total_inicial = df.height
        df_filtrado = df
        
        logger.info(f"Aplicando {len(self.filtros)} filtros ao DataFrame...")
        
        for i, (tipo_filtro, filtro, params) in enumerate(self.filtros, 1):
            logger.info(f"Aplicando filtro {i}/{len(self.filtros)}: {tipo_filtro}")
            
            try:
                if tipo_filtro == "cnae":
                    # Verificar se temos a coluna CNAE ou CNAESUBCLASSE
                    coluna_cnae = "CNAE" if "CNAE" in df_filtrado.columns else "CNAESUBCLASSE"
                    df_filtrado = filtro.filtrar_dataframe(df_filtrado, coluna_cnae=coluna_cnae, situacao_desejada=params["situacao"])
                else:
                    df_filtrado = filtro.filtrar_dataframe(df_filtrado)
                    
            except Exception as e:
                logger.error(f"Erro ao aplicar filtro {tipo_filtro}: {e}")
                continue
        
        total_final = df_filtrado.height
        
        logger.info(f"Filtros aplicados com sucesso:")
        logger.info(f"  Total inicial: {total_inicial:,} registros")
        logger.info(f"  Total final: {total_final:,} registros")
        logger.info(f"  Redução: {total_inicial - total_final:,} registros ({(total_inicial - total_final)/total_inicial*100:.2f}%)")
        
        return df_filtrado
    
    def obter_resumo_filtros(self) -> Dict[str, Any]:
        """
        Obtém um resumo dos filtros configurados.
        
        Returns:
            Dicionário com resumo dos filtros
        """
        resumo = {
            "total_filtros": len(self.filtros),
            "tipos_filtros": {},
            "filtros_detalhados": []
        }
        
        for tipo_filtro, filtro, params in self.filtros:
            # Contar tipos
            if tipo_filtro not in resumo["tipos_filtros"]:
                resumo["tipos_filtros"][tipo_filtro] = 0
            resumo["tipos_filtros"][tipo_filtro] += 1
            
            # Detalhes do filtro
            detalhes = {
                "tipo": tipo_filtro,
                "parametros": params
            }
            
            if hasattr(filtro, 'nome_filtro'):
                detalhes["nome"] = filtro.nome_filtro
            
            resumo["filtros_detalhados"].append(detalhes)
        
        return resumo