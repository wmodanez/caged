#!/usr/bin/env python3
"""
Descompactador de Arquivos CAGED
Módulo para descompactar arquivos .7z baixados do CAGED
"""

import re
import hashlib
from pathlib import Path
from typing import List, Optional, Dict, Tuple, Any
from tqdm import tqdm
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Importação das entidades
from src.Entity.movimentacao import Movimentacao
from src.Entity.saldo_mensal import SaldoMensal
from src.Entity.exclusao import Exclusao
from src.Entity.movimentacao_fora_prazo import MovimentacaoForaPrazo
from src.Entity.indicador import Indicador


class DescompactadorCaged:
    """
    Descompactador especializado para arquivos CAGED
    """
    
    def __init__(self, 
                 diretorio_origem: str = "files-zip",
                 diretorio_destino: str = "files-unzip",
                 max_workers: int = 4):
        """
        Inicializa o descompactador
        
        Args:
            diretorio_origem: Diretório com arquivos .7z
            diretorio_destino: Diretório para arquivos descompactados
            max_workers: Número máximo de workers para processamento paralelo
        """
        self.diretorio_origem = Path(diretorio_origem)
        self.diretorio_destino = Path(diretorio_destino)
        self.max_workers = max_workers
        self.metadados_arquivos: Dict[str, Dict] = {}

        from loguru import logger
        self.logger = logger
        
    def descompactar_arquivo(self, arquivo_7z: Path, destino: Optional[Path] = None) -> Tuple[bool, Dict]:
        """
        Descompacta um arquivo .7z específico
        
        Args:
            arquivo_7z: Caminho do arquivo .7z
            destino: Diretório destino (padrão: auto)
            
        Returns:
            Tuple[bool, Dict]: (Sucesso, Metadados do arquivo)
        """
        if not arquivo_7z.exists():
            self.logger.error(f"❌ Arquivo não encontrado: {arquivo_7z}")
            return False, {}
        
        # Extrair metadados do nome do arquivo
        metadados = self._extrair_metadados_nome(arquivo_7z.name)
        
        if destino is None:
            # Criar subdiretório baseado no ano
            ano = self._extrair_ano_do_nome(arquivo_7z.name)
            destino = self.diretorio_destino / str(ano)
        
        destino.mkdir(parents=True, exist_ok=True)
        
        try:
            self.logger.info(f"📦 Descompactando: {arquivo_7z.name}")
            import py7zr
            
            # Verificar informações do arquivo antes da descompactação
            info_zip = self._verificar_arquivo_zip(arquivo_7z)
            metadados.update({
                "tamanho_arquivo_zip": info_zip["tamanho"],
                "hash_md5_zip": info_zip["hash_md5"]
            })
            
            with py7zr.SevenZipFile(arquivo_7z, mode='r') as archive:
                arquivos_internos = archive.getnames()
                self.logger.info(f"   📋 {len(arquivos_internos)} arquivos encontrados")
                archive.extractall(path=destino)
                metadados["arquivos_extraidos"] = arquivos_internos
                metadados["data_descompactacao"] = datetime.now().isoformat()
                metadados["destino"] = str(destino)
            
            # Validar integridade após descompactação
            info_descompactado = self._verificar_arquivo_descompactado(arquivo_7z)
            if info_descompactado["existe"] and info_descompactado["tamanho_total"] > 0:
                metadados["arquivos_validados"] = len(info_descompactado["arquivos_txt"]) + len(info_descompactado["arquivos_csv"])
                metadados["tamanho_total_descompactado"] = info_descompactado["tamanho_total"]
                self.logger.info(f"✅ Descompactado e validado com sucesso em: {destino}")
                self.logger.info(f"   📊 {metadados['arquivos_validados']} arquivos válidos, {metadados['tamanho_total_descompactado']} bytes")
            else:
                self.logger.warning(f"⚠️  Descompactação concluída mas validação falhou para: {arquivo_7z.name}")
            
            self.metadados_arquivos[arquivo_7z.name] = metadados
            return True, metadados
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao descompactar {arquivo_7z.name}: {e}")
            # Tentar limpar arquivos parcialmente extraídos
            try:
                if destino.exists():
                    for arquivo_temp in destino.glob("*.tmp"):
                        arquivo_temp.unlink()
            except:
                pass
            return False, {"erro": str(e), "arquivo": str(arquivo_7z)}
    
    def descompactar_mensal(self, ano: int, mes: int) -> Tuple[bool, List[Dict]]:
        """
        Descompacta todos os arquivos de um mês específico
        
        Args:
            ano: Ano dos dados
            mes: Mês dos dados
            
        Returns:
            Tuple[bool, List[Dict]]: (Sucesso, Lista de metadados)
        """
        import time
        
        # Criar estrutura de pastas igual à origem
        diretorio_mes = f"{ano}{mes:02d}"
        diretorio_origem_mes = self.diretorio_origem / str(ano) / diretorio_mes
        diretorio_destino_mes = self.diretorio_destino / str(ano) / diretorio_mes
        
        # Verificar se o diretório de origem existe
        if not diretorio_origem_mes.exists():
            self.logger.error(f"❌ Diretório não encontrado: {diretorio_origem_mes}")
            return False, []
        
        # Encontrar arquivos .7z no diretório específico do mês
        arquivos_7z = list(diretorio_origem_mes.glob("*.7z"))
        
        if not arquivos_7z:
            self.logger.error(f"❌ Nenhum arquivo .7z encontrado para {ano}/{mes:02d}")
            return False, []
        
        self.logger.info(f"🎯 Descompactando {len(arquivos_7z)} arquivos de {ano}/{mes:02d}")
        
        # Criar diretório de destino
        diretorio_destino_mes.mkdir(parents=True, exist_ok=True)
        
        sucessos = 0
        metadados_lista = []
        
        # Verificar quais arquivos precisam ser descompactados
        self.logger.info(f"🔍 Verificando {len(arquivos_7z)} arquivos de {ano}/{mes:02d}...")
        arquivos_para_descompactar = []
        arquivos_verificados = 0
        inicio_verificacao = time.time()
        
        # Barra de progresso melhorada para verificação
        with tqdm(
            arquivos_7z, 
            desc="🔍 Verificando", 
            unit="arq",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}"
        ) as pbar:
            for arquivo in pbar:
                arquivos_verificados += 1
                
                if self._arquivo_precisa_descompactar(arquivo):
                    arquivos_para_descompactar.append(arquivo)
                else:
                    # Arquivo já descompactado, contar como sucesso
                    metadados = self.metadados_arquivos.get(arquivo.name, self._extrair_metadados_nome(arquivo.name))
                    metadados_lista.append(metadados)
                    sucessos += 1
                
                # Calcular estatísticas de performance em tempo real
                tempo_decorrido = time.time() - inicio_verificacao
                arquivos_por_segundo = arquivos_verificados / tempo_decorrido if tempo_decorrido > 0 else 0
                
                # Atualizar informações da barra de progresso
                pbar.set_postfix({
                    'Precisam': len(arquivos_para_descompactar),
                    'OK': sucessos,
                    'Vel': f"{arquivos_por_segundo:.1f}/s",
                    'ETA': f"{(len(arquivos_7z) - arquivos_verificados) / arquivos_por_segundo:.0f}s" if arquivos_por_segundo > 0 else "--"
                })
        
        self.logger.info(f"✅ {len(arquivos_para_descompactar)} de {len(arquivos_7z)} arquivos precisam ser descompactados")
        
        if arquivos_para_descompactar:
            self.logger.info(f"🚀 Descompactando {len(arquivos_para_descompactar)} arquivos...")
            inicio_descompactacao = time.time()
            bytes_processados = 0
            falhas_descompactacao = 0
            
            # Barra de progresso melhorada para descompactação
            with tqdm(
                arquivos_para_descompactar, 
                desc="📦 Descompactando", 
                unit="arq",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}"
            ) as pbar:
                for i, arquivo in enumerate(pbar, 1):
                    # Descompactar no diretório específico do mês
                    sucesso, metadados = self.descompactar_arquivo(arquivo, diretorio_destino_mes)
                    
                    # Calcular estatísticas de performance em tempo real
                    tempo_decorrido = time.time() - inicio_descompactacao
                    arquivos_por_segundo = i / tempo_decorrido if tempo_decorrido > 0 else 0
                    
                    if sucesso:
                        sucessos += 1
                        metadados_lista.append(metadados)
                        
                        # Calcular bytes processados se disponível
                        if 'tamanho_arquivo_zip' in metadados:
                            bytes_processados += metadados['tamanho_arquivo_zip']
                    else:
                        falhas_descompactacao += 1
                    
                    # Taxa de sucesso atual
                    taxa_sucesso = ((sucessos - len([m for m in metadados_lista if 'data_processamento' not in m])) / i) * 100 if i > 0 else 100
                    
                    # Atualizar informações da barra de progresso
                    pbar.set_postfix({
                        'OK': sucessos - len([m for m in metadados_lista if 'data_processamento' not in m]),
                        'Erro': falhas_descompactacao,
                        'Taxa': f"{taxa_sucesso:.1f}%",
                        'Vel': f"{arquivos_por_segundo:.1f}/s",
                        'MB': f"{bytes_processados / 1024 / 1024:.1f}",
                        'ETA': f"{(len(arquivos_para_descompactar) - i) / arquivos_por_segundo:.0f}s" if arquivos_por_segundo > 0 else "--"
                    })
        else:
            self.logger.info("🎉 Todos os arquivos já estão descompactados!")
            inicio_descompactacao = time.time()
            bytes_processados = 0
        
        # Estatísticas finais usando o novo método
        tempo_total = time.time() - inicio_verificacao
        tempo_verificacao_total = time.time() - inicio_verificacao
        tempo_descompactacao_total = time.time() - inicio_descompactacao if len(arquivos_para_descompactar) > 0 else 0
        
        self._mostrar_estatisticas_finais(
            tempo_total=tempo_total,
            total_arquivos=len(arquivos_7z),
            arquivos_processados=len(arquivos_para_descompactar),
            sucessos=sucessos,
            falhas=0,  # descompactar_mensal não rastreia falhas separadamente
            bytes_processados=bytes_processados,
            tempo_verificacao=tempo_verificacao_total,
            tempo_descompactacao=tempo_descompactacao_total
        )
        
        # Criar indicador para a competência
        self._criar_indicador_competencia(ano, mes, sucessos, len(arquivos_7z))
        
        return sucessos > 0, metadados_lista
    
    def descompactar_arquivos_paralelo(self, 
                                      max_workers: Optional[int] = None, 
                                      ano: Optional[int] = None,
                                      ano_inicio: Optional[int] = None,
                                      ano_fim: Optional[int] = None) -> Tuple[int, int, int, List[Dict]]:
        """
        Descompacta arquivos .7z de forma paralela, opcionalmente filtrados por ano.
        Verifica se arquivos já foram descompactados para evitar reprocessamento.
        
        Args:
            max_workers: Número máximo de workers (usa self.max_workers se None)
            ano: Ano específico para filtrar
            ano_inicio: Ano inicial da faixa
            ano_fim: Ano final da faixa
            
        Returns:
            Tupla com (total de arquivos, arquivos descompactados, falhas, metadados)
        """
        import time
        
        if max_workers is None:
            max_workers = self.max_workers
        
        inicio_total = time.time()
        self.logger.info("🚀 Iniciando descompactação paralela de arquivos CAGED")
        
        # Listar arquivos .7z com filtros
        arquivos_zip = self._listar_arquivos_zip_filtrados(ano, ano_inicio, ano_fim)
        
        if not arquivos_zip:
            self.logger.info("❌ Nenhum arquivo .7z encontrado para descompactar")
            return 0, 0, 0, []
        
        self.logger.info(f"📋 Encontrados {len(arquivos_zip)} arquivos .7z para análise")
        
        # Verificar quais arquivos precisam ser descompactados EM PARALELO
        self.logger.info("🔍 Verificando arquivos que precisam ser descompactados...")
        
        arquivos_para_descompactar = []
        arquivos_verificados = 0
        inicio_verificacao = time.time()
        
        # Usar ThreadPoolExecutor para verificação paralela
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submeter verificação de todos os arquivos
            future_to_arquivo = {
                executor.submit(self._verificar_arquivo_para_descompactar, arquivo_zip): arquivo_zip 
                for arquivo_zip in arquivos_zip
            }
            
            # Processar resultados conforme completam
            with tqdm(
                total=len(arquivos_zip), 
                desc="🔍 Verificando", 
                unit="arq",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}"
            ) as pbar:
                for future in as_completed(future_to_arquivo):
                    arquivo_zip = future_to_arquivo[future]
                    arquivos_verificados += 1
                    
                    try:
                        precisa_descompactar = future.result()
                        if precisa_descompactar:
                            arquivos_para_descompactar.append(arquivo_zip)
                    except Exception as e:
                        self.logger.error(f"❌ Erro ao verificar {arquivo_zip.name}: {e}")
                    
                    # Calcular estatísticas de performance
                    tempo_decorrido = time.time() - inicio_verificacao
                    arquivos_por_segundo = arquivos_verificados / tempo_decorrido if tempo_decorrido > 0 else 0
                    
                    pbar.update(1)
                    pbar.set_postfix({
                        'Precisam': len(arquivos_para_descompactar),
                        'OK': arquivos_verificados - len(arquivos_para_descompactar),
                        'Vel': f"{arquivos_por_segundo:.1f}/s"
                    })
        
        total = len(arquivos_zip)
        descompactados = 0
        falhas = 0
        metadados_lista = []
        
        self.logger.info(f"✅ Verificação concluída: {len(arquivos_para_descompactar)} de {total} arquivos precisam ser descompactados")
        
        if not arquivos_para_descompactar:
            self.logger.info("🎉 Todos os arquivos já estão descompactados!")
            # Recuperar metadados dos arquivos já processados
            for arquivo in arquivos_zip:
                metadados = self.metadados_arquivos.get(arquivo.name, self._extrair_metadados_nome(arquivo.name))
                metadados_lista.append(metadados)
            return total, 0, 0, metadados_lista
        
        self.logger.info(f"🚀 Iniciando descompactação paralela de {len(arquivos_para_descompactar)} arquivos com {max_workers} workers")
        
        # Barra de progresso principal com estatísticas melhoradas
        inicio_descompactacao = time.time()
        bytes_processados = 0
        
        with tqdm(
            total=len(arquivos_para_descompactar),
            desc="📦 Descompactando",
            unit="arq",
            position=0,
            leave=True,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}"
        ) as pbar_principal:
            
            # Usar ThreadPoolExecutor para descompactação paralela
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submeter apenas os arquivos que precisam ser descompactados
                future_to_arquivo = {
                    executor.submit(self.descompactar_arquivo, arquivo_zip): arquivo_zip 
                    for arquivo_zip in arquivos_para_descompactar
                }
                
                # Processar resultados conforme completam
                for future in as_completed(future_to_arquivo):
                    arquivo_zip = future_to_arquivo[future]
                    try:
                        sucesso, metadados = future.result()
                        
                        # Calcular estatísticas de performance
                        tempo_descompactacao = time.time() - inicio_descompactacao
                        arquivos_por_segundo = (descompactados + falhas + 1) / tempo_descompactacao if tempo_descompactacao > 0 else 0
                        
                        if sucesso:
                            descompactados += 1
                            metadados_lista.append(metadados)
                            
                            # Calcular bytes processados se disponível
                            if 'tamanho_arquivo_zip' in metadados:
                                bytes_processados += metadados['tamanho_arquivo_zip']
                            
                            # Taxa de sucesso
                            taxa_sucesso = (descompactados / (descompactados + falhas)) * 100 if (descompactados + falhas) > 0 else 100
                            
                            pbar_principal.set_postfix({
                                'OK': descompactados,
                                'Erro': falhas,
                                'Taxa': f"{taxa_sucesso:.1f}%",
                                'Vel': f"{arquivos_por_segundo:.1f}/s",
                                'MB': f"{bytes_processados / 1024 / 1024:.1f}"
                            })
                        else:
                            falhas += 1
                            taxa_sucesso = (descompactados / (descompactados + falhas)) * 100 if (descompactados + falhas) > 0 else 0
                            
                            pbar_principal.set_postfix({
                                'OK': descompactados,
                                'Erro': falhas,
                                'Taxa': f"{taxa_sucesso:.1f}%",
                                'Vel': f"{arquivos_por_segundo:.1f}/s",
                                'MB': f"{bytes_processados / 1024 / 1024:.1f}"
                            })
                            self.logger.error(f"❌ Falha na descompactação: {arquivo_zip.name}")
                        
                        # Atualizar barra de progresso
                        pbar_principal.update(1)
                        
                    except Exception as e:
                        falhas += 1
                        tempo_descompactacao = time.time() - inicio_descompactacao
                        arquivos_por_segundo = (descompactados + falhas) / tempo_descompactacao if tempo_descompactacao > 0 else 0
                        taxa_sucesso = (descompactados / (descompactados + falhas)) * 100 if (descompactados + falhas) > 0 else 0
                        
                        pbar_principal.set_postfix({
                            'OK': descompactados,
                            'Erro': falhas,
                            'Taxa': f"{taxa_sucesso:.1f}%",
                            'Vel': f"{arquivos_por_segundo:.1f}/s",
                            'MB': f"{bytes_processados / 1024 / 1024:.1f}"
                        })
                        self.logger.error(f"💥 Exceção na descompactação de {arquivo_zip.name}: {e}")
                        pbar_principal.update(1)
        
        # Estatísticas finais usando o novo método
        tempo_total = time.time() - inicio_total
        tempo_verificacao_total = time.time() - inicio_verificacao
        tempo_descompactacao_total = time.time() - inicio_descompactacao if len(arquivos_para_descompactar) > 0 else 0
        
        self._mostrar_estatisticas_finais(
            tempo_total=tempo_total,
            total_arquivos=len(arquivos_zip),
            arquivos_processados=len(arquivos_para_descompactar),
            sucessos=descompactados,
            falhas=falhas,
            bytes_processados=bytes_processados,
            tempo_verificacao=tempo_verificacao_total,
            tempo_descompactacao=tempo_descompactacao_total
        )
        
        # Mostrar resumo por ano se houver múltiplos anos
        if ano_inicio and ano_fim and ano_inicio != ano_fim:
            self._mostrar_resumo_por_ano(arquivos_para_descompactar)
        
        # Agrupar por competência e criar indicadores
        if ano:
            self._criar_indicadores_ano(ano, metadados_lista)
        
        return total, descompactados, falhas, metadados_lista
    
    def descompactar_todos(self, ano: Optional[int] = None) -> Tuple[bool, List[Dict]]:
        """
        Descompacta todos os arquivos .7z disponíveis
        
        Args:
            ano: Ano específico ou None para todos
            
        Returns:
            Tuple[bool, List[Dict]]: (Sucesso, Lista de metadados)
        """
        import time
        # Encontrar arquivos
        padrao = f"*{ano}*.7z" if ano else "*.7z"
        arquivos_7z = list(self.diretorio_origem.rglob(padrao))
        
        if not arquivos_7z:
            self.logger.error(f"❌ Nenhum arquivo .7z encontrado" + (f" para {ano}" if ano else ""))
            return False, []
        
        self.logger.info(f"🎯 Descompactando {len(arquivos_7z)} arquivos" + (f" de {ano}" if ano else ""))
        
        sucessos = 0
        metadados_lista = []
        
        # Verificar quais arquivos precisam ser descompactados
        self.logger.info("🔍 Verificando arquivos que precisam ser descompactados...")
        arquivos_para_descompactar = []
        arquivos_verificados = 0
        inicio_verificacao = time.time()
        
        with tqdm(
            arquivos_7z, 
            desc="🔍 Verificando", 
            unit="arq",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}"
        ) as pbar:
            for arquivo in pbar:
                arquivos_verificados += 1
                
                if self._arquivo_precisa_descompactar(arquivo):
                    arquivos_para_descompactar.append(arquivo)
                else:
                    # Arquivo já descompactado, recuperar metadados
                    metadados = self.metadados_arquivos.get(arquivo.name, self._extrair_metadados_nome(arquivo.name))
                    metadados_lista.append(metadados)
                    sucessos += 1
                
                # Calcular estatísticas de performance em tempo real
                tempo_decorrido = time.time() - inicio_verificacao
                arquivos_por_segundo = arquivos_verificados / tempo_decorrido if tempo_decorrido > 0 else 0
                
                # Atualizar informações da barra de progresso
                pbar.set_postfix({
                    'Precisam': len(arquivos_para_descompactar),
                    'OK': sucessos,
                    'Vel': f"{arquivos_por_segundo:.1f}/s",
                    'ETA': f"{(len(arquivos_7z) - arquivos_verificados) / arquivos_por_segundo:.0f}s" if arquivos_por_segundo > 0 else "--"
                })
        
        self.logger.info(f"✅ Verificação concluída: {len(arquivos_para_descompactar)} de {len(arquivos_7z)} arquivos precisam ser descompactados")
        
        if not arquivos_para_descompactar:
            self.logger.info("🎉 Todos os arquivos já estão descompactados!")
        else:
            self.logger.info(f"🚀 Iniciando descompactação de {len(arquivos_para_descompactar)} arquivos")
            
            # Descompactar apenas os arquivos necessários
            inicio_descompactacao = time.time()
            bytes_processados = 0
            falhas_descompactacao = 0
            
            with tqdm(
                arquivos_para_descompactar, 
                desc="📦 Descompactando", 
                unit="arq",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}"
            ) as pbar:
                for i, arquivo in enumerate(pbar, 1):
                    sucesso, metadados = self.descompactar_arquivo(arquivo)
                    
                    # Calcular estatísticas de performance em tempo real
                    tempo_decorrido = time.time() - inicio_descompactacao
                    arquivos_por_segundo = i / tempo_decorrido if tempo_decorrido > 0 else 0
                    
                    if sucesso:
                        sucessos += 1
                        metadados_lista.append(metadados)
                        
                        # Calcular bytes processados se disponível
                        if 'tamanho_arquivo_zip' in metadados:
                            bytes_processados += metadados['tamanho_arquivo_zip']
                    else:
                        falhas_descompactacao += 1
                    
                    # Taxa de sucesso atual
                    taxa_sucesso = ((sucessos - len([m for m in metadados_lista if 'data_processamento' not in m])) / i) * 100 if i > 0 else 100
                    
                    # Atualizar informações da barra de progresso
                    pbar.set_postfix({
                        'OK': sucessos - len([m for m in metadados_lista if 'data_processamento' not in m]),
                        'Erro': falhas_descompactacao,
                        'Taxa': f"{taxa_sucesso:.1f}%",
                        'Vel': f"{arquivos_por_segundo:.1f}/s",
                        'MB': f"{bytes_processados / 1024 / 1024:.1f}",
                        'ETA': f"{(len(arquivos_para_descompactar) - i) / arquivos_por_segundo:.0f}s" if arquivos_por_segundo > 0 else "--"
                    })
        
        # Estatísticas finais usando o novo método
        tempo_total = time.time() - inicio_verificacao
        tempo_verificacao_total = time.time() - inicio_verificacao
        tempo_descompactacao_total = time.time() - inicio_descompactacao if len(arquivos_para_descompactar) > 0 else 0
        
        self._mostrar_estatisticas_finais(
            tempo_total=tempo_total,
            total_arquivos=len(arquivos_7z),
            arquivos_processados=len(arquivos_para_descompactar),
            sucessos=sucessos,
            falhas=falhas_descompactacao,
            bytes_processados=bytes_processados,
            tempo_verificacao=tempo_verificacao_total,
            tempo_descompactacao=tempo_descompactacao_total
        )
        
        # Agrupar por competência e criar indicadores
        if ano:
            self._criar_indicadores_ano(ano, metadados_lista)
        
        return sucessos > 0, metadados_lista
    
    def listar_arquivos_descompactados(self, ano: Optional[int] = None) -> List[Path]:
        """
        Lista arquivos já descompactados
        
        Args:
            ano: Ano específico ou None para todos
            
        Returns:
            List[Path]: Lista de arquivos descompactados
        """
        if ano:
            caminho = self.diretorio_destino / str(ano)
            if not caminho.exists():
                return []
            return list(caminho.glob("*.txt")) + list(caminho.glob("*.csv"))
        else:
            arquivos = []
            for subdir in self.diretorio_destino.iterdir():
                if subdir.is_dir():
                    arquivos.extend(subdir.glob("*.txt"))
                    arquivos.extend(subdir.glob("*.csv"))
            return arquivos
    
    def _extrair_ano_do_nome(self, nome_arquivo: str) -> int:
        """
        Extrai ano do nome do arquivo
        
        Args:
            nome_arquivo: Nome do arquivo
            
        Returns:
            int: Ano extraído ou ano atual como fallback
        """
        import re
        from datetime import datetime
        
        matches = re.findall(r'(20\d{2})', nome_arquivo)
        if matches:
            return int(matches[0])
        else:
            return datetime.now().year
    
    def _extrair_metadados_nome(self, nome_arquivo: str) -> Dict:
        """
        Extrai metadados do nome do arquivo
        
        Args:
            nome_arquivo: Nome do arquivo
            
        Returns:
            Dict: Metadados extraídos
        """
        metadados = {
            "nome_arquivo": nome_arquivo,
            "data_processamento": datetime.now().isoformat()
        }
        
        # Extrair ano
        ano_match = re.search(r'(20\d{2})', nome_arquivo)
        if ano_match:
            metadados["ano"] = ano_match.group(1)
        
        # Extrair mês
        mes_match = re.search(r'(20\d{2})(\d{2})', nome_arquivo)
        if mes_match and 1 <= int(mes_match.group(2)) <= 12:
            metadados["mes"] = mes_match.group(2)
        
        # Tentar identificar UF
        ufs = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", 
               "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", 
               "RO", "RR", "RS", "SC", "SE", "SP", "TO"]
        
        for uf in ufs:
            if uf in nome_arquivo:
                metadados["uf"] = uf
                break
        
        # Identificar tipo de arquivo
        if "MOVIMENTACAO" in nome_arquivo.upper():
            metadados["tipo"] = "movimentacao"
        elif "EXCLUSAO" in nome_arquivo.upper():
            metadados["tipo"] = "exclusao"
        elif "FORA_PRAZO" in nome_arquivo.upper() or "FORAPRAZO" in nome_arquivo.upper():
            metadados["tipo"] = "fora_prazo"
        else:
            metadados["tipo"] = "outros"
            
        # Criar competência formatada
        if "ano" in metadados and "mes" in metadados:
            metadados["competencia"] = f"{metadados['ano']}-{int(metadados['mes']):02d}"
        
        return metadados
    
    def _verificar_arquivo_zip(self, caminho_arquivo_zip: Path) -> Dict[str, Any]:
        """
        Verifica informações do arquivo .7z com hash MD5 e timestamps
        
        Args:
            caminho_arquivo_zip: Caminho do arquivo .7z
            
        Returns:
            Dict: Informações do arquivo (existe, tamanho, data_modificacao, hash_md5)
        """
        info = {
            "existe": False,
            "tamanho": 0,
            "data_modificacao": None,
            "hash_md5": None
        }
        
        if caminho_arquivo_zip.exists():
            stat = caminho_arquivo_zip.stat()
            info["existe"] = True
            info["tamanho"] = stat.st_size
            info["data_modificacao"] = stat.st_mtime
            
            # Calcular hash MD5
            try:
                with open(caminho_arquivo_zip, 'rb') as f:
                    hash_md5 = hashlib.md5()
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)
                    info["hash_md5"] = hash_md5.hexdigest()
            except Exception as e:
                self.logger.debug(f"Erro ao calcular hash MD5 de {caminho_arquivo_zip}: {e}")
        
        return info
    
    def _verificar_arquivo_descompactado(self, caminho_arquivo_zip: Path) -> Dict[str, Any]:
        """
        Verifica se o arquivo já foi descompactado e valida integridade
        
        Args:
            caminho_arquivo_zip: Caminho do arquivo .7z
            
        Returns:
            Dict: Informações do arquivo descompactado
        """
        info = {
            "existe": False,
            "arquivos_txt": [],
            "arquivos_csv": [],
            "tamanho_total": 0,
            "data_modificacao": None
        }
        
        # Determinar diretório de destino
        ano = self._extrair_ano_do_nome(caminho_arquivo_zip.name)
        diretorio_destino = self.diretorio_destino / str(ano)
        
        if diretorio_destino.exists():
            # Procurar por arquivos .txt e .csv que correspondam ao arquivo .7z
            nome_base = caminho_arquivo_zip.stem  # Nome sem extensão
            
            # Busca mais específica baseada no nome do arquivo
            arquivos_txt = self._encontrar_arquivos_correspondentes(diretorio_destino, nome_base, ".txt")
            arquivos_csv = self._encontrar_arquivos_correspondentes(diretorio_destino, nome_base, ".csv")
            
            if arquivos_txt or arquivos_csv:
                info["existe"] = True
                info["arquivos_txt"] = [str(arq) for arq in arquivos_txt]
                info["arquivos_csv"] = [str(arq) for arq in arquivos_csv]
                
                # Calcular tamanho total e data de modificação mais recente
                tamanho_total = 0
                data_mais_recente = 0
                
                for arquivo in arquivos_txt + arquivos_csv:
                    if arquivo.exists():
                        stat = arquivo.stat()
                        tamanho_total += stat.st_size
                        data_mais_recente = max(data_mais_recente, stat.st_mtime)
                
                info["tamanho_total"] = tamanho_total
                info["data_modificacao"] = data_mais_recente
        
        return info
    
    def _encontrar_arquivos_correspondentes(self, diretorio: Path, nome_base: str, extensao: str) -> List[Path]:
        """
        Encontra arquivos que correspondem ao arquivo .7z baseado em padrões específicos do CAGED
        
        Args:
            diretorio: Diretório onde procurar
            nome_base: Nome base do arquivo .7z (sem extensão)
            extensao: Extensão a procurar (.txt ou .csv)
            
        Returns:
            List[Path]: Lista de arquivos correspondentes válidos
        """
        arquivos_encontrados = []
        
        if not diretorio.exists():
            return arquivos_encontrados
        
        # Padrões de busca específicos para CAGED
        padroes_busca = [
            f"{nome_base}*{extensao}",  # Correspondência direta
            f"*{nome_base}*{extensao}",  # Nome contido
            f"*{extensao}"  # Todos os arquivos da extensão (fallback)
        ]
        
        # Extrair metadados do nome base para busca mais inteligente
        metadados_base = self._extrair_metadados_nome(nome_base)
        
        for padrao in padroes_busca:
            arquivos_padrao = list(diretorio.glob(padrao))
            
            for arquivo in arquivos_padrao:
                if self._validar_arquivo_correspondente(arquivo, nome_base, metadados_base):
                    arquivos_encontrados.append(arquivo)
        
        # Remover duplicatas mantendo ordem
        arquivos_unicos = []
        for arquivo in arquivos_encontrados:
            if arquivo not in arquivos_unicos:
                arquivos_unicos.append(arquivo)
        
        return arquivos_unicos
    
    def _validar_arquivo_correspondente(self, arquivo: Path, nome_base: str, metadados_base: Dict) -> bool:
        """
        Valida se um arquivo corresponde ao arquivo .7z baseado em critérios específicos
        
        Args:
            arquivo: Arquivo a validar
            nome_base: Nome base do arquivo .7z
            metadados_base: Metadados extraídos do nome base
            
        Returns:
            bool: True se o arquivo é válido
        """
        if not arquivo.exists():
            return False
        
        # Verificar se o arquivo tem tamanho > 0
        if arquivo.stat().st_size == 0:
            self.logger.debug(f"📄 Arquivo vazio ignorado: {arquivo.name}")
            return False
        
        # Extrair metadados do arquivo encontrado
        metadados_arquivo = self._extrair_metadados_nome(arquivo.name)
        
        # Verificar correspondência de ano
        if "ano" in metadados_base and "ano" in metadados_arquivo:
            if metadados_base["ano"] != metadados_arquivo["ano"]:
                return False
        
        # Verificar correspondência de mês (se disponível)
        if "mes" in metadados_base and "mes" in metadados_arquivo:
            if metadados_base["mes"] != metadados_arquivo["mes"]:
                return False
        
        # Verificar correspondência de UF (se disponível)
        if "uf" in metadados_base and "uf" in metadados_arquivo:
            if metadados_base["uf"] != metadados_arquivo["uf"]:
                return False
        
        # Verificar correspondência de tipo (se disponível)
        if "tipo" in metadados_base and "tipo" in metadados_arquivo:
            if metadados_base["tipo"] != metadados_arquivo["tipo"]:
                # Permitir algumas variações de tipo
                tipos_equivalentes = {
                    "movimentacao": ["movimentacao", "mov"],
                    "exclusao": ["exclusao", "excl"],
                    "fora_prazo": ["fora_prazo", "foraprazo", "fp"]
                }
                
                tipo_base = metadados_base["tipo"]
                tipo_arquivo = metadados_arquivo["tipo"]
                
                correspondencia_encontrada = False
                for tipo_principal, variantes in tipos_equivalentes.items():
                    if tipo_base in variantes and tipo_arquivo in variantes:
                        correspondencia_encontrada = True
                        break
                
                if not correspondencia_encontrada:
                    return False
        
        self.logger.debug(f"✅ Arquivo válido encontrado: {arquivo.name}")
        return True
    
    def _arquivo_precisa_descompactar(self, arquivo_7z: Path) -> bool:
        """
        Verifica se o arquivo precisa ser descompactado usando verificação inteligente
        
        Args:
            arquivo_7z: Caminho do arquivo .7z
            
        Returns:
            bool: True se precisa descompactar
        """
        info_zip = self._verificar_arquivo_zip(arquivo_7z)
        info_descompactado = self._verificar_arquivo_descompactado(arquivo_7z)
        
        # Se o arquivo .7z não existe, não precisa descompactar
        if not info_zip["existe"]:
            self.logger.warning(f"❌ Arquivo .7z não encontrado: {arquivo_7z}")
            return False
        
        # Se não há arquivos descompactados, precisa descompactar
        if not info_descompactado["existe"]:
            self.logger.info(f"📦 Arquivo {arquivo_7z.name} não foi descompactado ainda")
            return True
        
        # Verificar se o arquivo .7z foi modificado após a descompactação
        if info_zip["data_modificacao"] and info_descompactado["data_modificacao"]:
            if info_zip["data_modificacao"] > info_descompactado["data_modificacao"]:
                self.logger.info(f"🔄 Arquivo {arquivo_7z.name} foi modificado após descompactação")
                return True
        
        # Verificar se os arquivos descompactados existem e têm tamanho > 0
        arquivos_descompactados = info_descompactado["arquivos_txt"] + info_descompactado["arquivos_csv"]
        if not arquivos_descompactados:
            self.logger.info(f"📂 Nenhum arquivo descompactado encontrado para {arquivo_7z.name}")
            return True
        
        # Verificar se todos os arquivos têm tamanho > 0
        for arquivo_path in arquivos_descompactados:
            arquivo = Path(arquivo_path)
            if not arquivo.exists() or arquivo.stat().st_size == 0:
                self.logger.info(f"📄 Arquivo vazio ou não encontrado: {arquivo.name}")
                return True
        
        self.logger.debug(f"✅ Arquivo {arquivo_7z.name} já está descompactado e atualizado")
        return False
    
    def _arquivo_ja_descompactado(self, arquivo_7z: Path) -> bool:
        """
        Verifica se arquivo já foi descompactado (método legado mantido para compatibilidade)
        
        Args:
            arquivo_7z: Caminho do arquivo .7z
            
        Returns:
            bool: True se já foi descompactado
        """
        # Usar o novo método mais robusto
        return not self._arquivo_precisa_descompactar(arquivo_7z)
    
    def _verificar_arquivo_para_descompactar(self, caminho_arquivo_zip: Path) -> bool:
        """
        Verifica se um arquivo .7z precisa ser descompactado (versão para uso paralelo).
        
        Args:
            caminho_arquivo_zip: Caminho do arquivo .7z
            
        Returns:
            True se o arquivo precisa ser descompactado, False caso contrário
        """
        # Verificar se o arquivo .7z existe
        info_zip = self._verificar_arquivo_zip(caminho_arquivo_zip)
        if not info_zip["existe"]:
            self.logger.warning(f"Arquivo .7z não encontrado: {caminho_arquivo_zip}")
            return False
        
        # Verificar se já foi descompactado
        info_descompactado = self._verificar_arquivo_descompactado(caminho_arquivo_zip)
        
        # Se não existe arquivo descompactado, precisa descompactar
        if not info_descompactado["existe"]:
            return True
        
        # Se existe arquivo descompactado, verificar se é mais recente que o .7z
        if info_descompactado["data_modificacao"] and info_zip["data_modificacao"]:
            if info_descompactado["data_modificacao"] < info_zip["data_modificacao"]:
                return True
        
        # Se chegou aqui, o arquivo já está descompactado e atualizado
        return False
    
    def _listar_arquivos_zip_filtrados(self, 
                                      ano: Optional[int] = None,
                                      ano_inicio: Optional[int] = None,
                                      ano_fim: Optional[int] = None) -> List[Path]:
        """
        Lista arquivos .7z na pasta files-zip, opcionalmente filtrados por ano.
        
        Args:
            ano: Ano específico para filtrar
            ano_inicio: Ano inicial da faixa
            ano_fim: Ano final da faixa
            
        Returns:
            Lista de caminhos dos arquivos .7z encontrados e filtrados
        """
        # Encontrar todos os arquivos .7z
        arquivos_zip = list(self.diretorio_origem.rglob("*.7z"))
        
        if not arquivos_zip:
            return []
        
        # Aplicar filtros de ano
        if ano:
            # Filtrar por ano específico
            arquivos_filtrados = []
            for arquivo in arquivos_zip:
                ano_arquivo = self._extrair_ano_do_nome(arquivo.name)
                if ano_arquivo == ano:
                    arquivos_filtrados.append(arquivo)
            return arquivos_filtrados
        
        elif ano_inicio and ano_fim:
            # Filtrar por faixa de anos
            arquivos_filtrados = []
            for arquivo in arquivos_zip:
                ano_arquivo = self._extrair_ano_do_nome(arquivo.name)
                if ano_inicio <= ano_arquivo <= ano_fim:
                    arquivos_filtrados.append(arquivo)
            return arquivos_filtrados
        
        elif ano_inicio:
            # Filtrar por ano inicial (sem fim)
            arquivos_filtrados = []
            for arquivo in arquivos_zip:
                ano_arquivo = self._extrair_ano_do_nome(arquivo.name)
                if ano_arquivo >= ano_inicio:
                    arquivos_filtrados.append(arquivo)
            return arquivos_filtrados
        
        elif ano_fim:
            # Filtrar por ano final (sem início)
            arquivos_filtrados = []
            for arquivo in arquivos_zip:
                ano_arquivo = self._extrair_ano_do_nome(arquivo.name)
                if ano_arquivo <= ano_fim:
                    arquivos_filtrados.append(arquivo)
            return arquivos_filtrados
        
        # Sem filtros, retornar todos
        return arquivos_zip
    
    def _criar_barra_progresso_avancada(self, 
                                       iterable, 
                                       desc: str, 
                                       operacao: str = "processando",
                                       mostrar_eta: bool = True,
                                       mostrar_velocidade: bool = True,
                                       mostrar_bytes: bool = False) -> tqdm:
        """
        Cria uma barra de progresso padronizada com informações detalhadas
        
        Args:
            iterable: Iterável para processar
            desc: Descrição da operação
            operacao: Tipo de operação (verificando, descompactando, etc.)
            mostrar_eta: Se deve mostrar ETA
            mostrar_velocidade: Se deve mostrar velocidade
            mostrar_bytes: Se deve mostrar informações de bytes
            
        Returns:
            tqdm: Objeto da barra de progresso configurada
        """
        # Formato personalizado da barra com mais informações
        bar_format = "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
        
        if mostrar_eta or mostrar_velocidade or mostrar_bytes:
            bar_format += " {postfix}"
        
        return tqdm(
            iterable,
            desc=desc,
            unit="arq",
            bar_format=bar_format,
            dynamic_ncols=True,  # Ajusta automaticamente à largura do terminal
            smoothing=0.1,       # Suavização da velocidade
            miniters=1,          # Atualização mínima
            maxinterval=0.5      # Intervalo máximo de atualização
        )
    
    def _atualizar_estatisticas_progresso(self, 
                                         pbar: tqdm,
                                         tempo_inicio: float,
                                         contador_atual: int,
                                         total_itens: int,
                                         sucessos: int = 0,
                                         falhas: int = 0,
                                         bytes_processados: int = 0,
                                         mostrar_eta: bool = True,
                                         mostrar_velocidade: bool = True,
                                         mostrar_bytes: bool = False) -> None:
        """
        Atualiza as estatísticas da barra de progresso de forma padronizada
        
        Args:
            pbar: Objeto da barra de progresso
            tempo_inicio: Timestamp do início da operação
            contador_atual: Contador atual de itens processados
            total_itens: Total de itens a processar
            sucessos: Número de sucessos
            falhas: Número de falhas
            bytes_processados: Bytes processados
            mostrar_eta: Se deve mostrar ETA
            mostrar_velocidade: Se deve mostrar velocidade
            mostrar_bytes: Se deve mostrar informações de bytes
        """
        import time
        
        # Calcular estatísticas básicas
        tempo_decorrido = time.time() - tempo_inicio
        itens_por_segundo = contador_atual / tempo_decorrido if tempo_decorrido > 0 else 0
        
        # Preparar informações para exibir
        postfix_info = {}
        
        # Adicionar contadores se houver sucessos/falhas
        if sucessos > 0 or falhas > 0:
            postfix_info['OK'] = sucessos
            if falhas > 0:
                postfix_info['Erro'] = falhas
                # Calcular taxa de sucesso
                total_processados = sucessos + falhas
                if total_processados > 0:
                    taxa_sucesso = (sucessos / total_processados) * 100
                    postfix_info['Taxa'] = f"{taxa_sucesso:.1f}%"
        
        # Adicionar velocidade
        if mostrar_velocidade and itens_por_segundo > 0:
            postfix_info['Vel'] = f"{itens_por_segundo:.1f}/s"
        
        # Adicionar informações de bytes
        if mostrar_bytes and bytes_processados > 0:
            mb_processados = bytes_processados / 1024 / 1024
            postfix_info['MB'] = f"{mb_processados:.1f}"
            
            # Velocidade de transferência
            if tempo_decorrido > 0:
                mb_por_segundo = mb_processados / tempo_decorrido
                if mb_por_segundo > 1:
                    postfix_info['MB/s'] = f"{mb_por_segundo:.1f}"
        
        # Adicionar ETA
        if mostrar_eta and itens_por_segundo > 0:
            itens_restantes = total_itens - contador_atual
            eta_segundos = itens_restantes / itens_por_segundo
            if eta_segundos > 0:
                if eta_segundos < 60:
                    postfix_info['ETA'] = f"{eta_segundos:.0f}s"
                elif eta_segundos < 3600:
                    postfix_info['ETA'] = f"{eta_segundos/60:.1f}m"
                else:
                    postfix_info['ETA'] = f"{eta_segundos/3600:.1f}h"
        
        # Atualizar a barra de progresso
        if postfix_info:
            pbar.set_postfix(postfix_info)
    
    def _mostrar_resumo_por_ano(self, arquivos_processados: List[Path]) -> None:
        """
        Mostra um resumo dos arquivos processados organizados por ano.
        
        Args:
            arquivos_processados: Lista de arquivos que foram processados
        """
        from collections import defaultdict
        
        # Agrupar arquivos por ano
        arquivos_por_ano = defaultdict(list)
        for arquivo in arquivos_processados:
            ano = self._extrair_ano_do_nome(arquivo.name)
            if ano:
                arquivos_por_ano[str(ano)].append(arquivo)
        
        if len(arquivos_por_ano) > 1:
            self.logger.info("📊 Resumo por ano:")
            for ano in sorted(arquivos_por_ano.keys()):
                quantidade = len(arquivos_por_ano[ano])
                self.logger.info(f"   {ano}: {quantidade} arquivo{'s' if quantidade > 1 else ''}")
    
    def _criar_indicador_competencia(self, ano: int, mes: int, arquivos_processados: int, total_arquivos: int) -> Indicador:
        """
        Cria um indicador para a competência processada
        
        Args:
            ano: Ano da competência
            mes: Mês da competência
            arquivos_processados: Quantidade de arquivos processados
            total_arquivos: Total de arquivos
            
        Returns:
            Indicador: Objeto com informações da competência
        """
        competencia = f"{ano}-{mes:02d}"
        taxa_sucesso = (arquivos_processados / total_arquivos) * 100 if total_arquivos > 0 else 0
        
        indicador = Indicador(
            id=0,  # ID temporário, deve ser atribuído na persistência
            cnpj="",  # Não aplicável neste contexto
            competencia=competencia,
            nome_indicador="taxa_descompactacao",
            valor=taxa_sucesso
        )
        
        print(f"📊 Indicador criado: {indicador}")
        return indicador
    
    def _criar_indicadores_ano(self, ano: int, metadados: List[Dict]) -> List[Indicador]:
        """
        Cria indicadores para o ano processado
        
        Args:
            ano: Ano dos dados
            metadados: Lista de metadados dos arquivos
            
        Returns:
            List[Indicador]: Lista de indicadores criados
        """
        # Agrupar por mês
        meses = {}
        for meta in metadados:
            if "mes" in meta:
                mes = meta["mes"]
                if mes not in meses:
                    meses[mes] = {"total": 0, "processados": 0}
                
                meses[mes]["total"] += 1
                if "arquivos_extraidos" in meta:
                    meses[mes]["processados"] += 1
        
        # Criar indicadores por mês
        indicadores = []
        for mes, dados in meses.items():
            indicador = self._criar_indicador_competencia(
                ano, mes, dados["processados"], dados["total"]
            )
            indicadores.append(indicador)
        
        return indicadores
    
    def _mostrar_estatisticas_finais(self, 
                                   tempo_total: float,
                                   total_arquivos: int,
                                   arquivos_processados: int,
                                   sucessos: int,
                                   falhas: int,
                                   bytes_processados: int = 0,
                                   tempo_verificacao: float = 0,
                                   tempo_descompactacao: float = 0) -> None:
        """
        Mostra estatísticas finais detalhadas da operação
        
        Args:
            tempo_total: Tempo total da operação
            total_arquivos: Total de arquivos analisados
            arquivos_processados: Arquivos que foram processados
            sucessos: Número de sucessos
            falhas: Número de falhas
            bytes_processados: Bytes processados
            tempo_verificacao: Tempo gasto na verificação
            tempo_descompactacao: Tempo gasto na descompactação
        """
        self.logger.info("\n" + "="*60)
        self.logger.info("📊 ESTATÍSTICAS FINAIS DA DESCOMPACTAÇÃO")
        self.logger.info("="*60)
        
        # Estatísticas gerais
        self.logger.info(f"⏱️  Tempo total: {tempo_total:.1f}s")
        self.logger.info(f"📁 Total de arquivos analisados: {total_arquivos}")
        self.logger.info(f"🔄 Arquivos processados: {arquivos_processados}")
        self.logger.info(f"✅ Sucessos: {sucessos}")
        
        if falhas > 0:
            self.logger.info(f"❌ Falhas: {falhas}")
            taxa_sucesso = (sucessos / (sucessos + falhas)) * 100 if (sucessos + falhas) > 0 else 0
            self.logger.info(f"📈 Taxa de sucesso: {taxa_sucesso:.1f}%")
        
        # Estatísticas de performance
        if tempo_total > 0:
            arquivos_por_segundo = total_arquivos / tempo_total
            self.logger.info(f"⚡ Velocidade média: {arquivos_por_segundo:.1f} arquivos/s")
        
        # Estatísticas de verificação
        if tempo_verificacao > 0:
            velocidade_verificacao = total_arquivos / tempo_verificacao
            self.logger.info(f"🔍 Verificação: {tempo_verificacao:.1f}s ({velocidade_verificacao:.1f} arq/s)")
        
        # Estatísticas de descompactação
        if tempo_descompactacao > 0 and arquivos_processados > 0:
            velocidade_descompactacao = arquivos_processados / tempo_descompactacao
            self.logger.info(f"📦 Descompactação: {tempo_descompactacao:.1f}s ({velocidade_descompactacao:.1f} arq/s)")
        
        # Estatísticas de dados
        if bytes_processados > 0:
            mb_processados = bytes_processados / 1024 / 1024
            gb_processados = mb_processados / 1024
            
            if gb_processados >= 1:
                self.logger.info(f"💾 Dados processados: {gb_processados:.2f} GB")
            else:
                self.logger.info(f"💾 Dados processados: {mb_processados:.1f} MB")
            
            if tempo_total > 0:
                mb_por_segundo = mb_processados / tempo_total
                if mb_por_segundo >= 1:
                    self.logger.info(f"🚀 Velocidade de dados: {mb_por_segundo:.1f} MB/s")
        
        # Eficiência
        if arquivos_processados > 0 and total_arquivos > 0:
            eficiencia = (arquivos_processados / total_arquivos) * 100
            self.logger.info(f"📊 Eficiência: {eficiencia:.1f}% (arquivos que precisaram processamento)")
        
        self.logger.info("="*60 + "\n")
    
    def limpar_arquivos_temporarios(self, ano: Optional[int] = None):
        """
        Remove arquivos temporários e de cache
        
        Args:
            ano: Ano específico ou None para todos
        """
        print("🧹 Limpando arquivos temporários...")
        
        if ano:
            caminhos = [self.diretorio_destino / str(ano)]
        else:
            caminhos = [self.diretorio_destino]
        
        for caminho in caminhos:
            if caminho.exists():
                # Remover arquivos temporários
                for temp_file in caminho.rglob("*.tmp"):
                    temp_file.unlink()
                    print(f"🗑️  Removido: {temp_file.name}")
    
    def gerar_relatorio_descompactacao(self, ano: Optional[int] = None) -> Dict:
        """
        Gera relatório sobre os arquivos descompactados
        
        Args:
            ano: Ano específico ou None para todos
            
        Returns:
            Dict: Relatório com estatísticas
        """
        arquivos = self.listar_arquivos_descompactados(ano)
        
        # Agrupar por tipo e extensão
        tipos = {}
        extensoes = {}
        competencias = {}
        
        for arquivo in arquivos:
            # Contar por extensão
            ext = arquivo.suffix.lower()
            extensoes[ext] = extensoes.get(ext, 0) + 1
            
            # Tentar extrair metadados do nome
            meta = self._extrair_metadados_nome(arquivo.name)
            
            # Contar por tipo
            tipo = meta.get("tipo", "desconhecido")
            tipos[tipo] = tipos.get(tipo, 0) + 1
            
            # Contar por competência
            comp = meta.get("competencia", "desconhecida")
            if comp not in competencias:
                competencias[comp] = 0
            competencias[comp] += 1
        
        # Gerar relatório
        return {
            "total_arquivos": len(arquivos),
            "por_extensao": extensoes,
            "por_tipo": tipos,
            "por_competencia": competencias
        }


# Função auxiliar para teste
def testar_descompactador():
    """
    Teste rápido do descompactador
    """
    print("🧪 Testando descompactador...")
    
    descompactador = DescompactadorCaged()
    
    # Listar arquivos .7z disponíveis
    arquivos_7z = list(descompactador.diretorio_origem.rglob("*.7z"))
    print(f"📋 Arquivos .7z encontrados: {len(arquivos_7z)}")
    
    # Listar arquivos já descompactados
    arquivos_descompactados = descompactador.listar_arquivos_descompactados()
    print(f"📁 Arquivos já descompactados: {len(arquivos_descompactados)}")
    
    # Gerar relatório
    if arquivos_descompactados:
        relatorio = descompactador.gerar_relatorio_descompactacao()
        print(f"📊 Relatório de descompactação: {relatorio}")
    
    return len(arquivos_7z) > 0


if __name__ == "__main__":
    testar_descompactador()