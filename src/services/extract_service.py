#!/usr/bin/env python3
"""
Descompactador de Arquivos CAGED
Módulo para descompactar arquivos .7z baixados do CAGED
"""

import re
import hashlib
import logging
import json
import time
from pathlib import Path
from typing import List, Optional, Dict, Tuple, Any
from tqdm import tqdm
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict, deque

# Importação das entidades
from src.entities.movimentacao import Movimentacao
from src.entities.saldo_mensal import SaldoMensal
from src.entities.exclusao import Exclusao
from src.entities.movimentacao_fora_prazo import MovimentacaoForaPrazo
from src.entities.indicador import Indicador

import logging

# Usar o logger centralizado configurado no main.py
logger = logging.getLogger("caged")

# Importar sistema de métricas
from ..utils.metrics import record_operation


class MonitorDescompactacao:
    """
    Sistema de monitoramento para o descompactador CAGED
    Implementa métricas de tempo, contadores de sucesso/falha e alertas
    """
    
    def __init__(self, logger: logging.Logger, usar_emojis: bool = True):
        """
        Inicializa o sistema de monitoramento
        
        Args:
            logger: Logger para registrar eventos
            usar_emojis: Se deve usar emojis nas mensagens
        """
        self.logger = logger
        self.usar_emojis = usar_emojis
        
        # Métricas de tempo
        self.tempos_processamento = defaultdict(list)
        self.inicio_operacao = None
        self.fim_operacao = None
        
        # Contadores de sucesso/falha
        self.contadores = {
            'total_arquivos': 0,
            'sucessos': 0,
            'falhas': 0,
            'bytes_processados': 0,
            'arquivos_verificados': 0,
            'arquivos_ignorados': 0
        }
        
        # Histórico de performance (últimas 100 operações)
        self.historico_performance = deque(maxlen=100)
        
        # Alertas e problemas
        self.alertas = []
        self.problemas_recorrentes = defaultdict(int)
        
        # Limites para alertas
        self.limites = {
            'taxa_falha_critica': 20.0,  # % de falhas
            'tempo_operacao_lento': 300.0,  # segundos
            'velocidade_minima': 0.5,  # arquivos/segundo
            'problemas_recorrentes': 3  # número de ocorrências
        }
    
    def iniciar_operacao(self, nome_operacao: str, total_itens: int = 0):
        """
        Inicia o monitoramento de uma operação
        
        Args:
            nome_operacao: Nome da operação sendo monitorada
            total_itens: Total de itens a serem processados
        """
        self.inicio_operacao = time.time()
        self.operacao_atual = nome_operacao
        self.contadores['total_arquivos'] = total_itens
        
        emoji = "🚀" if self.usar_emojis else ""
        self.logger.info(f"{emoji} Iniciando monitoramento: {nome_operacao} ({total_itens} itens)")
    
    def finalizar_operacao(self):
        """
        Finaliza o monitoramento da operação atual
        """
        if self.inicio_operacao is None:
            return
        
        self.fim_operacao = time.time()
        tempo_total = self.fim_operacao - self.inicio_operacao
        
        # Registrar no histórico
        self.historico_performance.append({
            'operacao': self.operacao_atual,
            'tempo_total': tempo_total,
            'sucessos': self.contadores['sucessos'],
            'falhas': self.contadores['falhas'],
            'timestamp': datetime.now()
        })
        
        # Verificar alertas
        self._verificar_alertas(tempo_total)
        
        # Gerar relatório final
        self._gerar_relatorio_final(tempo_total)
    
    def registrar_sucesso(self, tempo_processamento: float = 0, bytes_processados: int = 0):
        """
        Registra um sucesso na operação
        
        Args:
            tempo_processamento: Tempo gasto no processamento
            bytes_processados: Bytes processados
        """
        self.contadores['sucessos'] += 1
        self.contadores['bytes_processados'] += bytes_processados
        
        if tempo_processamento > 0:
            self.tempos_processamento[self.operacao_atual].append(tempo_processamento)
    
    def registrar_falha(self, erro: str, tipo_erro: str = "geral"):
        """
        Registra uma falha na operação
        
        Args:
            erro: Descrição do erro
            tipo_erro: Tipo/categoria do erro
        """
        self.contadores['falhas'] += 1
        self.problemas_recorrentes[tipo_erro] += 1
        
        # Verificar se é um problema recorrente
        if self.problemas_recorrentes[tipo_erro] >= self.limites['problemas_recorrentes']:
            self._adicionar_alerta(
                f"Problema recorrente detectado: {tipo_erro} ({self.problemas_recorrentes[tipo_erro]} ocorrências)",
                "warning"
            )
    
    def registrar_arquivo_ignorado(self):
        """Registra um arquivo que foi ignorado (já processado)"""
        self.contadores['arquivos_ignorados'] += 1
    
    def obter_metricas_tempo(self) -> Dict[str, float]:
        """
        Obtém métricas de tempo da operação atual
        
        Returns:
            Dicionário com métricas de tempo
        """
        if not self.inicio_operacao:
            return {}
        
        tempo_atual = time.time() - self.inicio_operacao
        total_processados = self.contadores['sucessos'] + self.contadores['falhas']
        
        metricas = {
            'tempo_decorrido': tempo_atual,
            'velocidade_atual': total_processados / tempo_atual if tempo_atual > 0 else 0,
            'eta_estimado': 0
        }
        
        # Calcular ETA se houver progresso
        if total_processados > 0 and self.contadores['total_arquivos'] > 0:
            restantes = self.contadores['total_arquivos'] - total_processados
            if restantes > 0 and metricas['velocidade_atual'] > 0:
                metricas['eta_estimado'] = restantes / metricas['velocidade_atual']
        
        return metricas
    
    def obter_taxa_sucesso(self) -> float:
        """
        Calcula a taxa de sucesso atual
        
        Returns:
            Taxa de sucesso em percentual
        """
        total = self.contadores['sucessos'] + self.contadores['falhas']
        if total == 0:
            return 100.0
        return (self.contadores['sucessos'] / total) * 100
    
    def _verificar_alertas(self, tempo_total: float):
        """
        Verifica condições para gerar alertas
        
        Args:
            tempo_total: Tempo total da operação
        """
        # Verificar taxa de falha
        taxa_sucesso = self.obter_taxa_sucesso()
        taxa_falha = 100 - taxa_sucesso
        
        if taxa_falha > self.limites['taxa_falha_critica']:
            self._adicionar_alerta(
                f"Taxa de falha crítica: {taxa_falha:.1f}% (limite: {self.limites['taxa_falha_critica']}%)",
                "critical"
            )
        
        # Verificar tempo de operação
        if tempo_total > self.limites['tempo_operacao_lento']:
            self._adicionar_alerta(
                f"Operação lenta detectada: {tempo_total:.1f}s (limite: {self.limites['tempo_operacao_lento']}s)",
                "warning"
            )
        
        # Verificar velocidade
        total_processados = self.contadores['sucessos'] + self.contadores['falhas']
        velocidade = total_processados / tempo_total if tempo_total > 0 else 0
        
        if velocidade < self.limites['velocidade_minima'] and total_processados > 0:
            self._adicionar_alerta(
                f"Velocidade baixa: {velocidade:.2f} arq/s (mínimo: {self.limites['velocidade_minima']} arq/s)",
                "warning"
            )
    
    def _adicionar_alerta(self, mensagem: str, nivel: str):
        """
        Adiciona um alerta ao sistema
        
        Args:
            mensagem: Mensagem do alerta
            nivel: Nível do alerta (info, warning, critical)
        """
        alerta = {
            'timestamp': datetime.now(),
            'nivel': nivel,
            'mensagem': mensagem,
            'operacao': getattr(self, 'operacao_atual', 'desconhecida')
        }
        
        self.alertas.append(alerta)
        
        # Log do alerta
        emoji_map = {
            'info': "ℹ️" if self.usar_emojis else "",
            'warning': "⚠️" if self.usar_emojis else "",
            'critical': "🚨" if self.usar_emojis else ""
        }
        
        emoji = emoji_map.get(nivel, "")
        log_method = getattr(self.logger, nivel if nivel != 'critical' else 'error')
        log_method(f"{emoji} ALERTA: {mensagem}")
    
    def _gerar_relatorio_final(self, tempo_total: float):
        """
        Gera relatório final da operação
        
        Args:
            tempo_total: Tempo total da operação
        """
        taxa_sucesso = self.obter_taxa_sucesso()
        velocidade = (self.contadores['sucessos'] + self.contadores['falhas']) / tempo_total if tempo_total > 0 else 0
        
        emoji = "📊" if self.usar_emojis else ""
        self.logger.info(f"{emoji} === RELATÓRIO DE MONITORAMENTO ===")
        
        # Estatísticas básicas
        emoji_stats = "📈" if self.usar_emojis else ""
        self.logger.info(f"{emoji_stats} Operação: {self.operacao_atual}")
        self.logger.info(f"{emoji_stats} Tempo total: {tempo_total:.2f}s")
        self.logger.info(f"{emoji_stats} Sucessos: {self.contadores['sucessos']}")
        self.logger.info(f"{emoji_stats} Falhas: {self.contadores['falhas']}")
        self.logger.info(f"{emoji_stats} Taxa de sucesso: {taxa_sucesso:.1f}%")
        self.logger.info(f"{emoji_stats} Velocidade: {velocidade:.2f} arquivos/s")
        
        # Bytes processados
        if self.contadores['bytes_processados'] > 0:
            mb_processados = self.contadores['bytes_processados'] / (1024 * 1024)
            mb_por_segundo = mb_processados / tempo_total if tempo_total > 0 else 0
            emoji_bytes = "💽" if self.usar_emojis else ""
            self.logger.info(f"{emoji_bytes} Dados: {mb_processados:.1f} MB ({mb_por_segundo:.1f} MB/s)")
        
        # Alertas
        if self.alertas:
            emoji_alert = "🚨" if self.usar_emojis else ""
            self.logger.info(f"{emoji_alert} Alertas gerados: {len(self.alertas)}")
            for alerta in self.alertas[-3:]:  # Mostrar últimos 3 alertas
                self.logger.info(f"  - {alerta['nivel'].upper()}: {alerta['mensagem']}")
        
        self.logger.info("=" * 50)


class DescompactadorCaged:
    """
    Descompactador especializado para arquivos CAGED
    """
    
    def __init__(self, 
                 diretorio_origem: str = "files-zip",
                 diretorio_destino: str = "files-unzip",
                 max_workers: int = 4,
                 usar_emojis: bool = True,
                 nivel_log: str = "INFO"):
        """
        Inicializa o descompactador
        
        Args:
            diretorio_origem: Diretório com arquivos .7z
            diretorio_destino: Diretório para arquivos descompactados
            max_workers: Número máximo de workers para processamento paralelo
            usar_emojis: Se deve usar emojis nas mensagens de log (padrão: True)
            nivel_log: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.diretorio_origem = Path(diretorio_origem)
        self.diretorio_destino = Path(diretorio_destino)
        self.max_workers = max_workers
        self.metadados_arquivos: Dict[str, Dict] = {}
        self.usar_emojis = usar_emojis
        
        # Criar diretório de destino se não existir
        self.diretorio_destino.mkdir(parents=True, exist_ok=True)
        
        # Usar o logger centralizado configurado no main.py
        self.logger = logging.getLogger("caged")
        
        # Inicializar sistema de monitoramento
        self.monitor = MonitorDescompactacao(self.logger, usar_emojis)
    

    
    def _log_info(self, mensagem: str, emoji: str = "ℹ️"):
        """Log de informação com emoji opcional"""
        if self.usar_emojis:
            self.logger.debug(f"{emoji} {mensagem}")
        else:
            self.logger.debug(mensagem)
    
    def _log_warning(self, mensagem: str, emoji: str = "⚠️"):
        """Log de aviso com emoji opcional"""
        if self.usar_emojis:
            self.logger.warning(f"{emoji} {mensagem}")
        else:
            self.logger.warning(mensagem)
    
    def _log_error(self, mensagem: str, emoji: str = "❌"):
        """Log de erro com emoji opcional"""
        if self.usar_emojis:
            self.logger.error(f"{emoji} {mensagem}")
        else:
            self.logger.error(mensagem)
    
    def _log_success(self, mensagem: str, emoji: str = "✅"):
        """Log de sucesso com emoji opcional"""
        if self.usar_emojis:
            self.logger.info(f"{emoji} {mensagem}")
        else:
            self.logger.info(mensagem)
    
    def _log_debug(self, mensagem: str, emoji: str = "🔍"):
        """Log de debug com emoji opcional"""
        if self.usar_emojis:
            self.logger.debug(f"{emoji} {mensagem}")
        else:
            self.logger.debug(mensagem)
    
    def _log_estruturado(self, nivel: str, mensagem: str, dados_extras: Optional[Dict] = None):
        """
        Log estruturado para análise posterior
        
        Args:
            nivel: Nível do log (info, warning, error, debug)
            mensagem: Mensagem principal
            dados_extras: Dados adicionais em formato estruturado
        """
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "nivel": nivel,
            "mensagem": mensagem,
            "modulo": "descompactador_caged"
        }
        
        if dados_extras:
            log_data.update(dados_extras)
        
        # Log estruturado como JSON para análise
        log_json = json.dumps(log_data, ensure_ascii=False, separators=(',', ':'))
        
        # Enviar para o logger apropriado
        if nivel == "debug":
            self.logger.debug(f"STRUCTURED: {log_json}")
        elif nivel == "info":
            self.logger.info(f"STRUCTURED: {log_json}")
        elif nivel == "warning":
            self.logger.warning(f"STRUCTURED: {log_json}")
        elif nivel == "error":
            self.logger.error(f"STRUCTURED: {log_json}")
        
    def descompactar_arquivo(self, arquivo_7z: Path, destino: Optional[Path] = None, estrutura_complexa: bool = False) -> Tuple[bool, Dict]:
        """
        Descompacta um arquivo .7z específico
        
        Args:
            arquivo_7z: Caminho do arquivo .7z
            destino: Diretório destino (padrão: auto)
            
        Returns:
            Tuple[bool, Dict]: (Sucesso, Metadados do arquivo)
        """
        # Iniciar monitoramento da operação
        inicio_processamento = time.time()
        
        if not arquivo_7z.exists():
            self._log_error(f"Arquivo não encontrado: {arquivo_7z}")
            self.monitor.registrar_falha(f"Arquivo não encontrado: {arquivo_7z}", "arquivo_nao_encontrado")
            return False, {}
        
        # Extrair metadados do nome do arquivo
        metadados = self._extrair_metadados_nome(arquivo_7z.name)
        
        if destino is None:
            # Usar método inteligente para determinar destino
            destino = self._destino_arquivo_unzip(arquivo_7z, estrutura_complexa)
        
        destino.mkdir(parents=True, exist_ok=True)
        
        try:
            self._log_info(f"Descompactando: {arquivo_7z.name}", "📦")
            import py7zr
            
            # Verificar informações do arquivo antes da descompactação
            info_zip = self._verificar_arquivo_zip(arquivo_7z)
            metadados.update({
                "tamanho_arquivo_zip": info_zip["tamanho"],
                "hash_md5_zip": info_zip["hash_md5"]
            })
            
            # Log estruturado para início da descompactação
            self._log_estruturado("info", "Iniciando descompactação", {
                "arquivo": arquivo_7z.name,
                "tamanho_zip": info_zip["tamanho"],
                "destino": str(destino)
            })
            
            with py7zr.SevenZipFile(arquivo_7z, mode='r') as archive:
                arquivos_internos = archive.getnames()
                self._log_debug(f"{len(arquivos_internos)} arquivos encontrados", "📋")
                archive.extractall(path=destino)
                metadados["arquivos_extraidos"] = arquivos_internos
                metadados["data_descompactacao"] = datetime.now().isoformat()
                metadados["destino"] = str(destino)
            
            # Validar integridade após descompactação
            info_descompactado = self._verificar_arquivo_descompactado(arquivo_7z)
            if info_descompactado["existe"] and info_descompactado["tamanho_total"] > 0:
                metadados["arquivos_validados"] = len(info_descompactado["arquivos_txt"]) + len(info_descompactado["arquivos_csv"])
                metadados["tamanho_total_descompactado"] = info_descompactado["tamanho_total"]
                self._log_success(f"Descompactado e validado com sucesso em: {destino}")
                self._log_debug(f"{metadados['arquivos_validados']} arquivos válidos, {metadados['tamanho_total_descompactado']} bytes", "📊")
                
                # Registrar sucesso no monitor
                tempo_processamento = time.time() - inicio_processamento
                self.monitor.registrar_sucesso(tempo_processamento, metadados['tamanho_total_descompactado'])
                
                # Registrar métricas de descompactação
                record_operation(
                    operation="extract_file",
                    success=True,
                    duration=tempo_processamento,
                    details={
                        "arquivo": arquivo_7z.name,
                        "arquivos_extraidos": len(metadados.get('arquivos_extraidos', [])),
                        "tamanho_bytes": metadados['tamanho_total_descompactado'],
                        "destino": str(destino)
                    }
                )
                
                # Log estruturado para sucesso
                self._log_estruturado("info", "Descompactação concluída com sucesso", {
                    "arquivo": arquivo_7z.name,
                    "arquivos_validados": metadados['arquivos_validados'],
                    "tamanho_total": metadados['tamanho_total_descompactado'],
                    "tempo_processamento": tempo_processamento
                })
            else:
                self._log_warning(f"Descompactação concluída mas validação falhou para: {arquivo_7z.name}")
                self.monitor.registrar_falha(f"Validação falhou para: {arquivo_7z.name}", "validacao_falhou")
            
            self.metadados_arquivos[arquivo_7z.name] = metadados
            
            return True, metadados
            
        except Exception as e:
            self._log_error(f"Erro ao descompactar {arquivo_7z.name}: {e}")
            
            # Registrar falha no monitor
            self.monitor.registrar_falha(f"Erro ao descompactar {arquivo_7z.name}: {e}", "erro_descompactacao")
            
            # Registrar métricas de falha
            tempo_processamento = time.time() - inicio_processamento
            record_operation(
                operation="extract_file",
                success=False,
                duration=tempo_processamento,
                details={
                    "arquivo": arquivo_7z.name,
                    "erro": str(e),
                    "destino": str(destino) if destino else "N/A"
                }
            )
            
            # Log estruturado para erro
            self._log_estruturado("error", "Falha na descompactação", {
                "arquivo": arquivo_7z.name,
                "erro": str(e),
                "destino": str(destino),
                "tempo_processamento": time.time() - inicio_processamento
            })
            
            # Tentar limpar arquivos parcialmente extraídos
            try:
                if destino.exists():
                    for arquivo_temp in destino.glob("*.tmp"):
                        arquivo_temp.unlink()
            except:
                pass
            return False, {"erro": str(e), "arquivo": str(arquivo_7z)}
    
    def descompactar_mensal(self, ano: int, mes: int, usar_paralelo: bool = True, max_workers: Optional[int] = None) -> Tuple[bool, List[Dict]]:
        """
        Descompacta todos os arquivos de um mês específico com otimizações avançadas
        
        Args:
            ano: Ano dos dados
            mes: Mês dos dados
            usar_paralelo: Se deve usar processamento paralelo (padrão: True)
            max_workers: Número máximo de workers para processamento paralelo
            
        Returns:
            Tuple[bool, List[Dict]]: (Sucesso, Lista de metadados)
        """
        import time
        
        # Registrar início da operação mensal
        inicio_operacao = time.time()
        
        # Criar estrutura de pastas igual à origem
        diretorio_mes = f"{ano}{mes:02d}"
        diretorio_origem_mes = self.diretorio_origem / str(ano) / diretorio_mes
        diretorio_destino_mes = self.diretorio_destino / str(ano) / diretorio_mes
        
        # Verificar se o diretório de origem existe
        if not diretorio_origem_mes.exists():
            self._log_error(f"Diretório não encontrado: {diretorio_origem_mes}")
            return False, []
        
        # Encontrar arquivos .7z no diretório específico do mês
        arquivos_7z = list(diretorio_origem_mes.glob("*.7z"))
        
        if not arquivos_7z:
            self._log_error(f"Nenhum arquivo .7z encontrado para {ano}/{mes:02d}")
            return False, []
        
        self._log_info(f"Descompactando {len(arquivos_7z)} arquivos de {ano}/{mes:02d}", "🎯")
        
        # Log estruturado para início do processamento mensal
        self._log_estruturado("info", "Iniciando processamento mensal", {
            "ano": ano,
            "mes": mes,
            "total_arquivos": len(arquivos_7z),
            "usar_paralelo": usar_paralelo,
            "max_workers": max_workers or self.max_workers
        })
        
        # Criar diretório de destino
        diretorio_destino_mes.mkdir(parents=True, exist_ok=True)
        
        # Se usar processamento paralelo e há muitos arquivos, usar método paralelo otimizado
        if usar_paralelo and len(arquivos_7z) > 3:
            self._log_info(f"Usando processamento paralelo para {len(arquivos_7z)} arquivos", "🚀")
            
            # Usar filtro específico para o mês
            total, descompactados, falhas, metadados_lista = self.descompactar_arquivos_paralelo(
                max_workers=max_workers or self.max_workers,
                ano=ano
            )
            
            # Filtrar apenas os arquivos do mês específico
            metadados_mes = []
            sucessos = 0
            
            for metadados in metadados_lista:
                if metadados.get('mes') == f"{mes:02d}":
                    metadados_mes.append(metadados)
                    if 'arquivos_extraidos' in metadados:
                        sucessos += 1
            
            # Criar indicador específico para a competência
            indicador = self._criar_indicador_competencia(ano, mes, sucessos, len(arquivos_7z))
            
            # Processar dados dos arquivos descompactados se solicitado
            if sucessos > 0:
                self._log_info(f"Processando dados dos arquivos descompactados de {ano}/{mes:02d}...", "🔄")
                entidades_criadas = self._processar_dados_mes(ano, mes, metadados_mes)
                self._log_info(f"{len(entidades_criadas)} entidades CAGED criadas para {ano}/{mes:02d}", "📊")
                
                # Log estruturado para conclusão do processamento
                self._log_estruturado("info", "Processamento mensal concluído", {
                    "ano": ano,
                    "mes": mes,
                    "sucessos": sucessos,
                    "total_arquivos": len(arquivos_7z),
                    "entidades_criadas": len(entidades_criadas),
                    "metodo": "paralelo" if usar_paralelo else "sequencial"
                })
            
            return sucessos > 0, metadados_mes
        
        # Processamento sequencial otimizado (para poucos arquivos ou quando paralelo é desabilitado)
        sucessos = 0
        metadados_lista = []
        
        # Verificar quais arquivos precisam ser descompactados
        self.logger.debug(f"🔍 Verificando {len(arquivos_7z)} arquivos de {ano}/{mes:02d}...")
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
        
        self.logger.debug(f"✅ {len(arquivos_para_descompactar)} de {len(arquivos_7z)} arquivos precisam ser descompactados")
        
        if arquivos_para_descompactar:
            self.logger.info(f"🚀 Descompactando {len(arquivos_para_descompactar)} arquivos de {ano}/{mes:02d}")
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
            self.logger.info(f"🎉 Todos os arquivos de {ano}/{mes:02d} já estão descompactados!")
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
        
        # Registrar métricas da operação mensal
        tempo_total_operacao = time.time() - inicio_operacao
        record_operation(
            operation="extract_monthly",
            success=sucessos > 0,
            duration=tempo_total_operacao,
            details={
                "ano": ano,
                "mes": mes,
                "total_arquivos": len(arquivos_7z),
                "arquivos_processados": len(arquivos_para_descompactar),
                "sucessos": sucessos,
                "bytes_processados": bytes_processados,
                "metodo": "paralelo" if usar_paralelo and len(arquivos_7z) > 3 else "sequencial"
            }
        )
        
        return sucessos > 0, metadados_lista
    
    def descompactar_arquivos_paralelo(self, 
                                      max_workers: Optional[int] = None, 
                                      ano: Optional[int] = None,
                                      ano_inicio: Optional[int] = None,
                                      ano_fim: Optional[int] = None,
                                      uf: Optional[str] = None,
                                      tipo_arquivo: Optional[str] = None) -> Tuple[int, int, int, List[Dict]]:
        """
        Descompacta arquivos .7z de forma paralela, opcionalmente filtrados por ano, UF e tipo.
        Verifica se arquivos já foram descompactados para evitar reprocessamento.
        
        Args:
            max_workers: Número máximo de workers (usa self.max_workers se None)
            ano: Ano específico para filtrar
            ano_inicio: Ano inicial da faixa
            ano_fim: Ano final da faixa
            uf: UF específica para filtrar (ex: 'SP', 'RJ')
            tipo_arquivo: Tipo de arquivo para filtrar ('movimentacao', 'exclusao', 'fora_prazo')
            
        Returns:
            Tupla com (total de arquivos, arquivos descompactados, falhas, metadados)
        """
        import time
        
        if max_workers is None:
            max_workers = self.max_workers
        
        inicio_total = time.time()
        self.logger.info("🚀 Iniciando descompactação paralela de arquivos CAGED")
        
        # Iniciar monitoramento da operação
        self.monitor.iniciar_operacao("Descompactação Paralela", 0)  # Total será atualizado após listar arquivos
        
        # Listar arquivos .7z com filtros
        arquivos_zip = self._listar_arquivos_zip_filtrados(ano, ano_inicio, ano_fim, uf, tipo_arquivo)
        
        if not arquivos_zip:
            self.logger.info("❌ Nenhum arquivo .7z encontrado para descompactar")
            self.monitor.finalizar_operacao()
            return 0, 0, 0, []
        
        # Atualizar total de arquivos no monitor
        self.monitor.contadores['total_arquivos'] = len(arquivos_zip)
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
            # Registrar arquivos ignorados no monitor
            for _ in arquivos_zip:
                self.monitor.registrar_arquivo_ignorado()
            # Recuperar metadados dos arquivos já processados
            for arquivo in arquivos_zip:
                metadados = self.metadados_arquivos.get(arquivo.name, self._extrair_metadados_nome(arquivo.name))
                metadados_lista.append(metadados)
            self.monitor.finalizar_operacao()
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
        
        # Finalizar monitoramento
        self.monitor.finalizar_operacao()
        
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
        
        # Iniciar monitoramento
        self.monitor.iniciar_operacao()
        
        # Encontrar arquivos
        padrao = f"*{ano}*.7z" if ano else "*.7z"
        arquivos_7z = list(self.diretorio_origem.rglob(padrao))
        
        if not arquivos_7z:
            self.logger.error(f"❌ Nenhum arquivo .7z encontrado" + (f" para {ano}" if ano else ""))
            self.monitor.finalizar_operacao()
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
        
        # Finalizar monitoramento
        self.monitor.finalizar_operacao()
        
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
        Extrai metadados específicos do CAGED do nome do arquivo
        
        Args:
            nome_arquivo: Nome do arquivo
            
        Returns:
            Dict: Metadados extraídos com informações específicas do CAGED
        """
        metadados = {
            "nome_arquivo": nome_arquivo,
            "data_processamento": datetime.now().isoformat(),
            "fonte": "CAGED"
        }
        
        # Extrair ano (formato 20XX)
        ano_match = re.search(r'(20\d{2})', nome_arquivo)
        if ano_match:
            metadados["ano"] = int(ano_match.group(1))
        
        # Extrair mês (formato YYYYMM)
        mes_match = re.search(r'(20\d{2})(\d{2})', nome_arquivo)
        if mes_match and 1 <= int(mes_match.group(2)) <= 12:
            metadados["mes"] = int(mes_match.group(2))
            metadados["competencia"] = f"{metadados['ano']}-{metadados['mes']:02d}"
        
        # Identificar UF com padrões específicos do CAGED
        from src.entities.enums import UF
        ufs = [uf.value for uf in UF if uf.value != '99']  # Excluir 'Não Identificado'
        
        # Busca mais robusta de UF no nome do arquivo
        nome_upper = nome_arquivo.upper()
        for uf in ufs:
            # Buscar UF como palavra separada ou no final/início
            if re.search(rf'\b{uf}\b|{uf}_|_{uf}|{uf}\d|\d{uf}', nome_upper):
                metadados["uf"] = uf
                break
        
        # Identificar tipo de arquivo com padrões específicos do CAGED
        if re.search(r'MOVIMENTAC[AÃ]O|MOVIMENT|MOV(?!IMENTO)', nome_upper):
            metadados["tipo"] = "movimentacao"
        elif re.search(r'EXCLUS[AÃ]O|EXCL', nome_upper):
            metadados["tipo"] = "exclusao"
        elif re.search(r'FORA[_\s]?PRAZO|FORAPRAZO|FORA_PRAZO', nome_upper):
            metadados["tipo"] = "fora_prazo"
        elif re.search(r'SALDO[_\s]?MENSAL|SALDO', nome_upper):
            metadados["tipo"] = "saldo_mensal"
        else:
            metadados["tipo"] = "outros"
        
        # Identificar modalidade (Novo CAGED vs CAGED Antigo)
        if re.search(r'NOVO[_\s]?CAGED|NCAGED', nome_upper):
            metadados["modalidade"] = "novo_caged"
        elif re.search(r'CAGED[_\s]?ANTIGO|ACAGED', nome_upper):
            metadados["modalidade"] = "caged_antigo"
        else:
            metadados["modalidade"] = "padrao"
        
        # Adicionar informações de entidade correspondente
        metadados["entidade_classe"] = self._mapear_entidade_classe(metadados.get("tipo", "outros"))
        
        return metadados
    
    def _mapear_entidade_classe(self, tipo_arquivo: str) -> str:
        """
        Mapeia o tipo de arquivo para a classe de entidade correspondente
        
        Args:
            tipo_arquivo: Tipo do arquivo identificado
            
        Returns:
            str: Nome da classe de entidade
        """
        mapeamento = {
            "movimentacao": "Movimentacao",
            "exclusao": "Exclusao",
            "fora_prazo": "MovimentacaoForaPrazo",
            "saldo_mensal": "SaldoMensal",
            "outros": "Movimentacao"  # Fallback padrão
        }
        
        return mapeamento.get(tipo_arquivo, "Movimentacao")
    
    def criar_entidade_caged(self, metadados: Dict, dados_arquivo: Optional[Dict] = None) -> Any:
        """
        Cria uma instância da entidade CAGED apropriada baseada nos metadados
        
        Args:
            metadados: Metadados extraídos do arquivo
            dados_arquivo: Dados opcionais do arquivo para popular a entidade
            
        Returns:
            Instância da entidade CAGED apropriada
        """
        tipo_arquivo = metadados.get("tipo", "movimentacao")
        entidade_classe = metadados.get("entidade_classe", "Movimentacao")
        
        # Dados padrão para a entidade
        dados_entidade = {
            "competencia": metadados.get("competencia", ""),
            "uf": metadados.get("uf", ""),
            "fonte_arquivo": metadados.get("nome_arquivo", ""),
            "data_processamento": metadados.get("data_processamento", datetime.now().isoformat())
        }
        
        # Adicionar dados específicos do arquivo se fornecidos
        if dados_arquivo:
            dados_entidade.update(dados_arquivo)
        
        try:
            if tipo_arquivo == "movimentacao":
                return Movimentacao(
                    id=0,  # ID será atribuído na persistência
                    cnpj=dados_entidade.get("cnpj", ""),
                    competencia=dados_entidade["competencia"],
                    uf=dados_entidade["uf"],
                    municipio=dados_entidade.get("municipio", ""),
                    estabelecimento=dados_entidade.get("estabelecimento", ""),
                    admissoes=dados_entidade.get("admissoes", 0),
                    desligamentos=dados_entidade.get("desligamentos", 0),
                    saldo=dados_entidade.get("saldo", 0)
                )
            elif tipo_arquivo == "exclusao":
                return Exclusao(
                    id=0,
                    cnpj=dados_entidade.get("cnpj", ""),
                    competencia=dados_entidade["competencia"],
                    motivo_exclusao=dados_entidade.get("motivo_exclusao", ""),
                    data_exclusao=dados_entidade.get("data_exclusao", "")
                )
            elif tipo_arquivo == "fora_prazo":
                return MovimentacaoForaPrazo(
                    id=0,
                    cnpj=dados_entidade.get("cnpj", ""),
                    competencia=dados_entidade["competencia"],
                    uf=dados_entidade["uf"],
                    municipio=dados_entidade.get("municipio", ""),
                    estabelecimento=dados_entidade.get("estabelecimento", ""),
                    admissoes=dados_entidade.get("admissoes", 0),
                    desligamentos=dados_entidade.get("desligamentos", 0),
                    saldo=dados_entidade.get("saldo", 0),
                    data_declaracao=dados_entidade.get("data_declaracao", "")
                )
            elif tipo_arquivo == "saldo_mensal":
                return SaldoMensal(
                    id=0,
                    cnpj=dados_entidade.get("cnpj", ""),
                    competencia=dados_entidade["competencia"],
                    uf=dados_entidade["uf"],
                    municipio=dados_entidade.get("municipio", ""),
                    estabelecimento=dados_entidade.get("estabelecimento", ""),
                    saldo_inicial=dados_entidade.get("saldo_inicial", 0),
                    admissoes=dados_entidade.get("admissoes", 0),
                    desligamentos=dados_entidade.get("desligamentos", 0),
                    saldo_final=dados_entidade.get("saldo_final", 0)
                )
            else:
                # Fallback para Movimentacao
                return Movimentacao(
                    id=0,
                    cnpj=dados_entidade.get("cnpj", ""),
                    competencia=dados_entidade["competencia"],
                    uf=dados_entidade["uf"],
                    municipio=dados_entidade.get("municipio", ""),
                    estabelecimento=dados_entidade.get("estabelecimento", ""),
                    admissoes=dados_entidade.get("admissoes", 0),
                    desligamentos=dados_entidade.get("desligamentos", 0),
                    saldo=dados_entidade.get("saldo", 0)
                )
        except Exception as e:
            self.logger.error(f"❌ Erro ao criar entidade {entidade_classe}: {e}")
            # Retornar entidade básica em caso de erro
            return Movimentacao(
                id=0,
                cnpj="",
                competencia=dados_entidade.get("competencia", ""),
                uf=dados_entidade.get("uf", ""),
                municipio="",
                estabelecimento="",
                admissoes=0,
                desligamentos=0,
                saldo=0
            )
    
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
                                      ano_fim: Optional[int] = None,
                                      uf: Optional[str] = None,
                                      tipo_arquivo: Optional[str] = None) -> List[Path]:
        """
        Lista arquivos .7z na pasta files-zip, opcionalmente filtrados por ano, UF e tipo.
        
        Args:
            ano: Ano específico para filtrar
            ano_inicio: Ano inicial da faixa
            ano_fim: Ano final da faixa
            uf: UF específica para filtrar (ex: 'SP', 'RJ')
            tipo_arquivo: Tipo de arquivo para filtrar ('movimentacao', 'exclusao', 'fora_prazo')
            
        Returns:
            Lista de caminhos dos arquivos .7z encontrados e filtrados
        """
        # Encontrar todos os arquivos .7z
        arquivos_zip = list(self.diretorio_origem.rglob("*.7z"))
        
        if not arquivos_zip:
            return []
        
        arquivos_filtrados = []
        
        for arquivo in arquivos_zip:
            # Extrair metadados do arquivo para aplicar filtros
            metadados = self._extrair_metadados_nome(arquivo.name)
            ano_arquivo = metadados.get('ano')
            uf_arquivo = metadados.get('uf')
            tipo_arquivo_atual = metadados.get('tipo')
            
            # Aplicar filtros de ano (garantir que ano_arquivo seja int)
            if ano and ano_arquivo != ano:
                continue
            
            if ano_inicio and ano_arquivo:
                try:
                    ano_int = int(ano_arquivo) if isinstance(ano_arquivo, str) else ano_arquivo
                    if ano_int < ano_inicio:
                        continue
                except (ValueError, TypeError):
                    continue
                
            if ano_fim and ano_arquivo:
                try:
                    ano_int = int(ano_arquivo) if isinstance(ano_arquivo, str) else ano_arquivo
                    if ano_int > ano_fim:
                        continue
                except (ValueError, TypeError):
                    continue
            
            # Aplicar filtro de UF
            if uf and uf_arquivo and uf_arquivo.upper() != uf.upper():
                continue
            
            # Aplicar filtro de tipo de arquivo
            if tipo_arquivo and tipo_arquivo_atual != tipo_arquivo:
                continue
            
            # Se passou por todos os filtros, adicionar à lista
            arquivos_filtrados.append(arquivo)
        
        return arquivos_filtrados
    
    def listar_arquivos_zip_locais(self, 
                                   ano: Optional[int] = None,
                                   ano_inicio: Optional[int] = None,
                                   ano_fim: Optional[int] = None,
                                   uf: Optional[str] = None,
                                   tipo_arquivo: Optional[str] = None,
                                   mostrar_detalhes: bool = True) -> List[Dict]:
        """
        Lista arquivos .7z locais com filtros avançados e informações detalhadas.
        
        Args:
            ano: Ano específico para filtrar
            ano_inicio: Ano inicial da faixa
            ano_fim: Ano final da faixa
            uf: UF específica para filtrar (ex: 'SP', 'RJ')
            tipo_arquivo: Tipo de arquivo para filtrar ('movimentacao', 'exclusao', 'fora_prazo')
            mostrar_detalhes: Se deve mostrar detalhes dos arquivos encontrados
            
        Returns:
            Lista de dicionários com informações dos arquivos encontrados
        """
        # Usar o método de filtros existente
        arquivos_zip = self._listar_arquivos_zip_filtrados(ano, ano_inicio, ano_fim, uf, tipo_arquivo)
        
        if not arquivos_zip:
            if mostrar_detalhes:
                self.logger.info("❌ Nenhum arquivo .7z encontrado com os filtros especificados")
            return []
        
        # Preparar informações detalhadas dos arquivos
        arquivos_info = []
        
        for arquivo in arquivos_zip:
            # Extrair metadados completos
            metadados = self._extrair_metadados_nome(arquivo.name)
            
            # Verificar informações do arquivo
            info_arquivo = self._verificar_arquivo_zip(arquivo)
            
            # Verificar se já foi descompactado
            ja_descompactado = not self._arquivo_precisa_descompactar(arquivo)
            
            # Montar informações completas
            arquivo_info = {
                'caminho': arquivo,
                'nome': arquivo.name,
                'tamanho': info_arquivo['tamanho'],
                'tamanho_mb': round(info_arquivo['tamanho'] / 1024 / 1024, 2),
                'hash_md5': info_arquivo['hash_md5'],
                'data_modificacao': info_arquivo['data_modificacao'],
                'ano': metadados.get('ano'),
                'mes': metadados.get('mes'),
                'uf': metadados.get('uf'),
                'tipo': metadados.get('tipo'),
                'competencia': metadados.get('competencia'),
                'ja_descompactado': ja_descompactado
            }
            
            arquivos_info.append(arquivo_info)
        
        # Ordenar por ano, mês e UF (tratando valores None)
        arquivos_info.sort(key=lambda x: (
            x.get('ano') or 0, 
            x.get('mes') or 0, 
            x.get('uf') or ''
        ))
        
        if mostrar_detalhes:
            self._mostrar_detalhes_arquivos_locais(arquivos_info, ano, ano_inicio, ano_fim, uf, tipo_arquivo)
        
        return arquivos_info
    
    def _mostrar_detalhes_arquivos_locais(self, 
                                         arquivos_info: List[Dict],
                                         ano: Optional[int] = None,
                                         ano_inicio: Optional[int] = None,
                                         ano_fim: Optional[int] = None,
                                         uf: Optional[str] = None,
                                         tipo_arquivo: Optional[str] = None) -> None:
        """
        Mostra detalhes dos arquivos .7z locais encontrados.
        
        Args:
            arquivos_info: Lista de informações dos arquivos
            ano: Filtro de ano aplicado
            ano_inicio: Filtro de ano inicial aplicado
            ano_fim: Filtro de ano final aplicado
            uf: Filtro de UF aplicado
            tipo_arquivo: Filtro de tipo aplicado
        """
        from collections import defaultdict
        
        total_arquivos = len(arquivos_info)
        total_tamanho_mb = sum(info['tamanho_mb'] for info in arquivos_info)
        ja_descompactados = sum(1 for info in arquivos_info if info['ja_descompactado'])
        
        # Mostrar filtros aplicados
        filtros_aplicados = []
        if ano:
            filtros_aplicados.append(f"Ano: {ano}")
        elif ano_inicio or ano_fim:
            if ano_inicio and ano_fim:
                filtros_aplicados.append(f"Anos: {ano_inicio}-{ano_fim}")
            elif ano_inicio:
                filtros_aplicados.append(f"Ano >= {ano_inicio}")
            elif ano_fim:
                filtros_aplicados.append(f"Ano <= {ano_fim}")
        
        if uf:
            filtros_aplicados.append(f"UF: {uf.upper()}")
        
        if tipo_arquivo:
            filtros_aplicados.append(f"Tipo: {tipo_arquivo}")
        
        self.logger.info(f"📋 Arquivos .7z encontrados: {total_arquivos}")
        if filtros_aplicados:
            self.logger.info(f"🔍 Filtros: {', '.join(filtros_aplicados)}")
        
        self.logger.info(f"📊 Tamanho total: {total_tamanho_mb:.1f} MB")
        self.logger.info(f"✅ Já descompactados: {ja_descompactados}/{total_arquivos}")
        
        # Agrupar por ano
        arquivos_por_ano = defaultdict(list)
        for info in arquivos_info:
            ano_arquivo = info.get('ano', 'Desconhecido')
            arquivos_por_ano[str(ano_arquivo)].append(info)
        
        # Mostrar resumo por ano
        if len(arquivos_por_ano) > 1:
            self.logger.info("\n📅 Resumo por ano:")
            for ano_key in sorted(arquivos_por_ano.keys()):
                arquivos_ano = arquivos_por_ano[ano_key]
                total_ano = len(arquivos_ano)
                descompactados_ano = sum(1 for info in arquivos_ano if info['ja_descompactado'])
                tamanho_ano = sum(info['tamanho_mb'] for info in arquivos_ano)
                
                self.logger.info(f"   {ano_key}: {total_ano} arquivo{'s' if total_ano > 1 else ''}, "
                               f"{descompactados_ano} descompactado{'s' if descompactados_ano != 1 else ''}, "
                               f"{tamanho_ano:.1f} MB")
        
        # Agrupar por UF se não houver filtro de UF específico
        if not uf and total_arquivos > 0:
            arquivos_por_uf = defaultdict(list)
            for info in arquivos_info:
                uf_arquivo = info.get('uf', 'Desconhecida')
                arquivos_por_uf[uf_arquivo].append(info)
            
            if len(arquivos_por_uf) > 1:
                self.logger.info("\n🗺️  Resumo por UF:")
                # Filtrar None e ordenar
                uf_keys = [k for k in arquivos_por_uf.keys() if k is not None]
                for uf_key in sorted(uf_keys):
                    arquivos_uf = arquivos_por_uf[uf_key]
                    total_uf = len(arquivos_uf)
                    descompactados_uf = sum(1 for info in arquivos_uf if info['ja_descompactado'])
                    
                    self.logger.info(f"   {uf_key}: {total_uf} arquivo{'s' if total_uf > 1 else ''}, "
                                   f"{descompactados_uf} descompactado{'s' if descompactados_uf != 1 else ''}")
        
        # Agrupar por tipo se não houver filtro de tipo específico
        if not tipo_arquivo and total_arquivos > 0:
            arquivos_por_tipo = defaultdict(list)
            for info in arquivos_info:
                tipo_info = info.get('tipo', 'outros')
                arquivos_por_tipo[tipo_info].append(info)
            
            if len(arquivos_por_tipo) > 1:
                self.logger.info("\n📁 Resumo por tipo:")
                # Filtrar None e ordenar
                tipo_keys = [k for k in arquivos_por_tipo.keys() if k is not None]
                for tipo_key in sorted(tipo_keys):
                    arquivos_tipo = arquivos_por_tipo[tipo_key]
                    total_tipo = len(arquivos_tipo)
                    descompactados_tipo = sum(1 for info in arquivos_tipo if info['ja_descompactado'])
                    
                    self.logger.info(f"   {tipo_key}: {total_tipo} arquivo{'s' if total_tipo > 1 else ''}, "
                                   f"{descompactados_tipo} descompactado{'s' if descompactados_tipo != 1 else ''}")
    
    def _destino_arquivo_unzip(self, arquivo_7z: Path, estrutura_complexa: bool = False) -> Path:
        """
        Determina o diretório de destino inteligente para descompactação.
        Mantém estrutura hierárquica: files-unzip/ANO/MES/ ou files-unzip/ANO/
        Suporte a estruturas complexas: files-unzip/ANO/MES/UF/ ou files-unzip/ANO/MES/TIPO/
        
        Args:
            arquivo_7z: Caminho do arquivo .7z
            estrutura_complexa: Se deve usar estrutura de pastas complexa
            
        Returns:
            Path: Caminho do diretório de destino
        """
        # Extrair metadados do arquivo
        metadados = self._extrair_metadados_nome(arquivo_7z.name)
        ano = metadados.get('ano')
        mes = metadados.get('mes')
        uf = metadados.get('uf')
        tipo = metadados.get('tipo')
        
        # Estrutura base: files-unzip/ANO/
        if ano:
            destino_base = self.diretorio_destino / str(ano)
        else:
            # Fallback: usar ano atual se não conseguir extrair
            from datetime import datetime
            ano_atual = datetime.now().year
            destino_base = self.diretorio_destino / str(ano_atual)
        
        # Se temos mês, criar subdiretório: files-unzip/ANO/MES/
        if mes:
            # Formato: AAAAMM (ex: 202401)
            try:
                mes_int = int(mes) if isinstance(mes, str) else mes
                competencia = f"{ano}{mes_int:02d}"
                destino_final = destino_base / competencia
            except (ValueError, TypeError):
                # Se não conseguir converter mês, usar apenas o ano
                destino_final = destino_base
        else:
            # Sem mês específico, usar apenas o ano
            destino_final = destino_base
        
        # Estrutura complexa adicional (opcional)
        if estrutura_complexa:
            # Adicionar subdiretório por UF se disponível
            if uf and uf != 'BR':  # BR indica arquivo nacional
                destino_final = destino_final / uf
            
            # Adicionar subdiretório por tipo se não for movimentação padrão
            if tipo and tipo not in ['movimentacao', 'outros']:
                destino_final = destino_final / tipo.upper()
        
        return destino_final
    
    def _criar_estrutura_diretorios_complexa(self, metadados_lista: List[Dict]) -> Dict[str, List[Path]]:
        """
        Cria estrutura de diretórios complexa baseada nos metadados dos arquivos.
        
        Args:
            metadados_lista: Lista de metadados dos arquivos
            
        Returns:
            Dict: Mapeamento de categorias para listas de diretórios
        """
        from collections import defaultdict
        
        estrutura = {
            'por_ano': defaultdict(list),
            'por_uf': defaultdict(list),
            'por_tipo': defaultdict(list),
            'por_competencia': defaultdict(list)
        }
        
        for metadados in metadados_lista:
            ano = metadados.get('ano')
            mes = metadados.get('mes')
            uf = metadados.get('uf')
            tipo = metadados.get('tipo')
            
            # Organizar por ano
            if ano:
                estrutura['por_ano'][str(ano)].append(metadados)
            
            # Organizar por UF
            if uf:
                estrutura['por_uf'][uf].append(metadados)
            
            # Organizar por tipo
            if tipo:
                estrutura['por_tipo'][tipo].append(metadados)
            
            # Organizar por competência (AAAAMM)
            if ano and mes:
                try:
                    mes_int = int(mes) if isinstance(mes, str) else mes
                    competencia = f"{ano}{mes_int:02d}"
                    estrutura['por_competencia'][competencia].append(metadados)
                except (ValueError, TypeError):
                    pass
        
        return estrutura
    
    def _otimizar_estrutura_diretorios(self, arquivos_7z: List[Path]) -> Dict[str, Any]:
        """
        Analisa arquivos e sugere estrutura de diretórios otimizada.
        
        Args:
            arquivos_7z: Lista de arquivos .7z
            
        Returns:
            Dict: Análise e sugestões de estrutura
        """
        from collections import Counter
        
        # Extrair metadados de todos os arquivos
        todos_metadados = []
        for arquivo in arquivos_7z:
            metadados = self._extrair_metadados_nome(arquivo.name)
            metadados['arquivo'] = arquivo
            todos_metadados.append(metadados)
        
        # Análise estatística
        anos = [m.get('ano') for m in todos_metadados if m.get('ano')]
        ufs = [m.get('uf') for m in todos_metadados if m.get('uf')]
        tipos = [m.get('tipo') for m in todos_metadados if m.get('tipo')]
        meses = [m.get('mes') for m in todos_metadados if m.get('mes')]
        
        analise = {
            'total_arquivos': len(arquivos_7z),
            'anos_unicos': len(set(anos)),
            'ufs_unicas': len(set(ufs)),
            'tipos_unicos': len(set(tipos)),
            'meses_unicos': len(set(meses)),
            'distribuicao_anos': dict(Counter(anos)),
            'distribuicao_ufs': dict(Counter(ufs)),
            'distribuicao_tipos': dict(Counter(tipos)),
            'sugestao_estrutura_complexa': False
        }
        
        # Sugerir estrutura complexa se houver diversidade suficiente
        if analise['ufs_unicas'] > 3 or analise['tipos_unicos'] > 2:
            analise['sugestao_estrutura_complexa'] = True
            analise['motivo_sugestao'] = []
            
            if analise['ufs_unicas'] > 3:
                analise['motivo_sugestao'].append(f"Múltiplas UFs ({analise['ufs_unicas']})")
            
            if analise['tipos_unicos'] > 2:
                analise['motivo_sugestao'].append(f"Múltiplos tipos ({analise['tipos_unicos']})")
        
        return analise
    
    def _validar_estrutura_diretorios(self, diretorio_base: Path) -> Dict[str, Any]:
        """
        Valida a estrutura de diretórios existente.
        
        Args:
            diretorio_base: Diretório base para validação
            
        Returns:
            Dict: Resultado da validação
        """
        validacao = {
            'estrutura_valida': True,
            'problemas': [],
            'estatisticas': {
                'total_diretorios': 0,
                'diretorios_ano': 0,
                'diretorios_competencia': 0,
                'diretorios_uf': 0,
                'diretorios_tipo': 0
            },
            'sugestoes': []
        }
        
        if not diretorio_base.exists():
            validacao['estrutura_valida'] = False
            validacao['problemas'].append(f"Diretório base não existe: {diretorio_base}")
            return validacao
        
        # Analisar estrutura existente
        for item in diretorio_base.rglob('*'):
            if item.is_dir():
                validacao['estatisticas']['total_diretorios'] += 1
                nome = item.name
                
                # Verificar padrões de diretórios
                if nome.isdigit() and len(nome) == 4 and nome.startswith('20'):
                    validacao['estatisticas']['diretorios_ano'] += 1
                elif nome.isdigit() and len(nome) == 6 and nome.startswith('20'):
                    validacao['estatisticas']['diretorios_competencia'] += 1
                elif len(nome) == 2 and nome.isupper():
                    validacao['estatisticas']['diretorios_uf'] += 1
                elif nome.upper() in ['MOVIMENTACAO', 'EXCLUSAO', 'FORA_PRAZO']:
                    validacao['estatisticas']['diretorios_tipo'] += 1
        
        # Gerar sugestões
        if validacao['estatisticas']['diretorios_uf'] > 0:
            validacao['sugestoes'].append("Estrutura por UF detectada - considere usar estrutura_complexa=True")
        
        if validacao['estatisticas']['diretorios_tipo'] > 0:
            validacao['sugestoes'].append("Estrutura por tipo detectada - considere usar estrutura_complexa=True")
        
        return validacao
    
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
    
    def _mostrar_resumo_por_ano(self, metadados_por_ano: Dict[int, List[Dict]]) -> None:
        """
        Mostra resumo estatístico agrupado por ano
        
        Args:
            metadados_por_ano: Dicionário com metadados agrupados por ano
        """
        self.logger.info("\n" + "="*60)
        self.logger.info("📊 RESUMO POR ANO")
        self.logger.info("="*60)
        
        for ano in sorted(metadados_por_ano.keys()):
            metadados_ano = metadados_por_ano[ano]
            
            # Estatísticas básicas
            total_arquivos = len(metadados_ano)
            
            # Agrupar por mês
            por_mes = {}
            por_uf = {}
            por_tipo = {}
            
            for meta in metadados_ano:
                # Por mês
                mes = meta.get('mes', 'desconhecido')
                por_mes[mes] = por_mes.get(mes, 0) + 1
                
                # Por UF
                uf = meta.get('uf', 'desconhecida')
                por_uf[uf] = por_uf.get(uf, 0) + 1
                
                # Por tipo
                tipo = meta.get('tipo', 'desconhecido')
                por_tipo[tipo] = por_tipo.get(tipo, 0) + 1
            
            # Exibir estatísticas do ano
            self.logger.info(f"\n📅 ANO {ano}:")
            self.logger.info(f"   📁 Total de arquivos: {total_arquivos}")
            
            # Meses com mais arquivos
            if por_mes:
                meses_ordenados = sorted(por_mes.items(), key=lambda x: x[1], reverse=True)
                self.logger.info(f"   📆 Meses: {len(por_mes)} diferentes")
                top_meses = meses_ordenados[:3]
                for mes, count in top_meses:
                    self.logger.info(f"      • {mes}: {count} arquivos")
            
            # UFs com mais arquivos
            if por_uf:
                ufs_ordenadas = sorted(por_uf.items(), key=lambda x: x[1], reverse=True)
                self.logger.info(f"   🗺️  UFs: {len(por_uf)} diferentes")
                top_ufs = ufs_ordenadas[:5]
                for uf, count in top_ufs:
                    self.logger.info(f"      • {uf}: {count} arquivos")
            
            # Tipos de arquivo
            if por_tipo:
                tipos_ordenados = sorted(por_tipo.items(), key=lambda x: x[1], reverse=True)
                self.logger.info(f"   📋 Tipos: {len(por_tipo)} diferentes")
                for tipo, count in tipos_ordenados:
                    self.logger.info(f"      • {tipo}: {count} arquivos")
        
        self.logger.info("\n" + "="*60)
    
    def gerar_relatorio_descompactacao(self, ano: Optional[int] = None, incluir_metricas: bool = True) -> Dict:
        """
        Gera relatório detalhado sobre os arquivos descompactados
        
        Args:
            ano: Ano específico ou None para todos
            incluir_metricas: Se deve incluir métricas de performance
            
        Returns:
            Dict: Relatório com estatísticas detalhadas
        """
        import time
        inicio = time.time()
        
        arquivos = self.listar_arquivos_descompactados(ano)
        
        # Estatísticas básicas
        tipos = {}
        extensoes = {}
        competencias = {}
        ufs = {}
        tamanhos = []
        
        # Métricas de performance
        tempo_processamento = 0
        arquivos_processados = 0
        
        for arquivo in arquivos:
            # Contar por extensão
            ext = arquivo.suffix.lower()
            extensoes[ext] = extensoes.get(ext, 0) + 1
            
            # Tamanho do arquivo
            try:
                tamanho = arquivo.stat().st_size
                tamanhos.append(tamanho)
            except:
                pass
            
            # Tentar extrair metadados do nome
            meta = self._extrair_metadados_nome(arquivo.name)
            
            # Contar por tipo
            tipo = meta.get("tipo", "desconhecido")
            tipos[tipo] = tipos.get(tipo, 0) + 1
            
            # Contar por competência
            comp = meta.get("competencia", "desconhecida")
            competencias[comp] = competencias.get(comp, 0) + 1
            
            # Contar por UF
            uf = meta.get("uf", "desconhecida")
            ufs[uf] = ufs.get(uf, 0) + 1
        
        # Calcular estatísticas de tamanho
        estatisticas_tamanho = {}
        if tamanhos:
            estatisticas_tamanho = {
                "total_bytes": sum(tamanhos),
                "media_bytes": sum(tamanhos) / len(tamanhos),
                "maior_arquivo": max(tamanhos),
                "menor_arquivo": min(tamanhos)
            }
        
        # Métricas de performance
        tempo_relatorio = time.time() - inicio
        metricas_performance = {}
        
        if incluir_metricas:
            metricas_performance = {
                "tempo_geracao_relatorio": tempo_relatorio,
                "arquivos_por_segundo": len(arquivos) / tempo_relatorio if tempo_relatorio > 0 else 0,
                "timestamp_geracao": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        
        # Gerar relatório completo
        relatorio = {
            "resumo": {
                "total_arquivos": len(arquivos),
                "ano_filtro": ano,
                "data_geracao": time.strftime("%Y-%m-%d %H:%M:%S")
            },
            "distribuicao": {
                "por_extensao": dict(sorted(extensoes.items(), key=lambda x: x[1], reverse=True)),
                "por_tipo": dict(sorted(tipos.items(), key=lambda x: x[1], reverse=True)),
                "por_competencia": dict(sorted(competencias.items())),
                "por_uf": dict(sorted(ufs.items(), key=lambda x: x[1], reverse=True))
            },
            "estatisticas_tamanho": estatisticas_tamanho
        }
        
        if incluir_metricas:
            relatorio["metricas_performance"] = metricas_performance
        
        return relatorio
    
    def processar_dados_arquivo_caged(self, arquivo_descompactado: Path, metadados: Dict) -> List[Any]:
        """
        Processa os dados de um arquivo descompactado e cria entidades CAGED
        
        Args:
            arquivo_descompactado: Caminho do arquivo descompactado (.txt ou .csv)
            metadados: Metadados extraídos do arquivo
            
        Returns:
            Lista de entidades CAGED criadas
        """
        entidades = []
        
        try:
            if not arquivo_descompactado.exists():
                self.logger.warning(f"⚠️  Arquivo não encontrado: {arquivo_descompactado}")
                return entidades
            
            # Determinar o tipo de arquivo e processamento
            extensao = arquivo_descompactado.suffix.lower()
            
            if extensao == '.txt':
                entidades = self._processar_arquivo_txt_caged(arquivo_descompactado, metadados)
            elif extensao == '.csv':
                entidades = self._processar_arquivo_csv_caged(arquivo_descompactado, metadados)
            else:
                self.logger.warning(f"⚠️  Tipo de arquivo não suportado: {extensao}")
            
            self.logger.info(f"📋 Processadas {len(entidades)} entidades de {arquivo_descompactado.name}")
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao processar arquivo {arquivo_descompactado}: {e}")
        
        return entidades
    
    def _processar_arquivo_txt_caged(self, arquivo_txt: Path, metadados: Dict) -> List[Any]:
        """
        Processa um arquivo .txt do CAGED e cria entidades
        
        Args:
            arquivo_txt: Caminho do arquivo .txt
            metadados: Metadados extraídos
            
        Returns:
            Lista de entidades criadas
        """
        entidades = []
        
        try:
            with open(arquivo_txt, 'r', encoding='utf-8', errors='ignore') as f:
                linhas = f.readlines()
            
            # Processar cada linha (assumindo formato específico do CAGED)
            for i, linha in enumerate(linhas):
                linha = linha.strip()
                if not linha or linha.startswith('#'):  # Pular linhas vazias e comentários
                    continue
                
                try:
                    # Parsear dados da linha (formato específico do CAGED)
                    dados_linha = self._parsear_linha_caged(linha, metadados)
                    if dados_linha:
                        entidade = self.criar_entidade_caged(metadados, dados_linha)
                        entidades.append(entidade)
                        
                except Exception as e:
                    self.logger.debug(f"⚠️  Erro na linha {i+1} de {arquivo_txt.name}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"❌ Erro ao ler arquivo {arquivo_txt}: {e}")
        
        return entidades
    
    def _processar_arquivo_csv_caged(self, arquivo_csv: Path, metadados: Dict) -> List[Any]:
        """
        Processa um arquivo .csv do CAGED e cria entidades
        
        Args:
            arquivo_csv: Caminho do arquivo .csv
            metadados: Metadados extraídos
            
        Returns:
            Lista de entidades criadas
        """
        entidades = []
        
        try:
            import pandas as pd
            
            # Ler CSV com tratamento de erros
            df = pd.read_csv(
                arquivo_csv, 
                encoding='utf-8', 
                sep=';',  # Separador comum em arquivos CAGED
                on_bad_lines='skip',
                low_memory=False
            )
            
            # Processar cada linha do DataFrame
            for index, row in df.iterrows():
                try:
                    dados_linha = self._converter_row_para_dict(row, metadados)
                    if dados_linha:
                        entidade = self.criar_entidade_caged(metadados, dados_linha)
                        entidades.append(entidade)
                        
                except Exception as e:
                    self.logger.debug(f"⚠️  Erro na linha {index+1} de {arquivo_csv.name}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"❌ Erro ao ler CSV {arquivo_csv}: {e}")
        
        return entidades
    
    def _parsear_linha_caged(self, linha: str, metadados: Dict) -> Optional[Dict]:
        """
        Parseia uma linha de arquivo .txt do CAGED
        
        Args:
            linha: Linha do arquivo
            metadados: Metadados do arquivo
            
        Returns:
            Dicionário com dados parseados ou None
        """
        try:
            # Formato típico do CAGED (ajustar conforme necessário)
            # Exemplo: CNPJ;UF;Município;Estabelecimento;Admissões;Desligamentos
            campos = linha.split(';')
            
            if len(campos) < 6:
                return None
            
            return {
                "cnpj": campos[0].strip(),
                "uf": campos[1].strip(),
                "municipio": campos[2].strip(),
                "estabelecimento": campos[3].strip(),
                "admissoes": int(campos[4].strip()) if campos[4].strip().isdigit() else 0,
                "desligamentos": int(campos[5].strip()) if campos[5].strip().isdigit() else 0,
                "saldo": int(campos[4].strip()) - int(campos[5].strip()) if campos[4].strip().isdigit() and campos[5].strip().isdigit() else 0
            }
            
        except Exception as e:
            self.logger.debug(f"Erro ao parsear linha: {e}")
            return None
    
    def _converter_row_para_dict(self, row, metadados: Dict) -> Optional[Dict]:
        """
        Converte uma linha do pandas DataFrame para dicionário
        
        Args:
            row: Linha do DataFrame
            metadados: Metadados do arquivo
            
        Returns:
            Dicionário com dados convertidos ou None
        """
        try:
            # Mapear colunas do CSV para campos da entidade
            # Ajustar conforme estrutura real dos arquivos CAGED
            dados = {}
            
            # Campos comuns
            if 'cnpj' in row.index:
                dados['cnpj'] = str(row['cnpj']).strip()
            elif 'CNPJ' in row.index:
                dados['cnpj'] = str(row['CNPJ']).strip()
            
            if 'uf' in row.index:
                dados['uf'] = str(row['uf']).strip()
            elif 'UF' in row.index:
                dados['uf'] = str(row['UF']).strip()
            
            if 'municipio' in row.index:
                dados['municipio'] = str(row['municipio']).strip()
            elif 'Município' in row.index:
                dados['municipio'] = str(row['Município']).strip()
            
            # Campos numéricos
            for campo_num in ['admissoes', 'desligamentos', 'saldo', 'saldo_inicial', 'saldo_final']:
                if campo_num in row.index:
                    try:
                        dados[campo_num] = int(float(str(row[campo_num]).replace(',', '.')))
                    except:
                        dados[campo_num] = 0
            
            return dados if dados else None
            
        except Exception as e:
            self.logger.debug(f"Erro ao converter row: {e}")
            return None
    
    def _processar_dados_mes(self, ano: int, mes: int, metadados_lista: List[Dict]) -> List[Any]:
        """
        Processa todos os dados dos arquivos descompactados de um mês específico
        
        Args:
            ano: Ano dos dados
            mes: Mês dos dados
            metadados_lista: Lista de metadados dos arquivos processados
            
        Returns:
            Lista de todas as entidades CAGED criadas
        """
        entidades_totais = []
        
        try:
            # Filtrar apenas metadados do mês específico
            metadados_mes = [
                meta for meta in metadados_lista 
                if meta.get('mes') == f"{mes:02d}" and 'destino' in meta
            ]
            
            if not metadados_mes:
                self.logger.warning(f"⚠️  Nenhum arquivo processado encontrado para {ano}/{mes:02d}")
                return entidades_totais
            
            # Processar cada arquivo descompactado
            for metadados in metadados_mes:
                destino = Path(metadados['destino'])
                
                if not destino.exists():
                    self.logger.warning(f"⚠️  Diretório não encontrado: {destino}")
                    continue
                
                # Encontrar arquivos .txt e .csv no diretório
                arquivos_dados = list(destino.glob("*.txt")) + list(destino.glob("*.csv"))
                
                for arquivo_dados in arquivos_dados:
                    try:
                        entidades = self.processar_dados_arquivo_caged(arquivo_dados, metadados)
                        entidades_totais.extend(entidades)
                        
                    except Exception as e:
                        self.logger.error(f"❌ Erro ao processar {arquivo_dados}: {e}")
                        continue
            
            # Criar indicadores adicionais baseados nas entidades processadas
            if entidades_totais:
                self._criar_indicadores_entidades(ano, mes, entidades_totais)
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao processar dados do mês {ano}/{mes:02d}: {e}")
        
        return entidades_totais
    
    def _criar_indicadores_entidades(self, ano: int, mes: int, entidades: List[Any]) -> List[Indicador]:
        """
        Cria indicadores baseados nas entidades processadas
        
        Args:
            ano: Ano dos dados
            mes: Mês dos dados
            entidades: Lista de entidades processadas
            
        Returns:
            Lista de indicadores criados
        """
        indicadores = []
        competencia = f"{ano}-{mes:02d}"
        
        try:
            # Contar entidades por tipo
            contadores = {
                'movimentacao': 0,
                'exclusao': 0,
                'fora_prazo': 0,
                'saldo_mensal': 0
            }
            
            # Estatísticas agregadas
            total_admissoes = 0
            total_desligamentos = 0
            total_saldo = 0
            ufs_processadas = set()
            
            for entidade in entidades:
                # Contar por tipo
                tipo_entidade = type(entidade).__name__.lower()
                if 'movimentacao' in tipo_entidade:
                    if 'fora_prazo' in tipo_entidade:
                        contadores['fora_prazo'] += 1
                    else:
                        contadores['movimentacao'] += 1
                elif 'exclusao' in tipo_entidade:
                    contadores['exclusao'] += 1
                elif 'saldo' in tipo_entidade:
                    contadores['saldo_mensal'] += 1
                
                # Agregar estatísticas
                if hasattr(entidade, 'admissoes'):
                    total_admissoes += getattr(entidade, 'admissoes', 0)
                if hasattr(entidade, 'desligamentos'):
                    total_desligamentos += getattr(entidade, 'desligamentos', 0)
                if hasattr(entidade, 'saldo'):
                    total_saldo += getattr(entidade, 'saldo', 0)
                if hasattr(entidade, 'uf'):
                    uf = getattr(entidade, 'uf', '')
                    if uf:
                        ufs_processadas.add(uf)
            
            # Criar indicadores
            indicadores.extend([
                Indicador(
                    id=0,
                    cnpj="SISTEMA",
                    competencia=competencia,
                    nome_indicador="TOTAL_ENTIDADES_PROCESSADAS",
                    valor=len(entidades)
                ),
                Indicador(
                    id=0,
                    cnpj="SISTEMA",
                    competencia=competencia,
                    nome_indicador="TOTAL_MOVIMENTACOES",
                    valor=contadores['movimentacao']
                ),
                Indicador(
                    id=0,
                    cnpj="SISTEMA",
                    competencia=competencia,
                    nome_indicador="TOTAL_EXCLUSOES",
                    valor=contadores['exclusao']
                ),
                Indicador(
                    id=0,
                    cnpj="SISTEMA",
                    competencia=competencia,
                    nome_indicador="TOTAL_FORA_PRAZO",
                    valor=contadores['fora_prazo']
                ),
                Indicador(
                    id=0,
                    cnpj="SISTEMA",
                    competencia=competencia,
                    nome_indicador="TOTAL_ADMISSOES",
                    valor=total_admissoes
                ),
                Indicador(
                    id=0,
                    cnpj="SISTEMA",
                    competencia=competencia,
                    nome_indicador="TOTAL_DESLIGAMENTOS",
                    valor=total_desligamentos
                ),
                Indicador(
                    id=0,
                    cnpj="SISTEMA",
                    competencia=competencia,
                    nome_indicador="SALDO_LIQUIDO",
                    valor=total_saldo
                ),
                Indicador(
                    id=0,
                    cnpj="SISTEMA",
                    competencia=competencia,
                    nome_indicador="UFS_PROCESSADAS",
                    valor=len(ufs_processadas)
                )
            ])
            
            self.logger.info(f"📊 {len(indicadores)} indicadores criados para {competencia}")
            self.logger.info(f"   📈 {len(entidades)} entidades, {len(ufs_processadas)} UFs, {total_admissoes} admissões, {total_desligamentos} desligamentos")
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao criar indicadores para {competencia}: {e}")
        
        return indicadores


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