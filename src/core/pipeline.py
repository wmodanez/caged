#!/usr/bin/env python3
"""
Pipeline Principal do Sistema CAGED
Implementação do Item 1.4 do Plano de Melhorias

Este módulo define o pipeline principal de processamento de dados.
"""

import asyncio
import logging
import psutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any, Callable, Union, Coroutine
from enum import Enum
from contextlib import asynccontextmanager

from core.config import CAGEDConfig, get_config
from core.exceptions import PipelineError, ValidationError
from utils.logger import setup_logger
from utils.cache import cache_manager


class ProcessingStage(Enum):
    """Estágios de processamento"""
    DOWNLOAD = "download"
    EXTRACT = "extract"
    CONVERT = "convert"
    VALIDATE = "validate"
    CLEANUP = "cleanup"


class ProcessingStatus(Enum):
    """Status de processamento"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ProcessingItem:
    """Item de processamento"""
    id: str
    ano: int
    mes: int
    stages: List[ProcessingStage]
    status: ProcessingStatus = ProcessingStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class ProcessingResult:
    """Resultado de processamento"""
    item: ProcessingItem
    success: bool
    duration: float
    files_processed: int = 0
    bytes_processed: int = 0
    errors: List[str] = None
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []


class PipelineStageHandler:
    """Handler base para estágios do pipeline"""
    
    def __init__(self, config: CAGEDConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
    
    async def process(self, item: ProcessingItem) -> ProcessingResult:
        """Processa um item"""
        raise NotImplementedError("Subclasses devem implementar o método process")
    
    def validate_item(self, item: ProcessingItem) -> bool:
        """Valida se o item pode ser processado por este handler"""
        return True


class CAGEDPipeline:
    """Pipeline principal do sistema CAGED"""
    
    def __init__(self, config: Optional[CAGEDConfig] = None):
        self.config = config or get_config()
        self.logger = setup_logger("pipeline", self.config.logging.level)
        
        # Handlers para cada estágio
        self._stage_handlers: Dict[ProcessingStage, PipelineStageHandler] = {}
        
        # Estatísticas
        self._stats = {
            "total_items": 0,
            "completed_items": 0,
            "failed_items": 0,
            "skipped_items": 0,
            "start_time": None,
            "end_time": None,
        }
        
        # Callbacks
        self._progress_callback: Optional[Callable] = None
        self._error_callback: Optional[Callable] = None
    
    def register_stage_handler(self, stage: ProcessingStage, handler: PipelineStageHandler):
        """Registra handler para um estágio"""
        self._stage_handlers[stage] = handler
        self.logger.info(f"Handler registrado para estágio {stage.value}")
    
    def set_progress_callback(self, callback: Callable[[ProcessingItem, ProcessingStage, float], None]):
        """Define callback de progresso"""
        self._progress_callback = callback
    
    def set_error_callback(self, callback: Callable[[ProcessingItem, Exception], None]):
        """Define callback de erro"""
        self._error_callback = callback
    
    async def process_items(self, items: List[ProcessingItem], 
                          parallel: bool = None) -> List[ProcessingResult]:
        """Processa lista de itens"""
        if parallel is None:
            parallel = self.config.processing.enable_parallel
        
        self._stats["total_items"] = len(items)
        self._stats["start_time"] = datetime.now()
        
        self.logger.info(f"Iniciando processamento de {len(items)} itens (paralelo: {parallel})")
        
        try:
            if parallel and len(items) > 1:
                results = await self._process_parallel(items)
            else:
                results = await self._process_sequential(items)
            
            self._stats["end_time"] = datetime.now()
            self._log_final_stats(results)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Erro no pipeline: {e}")
            raise PipelineError(f"Falha no processamento: {e}")
    
    async def _process_sequential(self, items: List[ProcessingItem]) -> List[ProcessingResult]:
        """Processamento sequencial"""
        results = []
        
        for i, item in enumerate(items):
            try:
                self.logger.info(f"Processando item {i+1}/{len(items)}: {item.id}")
                result = await self._process_single_item(item)
                results.append(result)
                
                # Callback de progresso
                if self._progress_callback:
                    progress = (i + 1) / len(items)
                    self._progress_callback(item, None, progress)
                    
            except Exception as e:
                self.logger.error(f"Erro ao processar item {item.id}: {e}")
                result = ProcessingResult(
                    item=item,
                    success=False,
                    duration=0.0,
                    errors=[str(e)]
                )
                results.append(result)
                
                if self._error_callback:
                    self._error_callback(item, e)
        
        return results
    
    async def _process_parallel(self, items: List[ProcessingItem]) -> List[ProcessingResult]:
        """Processamento paralelo"""
        max_workers = min(self.config.processing.max_workers, len(items))
        self.logger.info(f"Processamento paralelo com {max_workers} workers")
        
        # Criar tasks para processamento assíncrono
        tasks = []
        semaphore = asyncio.Semaphore(max_workers)
        
        async def process_with_semaphore(item: ProcessingItem) -> ProcessingResult:
            async with semaphore:
                return await self._process_single_item(item)
        
        # Criar todas as tasks
        for item in items:
            task = asyncio.create_task(process_with_semaphore(item))
            tasks.append(task)
        
        # Aguardar conclusão de todas as tasks
        results = []
        completed = 0
        
        for task in asyncio.as_completed(tasks):
            try:
                result = await task
                results.append(result)
                completed += 1
                
                # Callback de progresso
                if self._progress_callback:
                    progress = completed / len(items)
                    self._progress_callback(result.item, None, progress)
                    
            except Exception as e:
                self.logger.error(f"Erro em task paralela: {e}")
                # Criar resultado de erro
                error_result = ProcessingResult(
                    item=ProcessingItem(id="unknown", ano=0, mes=0, stages=[]),
                    success=False,
                    duration=0.0,
                    errors=[str(e)]
                )
                results.append(error_result)
        
        return results
    
    async def _process_single_item(self, item: ProcessingItem) -> ProcessingResult:
        """Processa um único item através de todos os estágios"""
        item.status = ProcessingStatus.RUNNING
        item.start_time = datetime.now()
        
        total_files = 0
        total_bytes = 0
        all_errors = []
        all_warnings = []
        
        try:
            for stage in item.stages:
                if stage not in self._stage_handlers:
                    warning = f"Handler não encontrado para estágio {stage.value}"
                    self.logger.warning(warning)
                    all_warnings.append(warning)
                    continue
                
                handler = self._stage_handlers[stage]
                
                # Validar se o handler pode processar o item
                if not handler.validate_item(item):
                    warning = f"Item {item.id} não pode ser processado pelo handler {stage.value}"
                    self.logger.warning(warning)
                    all_warnings.append(warning)
                    continue
                
                self.logger.debug(f"Executando estágio {stage.value} para item {item.id}")
                
                # Callback de progresso por estágio
                if self._progress_callback:
                    stage_progress = item.stages.index(stage) / len(item.stages)
                    self._progress_callback(item, stage, stage_progress)
                
                # Processar estágio
                stage_result = await handler.process(item)
                
                if not stage_result.success:
                    all_errors.extend(stage_result.errors)
                    break
                
                total_files += stage_result.files_processed
                total_bytes += stage_result.bytes_processed
                all_warnings.extend(stage_result.warnings)
            
            # Determinar sucesso geral
            success = len(all_errors) == 0
            
            if success:
                item.status = ProcessingStatus.COMPLETED
                self._stats["completed_items"] += 1
            else:
                item.status = ProcessingStatus.FAILED
                self._stats["failed_items"] += 1
            
            item.end_time = datetime.now()
            duration = (item.end_time - item.start_time).total_seconds()
            
            return ProcessingResult(
                item=item,
                success=success,
                duration=duration,
                files_processed=total_files,
                bytes_processed=total_bytes,
                errors=all_errors,
                warnings=all_warnings
            )
            
        except Exception as e:
            item.status = ProcessingStatus.FAILED
            item.end_time = datetime.now()
            self._stats["failed_items"] += 1
            
            error_msg = f"Erro inesperado no processamento: {e}"
            self.logger.error(error_msg)
            
            duration = (item.end_time - item.start_time).total_seconds() if item.start_time else 0.0
            
            return ProcessingResult(
                item=item,
                success=False,
                duration=duration,
                errors=[error_msg]
            )
    
    def _log_final_stats(self, results: List[ProcessingResult]):
        """Log das estatísticas finais"""
        total_duration = (self._stats["end_time"] - self._stats["start_time"]).total_seconds()
        
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        
        total_files = sum(r.files_processed for r in results)
        total_bytes = sum(r.bytes_processed for r in results)
        
        self.logger.info("=== ESTATÍSTICAS FINAIS ===")
        self.logger.info(f"📊 Total de itens: {len(results)}")
        self.logger.info(f"✅ Sucessos: {successful}")
        self.logger.info(f"❌ Falhas: {failed}")
        self.logger.info(f"📁 Arquivos processados: {total_files}")
        self.logger.info(f"💾 Bytes processados: {total_bytes:,}")
        self.logger.info(f"⏱️ Tempo total: {total_duration:.2f}s")
        
        if total_duration > 0:
            throughput = total_files / total_duration
            self.logger.info(f"🚀 Throughput: {throughput:.2f} arquivos/s")
    
    def get_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas do pipeline"""
        return self._stats.copy()
    
    def create_processing_item(self, ano: int, mes: int, 
                             stages: List[ProcessingStage]) -> ProcessingItem:
        """Cria item de processamento"""
        item_id = f"{ano:04d}-{mes:02d}"
        return ProcessingItem(
            id=item_id,
            ano=ano,
            mes=mes,
            stages=stages
        )
    
    def create_date_range_items(self, ano_inicio: int, mes_inicio: int,
                              ano_fim: int, mes_fim: int,
                              stages: List[ProcessingStage]) -> List[ProcessingItem]:
        """Cria itens para uma faixa de datas"""
        items = []
        
        current_ano = ano_inicio
        current_mes = mes_inicio
        
        while (current_ano < ano_fim) or (current_ano == ano_fim and current_mes <= mes_fim):
            item = self.create_processing_item(current_ano, current_mes, stages)
            items.append(item)
            
            # Avançar para próximo mês
            current_mes += 1
            if current_mes > 12:
                current_mes = 1
                current_ano += 1
        
        return items


@dataclass
class ResourceMonitor:
    """Monitor de recursos do sistema"""
    cpu_threshold: float = 80.0  # Porcentagem
    memory_threshold: float = 80.0  # Porcentagem
    disk_threshold: float = 90.0  # Porcentagem
    check_interval: float = 1.0  # Segundos
    
    def check_resources(self) -> Dict[str, Any]:
        """Verifica recursos do sistema"""
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('.')
        
        return {
            'cpu_percent': cpu_percent,
            'memory_percent': memory.percent,
            'disk_percent': (disk.used / disk.total) * 100,
            'memory_available_gb': memory.available / (1024**3),
            'disk_free_gb': disk.free / (1024**3),
            'cpu_overload': cpu_percent > self.cpu_threshold,
            'memory_overload': memory.percent > self.memory_threshold,
            'disk_overload': (disk.used / disk.total) * 100 > self.disk_threshold
        }
    
    def should_throttle(self) -> bool:
        """Verifica se deve reduzir a carga"""
        resources = self.check_resources()
        return any([
            resources['cpu_overload'],
            resources['memory_overload'],
            resources['disk_overload']
        ])


class ConnectionPool:
    """Pool de conexões para operações paralelas"""
    
    def __init__(self, max_connections: int = 5, connection_factory: Callable = None):
        self.max_connections = max_connections
        self.connection_factory = connection_factory
        self._pool: List[Any] = []
        self._in_use: List[Any] = []
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger("connection_pool")
    
    async def initialize(self):
        """Inicializa o pool de conexões"""
        if not self.connection_factory:
            return
        
        self.logger.info(f"🔗 Inicializando pool com {self.max_connections} conexões")
        
        for i in range(self.max_connections):
            try:
                conn = await self._create_connection()
                if conn:
                    self._pool.append(conn)
                    self.logger.debug(f"✅ Conexão {i+1} criada")
            except Exception as e:
                self.logger.warning(f"⚠️ Falha ao criar conexão {i+1}: {e}")
    
    async def _create_connection(self):
        """Cria nova conexão"""
        if self.connection_factory:
            return await self.connection_factory()
        return None
    
    @asynccontextmanager
    async def get_connection(self):
        """Context manager para obter conexão do pool"""
        connection = None
        try:
            async with self._lock:
                if self._pool:
                    connection = self._pool.pop()
                    self._in_use.append(connection)
                else:
                    # Criar nova conexão se pool vazio
                    connection = await self._create_connection()
                    if connection:
                        self._in_use.append(connection)
            
            if connection:
                yield connection
            else:
                raise Exception("Não foi possível obter conexão")
        
        finally:
            if connection:
                async with self._lock:
                    if connection in self._in_use:
                        self._in_use.remove(connection)
                    
                    # Verificar se conexão ainda é válida
                    if await self._is_connection_valid(connection):
                        self._pool.append(connection)
                    else:
                        await self._close_connection(connection)
    
    async def _is_connection_valid(self, connection) -> bool:
        """Verifica se conexão ainda é válida"""
        # Implementação específica para cada tipo de conexão
        return True
    
    async def _close_connection(self, connection):
        """Fecha conexão"""
        try:
            if hasattr(connection, 'close'):
                await connection.close()
            elif hasattr(connection, 'quit'):
                connection.quit()
        except:
            pass
    
    async def close_all(self):
        """Fecha todas as conexões"""
        async with self._lock:
            all_connections = self._pool + self._in_use
            for conn in all_connections:
                await self._close_connection(conn)
            self._pool.clear()
            self._in_use.clear()
    
    def get_stats(self) -> Dict[str, int]:
        """Retorna estatísticas do pool"""
        return {
            'available': len(self._pool),
            'in_use': len(self._in_use),
            'total': len(self._pool) + len(self._in_use),
            'max_connections': self.max_connections
        }


class ParallelPipeline(CAGEDPipeline):
    """Pipeline com processamento paralelo avançado"""
    
    def __init__(self, config: Optional[CAGEDConfig] = None):
        super().__init__(config)
        self.logger = setup_logger("parallel_pipeline", self.config.logging.level)
        
        # Configurações de paralelismo
        self.max_workers = self.config.processing.max_workers
        self.resource_monitor = ResourceMonitor()
        self.connection_pool: Optional[ConnectionPool] = None
        
        # Controle de throttling
        self._throttle_enabled = True
        self._current_workers = self.max_workers
        self._last_resource_check = 0
        
        # Métricas avançadas
        self._parallel_stats = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'throttle_events': 0,
            'resource_checks': 0,
            'avg_task_duration': 0.0,
            'peak_workers': 0,
            'cache_hits': 0,
            'cache_misses': 0
        }
    
    def set_connection_pool(self, pool: ConnectionPool):
        """Define pool de conexões"""
        self.connection_pool = pool
        self.logger.info("🔗 Pool de conexões configurado")
    
    async def process_items_parallel(self, items: List[ProcessingItem], 
                                   enable_throttling: bool = True,
                                   batch_size: Optional[int] = None) -> List[ProcessingResult]:
        """Processamento paralelo avançado com controle de recursos"""
        self._throttle_enabled = enable_throttling
        self._parallel_stats['total_tasks'] = len(items)
        
        if batch_size is None:
            batch_size = min(len(items), self.max_workers * 2)
        
        self.logger.info(f"🚀 Iniciando processamento paralelo avançado")
        self.logger.info(f"📊 Itens: {len(items)}, Workers: {self.max_workers}, Batch: {batch_size}")
        
        try:
            # Inicializar pool de conexões se disponível
            if self.connection_pool:
                await self.connection_pool.initialize()
            
            # Processar em batches para controle de memória
            all_results = []
            
            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(items) + batch_size - 1) // batch_size
                
                self.logger.info(f"📦 Processando batch {batch_num}/{total_batches} ({len(batch)} itens)")
                
                batch_results = await self._process_batch_with_monitoring(batch)
                all_results.extend(batch_results)
                
                # Log de progresso
                completed = len(all_results)
                progress = (completed / len(items)) * 100
                self.logger.info(f"📈 Progresso: {completed}/{len(items)} ({progress:.1f}%)")
                
                # Pequena pausa entre batches para não sobrecarregar
                if i + batch_size < len(items):
                    await asyncio.sleep(0.1)
            
            self._log_parallel_stats()
            return all_results
            
        except Exception as e:
            self.logger.error(f"❌ Erro no processamento paralelo: {e}")
            raise PipelineError(f"Falha no processamento paralelo: {e}") from e
        
        finally:
            # Fechar pool de conexões
            if self.connection_pool:
                await self.connection_pool.close_all()
    
    async def _process_batch_with_monitoring(self, batch: List[ProcessingItem]) -> List[ProcessingResult]:
        """Processa batch com monitoramento de recursos"""
        # Ajustar número de workers baseado nos recursos
        if self._throttle_enabled:
            await self._adjust_workers_based_on_resources()
        
        # Criar semáforo para controlar concorrência
        semaphore = asyncio.Semaphore(self._current_workers)
        
        # Criar tasks com monitoramento
        tasks = []
        for item in batch:
            task = asyncio.create_task(
                self._process_item_with_monitoring(item, semaphore)
            )
            tasks.append(task)
        
        # Aguardar conclusão com monitoramento contínuo
        results = []
        monitor_task = asyncio.create_task(self._monitor_resources_continuously())
        
        try:
            # Processar tasks conforme completam
            for coro in asyncio.as_completed(tasks):
                try:
                    result = await coro
                    results.append(result)
                    
                    if result.success:
                        self._parallel_stats['completed_tasks'] += 1
                    else:
                        self._parallel_stats['failed_tasks'] += 1
                    
                    # Atualizar estatísticas de duração
                    self._update_duration_stats(result.duration)
                    
                except Exception as e:
                    self.logger.error(f"❌ Erro em task: {e}")
                    self._parallel_stats['failed_tasks'] += 1
        
        finally:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
        
        return results
    
    async def _process_item_with_monitoring(self, item: ProcessingItem, 
                                          semaphore: asyncio.Semaphore) -> ProcessingResult:
        """Processa item individual com monitoramento"""
        async with semaphore:
            start_time = time.time()
            
            try:
                # Verificar cache primeiro
                cache_key = f"item_{item.id}_{hash(tuple(item.stages))}"
                
                if cache_manager.has_cached_item(cache_key):
                    self.logger.debug(f"💾 Cache hit para item {item.id}")
                    self._parallel_stats['cache_hits'] += 1
                    
                    cached_result = cache_manager.get_cached_item(cache_key)
                    if cached_result:
                        return cached_result
                
                self._parallel_stats['cache_misses'] += 1
                
                # Processar item normalmente
                result = await self._process_single_item(item)
                
                # Cachear resultado se bem-sucedido
                if result.success:
                    cache_manager.cache_item(cache_key, result, category="results")
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                self.logger.error(f"❌ Erro ao processar item {item.id}: {e}")
                
                return ProcessingResult(
                    item=item,
                    success=False,
                    duration=duration,
                    errors=[str(e)]
                )
    
    async def _adjust_workers_based_on_resources(self):
        """Ajusta número de workers baseado nos recursos disponíveis"""
        current_time = time.time()
        
        # Verificar recursos apenas periodicamente
        if current_time - self._last_resource_check < 5.0:  # 5 segundos
            return
        
        self._last_resource_check = current_time
        self._parallel_stats['resource_checks'] += 1
        
        resources = self.resource_monitor.check_resources()
        
        # Ajustar workers baseado na carga
        if resources['cpu_overload'] or resources['memory_overload']:
            # Reduzir workers
            new_workers = max(1, self._current_workers - 1)
            if new_workers != self._current_workers:
                self._current_workers = new_workers
                self._parallel_stats['throttle_events'] += 1
                self.logger.warning(
                    f"🐌 Throttling: reduzindo workers para {self._current_workers} "
                    f"(CPU: {resources['cpu_percent']:.1f}%, "
                    f"Mem: {resources['memory_percent']:.1f}%)"
                )
        
        elif not self.resource_monitor.should_throttle():
            # Aumentar workers se recursos disponíveis
            new_workers = min(self.max_workers, self._current_workers + 1)
            if new_workers != self._current_workers:
                self._current_workers = new_workers
                self.logger.info(f"🚀 Aumentando workers para {self._current_workers}")
        
        # Atualizar pico de workers
        self._parallel_stats['peak_workers'] = max(
            self._parallel_stats['peak_workers'], 
            self._current_workers
        )
    
    async def _monitor_resources_continuously(self):
        """Monitor contínuo de recursos durante processamento"""
        try:
            while True:
                await asyncio.sleep(2.0)  # Verificar a cada 2 segundos
                
                if self._throttle_enabled:
                    await self._adjust_workers_based_on_resources()
                
                # Log de recursos se debug habilitado
                if self.config.debug_mode:
                    resources = self.resource_monitor.check_resources()
                    self.logger.debug(
                        f"📊 Recursos: CPU {resources['cpu_percent']:.1f}%, "
                        f"Mem {resources['memory_percent']:.1f}%, "
                        f"Workers {self._current_workers}"
                    )
        
        except asyncio.CancelledError:
            pass
    
    def _update_duration_stats(self, duration: float):
        """Atualiza estatísticas de duração"""
        completed = self._parallel_stats['completed_tasks']
        if completed > 0:
            current_avg = self._parallel_stats['avg_task_duration']
            new_avg = (current_avg * (completed - 1) + duration) / completed
            self._parallel_stats['avg_task_duration'] = new_avg
    
    def _log_parallel_stats(self):
        """Log das estatísticas de processamento paralelo"""
        stats = self._parallel_stats
        
        self.logger.info("=== ESTATÍSTICAS PROCESSAMENTO PARALELO ===")
        self.logger.info(f"📊 Total de tasks: {stats['total_tasks']}")
        self.logger.info(f"✅ Completadas: {stats['completed_tasks']}")
        self.logger.info(f"❌ Falharam: {stats['failed_tasks']}")
        self.logger.info(f"⚡ Workers pico: {stats['peak_workers']}")
        self.logger.info(f"🐌 Eventos throttle: {stats['throttle_events']}")
        self.logger.info(f"🔍 Verificações recursos: {stats['resource_checks']}")
        self.logger.info(f"💾 Cache hits: {stats['cache_hits']}")
        self.logger.info(f"💿 Cache misses: {stats['cache_misses']}")
        
        if stats['completed_tasks'] > 0:
            cache_hit_rate = (stats['cache_hits'] / (stats['cache_hits'] + stats['cache_misses'])) * 100
            self.logger.info(f"📈 Taxa cache hit: {cache_hit_rate:.1f}%")
            self.logger.info(f"⏱️ Duração média task: {stats['avg_task_duration']:.2f}s")
        
        # Log de recursos finais
        resources = self.resource_monitor.check_resources()
        self.logger.info(f"💻 Recursos finais: CPU {resources['cpu_percent']:.1f}%, Mem {resources['memory_percent']:.1f}%")
    
    def get_parallel_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas de processamento paralelo"""
        stats = self._parallel_stats.copy()
        stats['current_workers'] = self._current_workers
        stats['max_workers'] = self.max_workers
        stats['throttle_enabled'] = self._throttle_enabled
        
        if self.connection_pool:
            stats['connection_pool'] = self.connection_pool.get_stats()
        
        return stats


# Funções utilitárias para criar pipelines
def create_pipeline(config: Optional[CAGEDConfig] = None) -> CAGEDPipeline:
    """Cria instância do pipeline básico com handlers registrados"""
    pipeline = CAGEDPipeline(config)
    
    # Registrar handlers de estágio
    from src.core.stage_handlers import create_stage_handlers
    handlers = create_stage_handlers(pipeline.config)
    
    for stage, handler in handlers.items():
        pipeline.register_stage_handler(stage, handler)
    
    return pipeline


def create_parallel_pipeline(config: Optional[CAGEDConfig] = None) -> ParallelPipeline:
    """Cria instância do pipeline paralelo avançado com handlers registrados"""
    pipeline = ParallelPipeline(config)
    
    # Registrar handlers de estágio
    from src.core.stage_handlers import create_stage_handlers
    handlers = create_stage_handlers(pipeline.config)
    
    for stage, handler in handlers.items():
        pipeline.register_stage_handler(stage, handler)
    
    return pipeline