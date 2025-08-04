#!/usr/bin/env python3
"""
Pipeline Principal do Sistema CAGED
Implementação do Item 1.4 do Plano de Melhorias

Este módulo define o pipeline principal de processamento de dados.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any, Callable, Union
from enum import Enum

from .config import CAGEDConfig, get_config
from .exceptions import PipelineError, ValidationError
from ..utils.logger import setup_logger


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


# Função utilitária para criar pipeline
def create_pipeline(config: Optional[CAGEDConfig] = None) -> CAGEDPipeline:
    """Cria instância do pipeline"""
    return CAGEDPipeline(config)