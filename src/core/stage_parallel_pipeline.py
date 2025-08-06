#!/usr/bin/env python3
"""
Implementação de Pipeline com Estágios Paralelos para CAGED

Este módulo implementa um pipeline que executa os estágios de processamento
(download, extração, conversão) em paralelo para cada item.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable, Tuple
from pathlib import Path

from src.core.config import CAGEDConfig
from src.core.pipeline import (
    CAGEDPipeline, ProcessingItem, ProcessingResult, ProcessingStage,
    PipelineStageHandler, ResourceMonitor
)
from src.core.exceptions import PipelineError
from src.utils.logger import setup_logger
from src.utils.metrics import record_operation


class StageParallelPipeline(CAGEDPipeline):
    """
    Pipeline que executa estágios em paralelo para cada item
    
    Esta implementação permite que assim que um estágio é concluído para um item,
    o próximo estágio comece imediatamente, sem esperar que o mesmo estágio
    seja concluído para todos os itens.
    
    Exemplo:
    - Item 1: Download concluído -> Inicia extração imediatamente
    - Item 2: Download ainda em andamento
    """
    
    def __init__(self, config: Optional[CAGEDConfig] = None):
        super().__init__(config)
        self.logger = setup_logger("stage_parallel_pipeline", self.config.logging.level)
        
        # Configurações de paralelismo
        self.max_workers = self.config.processing.max_workers
        self.resource_monitor = ResourceMonitor()
        
        # Filas de processamento para cada estágio
        self._stage_queues = {}
        for stage in ProcessingStage:
            self._stage_queues[stage] = asyncio.Queue()
        
        # Estatísticas
        self._stats = {
            "items_processed": 0,
            "stages_completed": 0,
            "success_count": 0,
            "failure_count": 0,
            "start_time": None,
            "end_time": None
        }
    
    async def process_items_with_parallel_stages(self, items: List[ProcessingItem]) -> List[ProcessingResult]:
        """
        Processa itens com estágios em paralelo
        
        Esta função implementa um pipeline onde cada estágio é executado em paralelo,
        e assim que um estágio é concluído para um item, o próximo estágio é iniciado
        imediatamente para esse item.
        """
        self.logger.info(f"🚀 Iniciando pipeline com estágios paralelos para {len(items)} itens")
        self._stats["start_time"] = datetime.now()
        pipeline_start_time = time.time()
        
        # Resultados finais
        results = []
        
        try:
            # Criar workers para cada estágio
            workers = []
            for stage in ProcessingStage:
                if stage in self._stage_handlers:
                    for _ in range(self.max_workers):
                        worker = asyncio.create_task(
                            self._stage_worker(stage, self._stage_queues[stage])
                        )
                        workers.append(worker)
            
            # Criar task para monitorar conclusão
            completion_queue = asyncio.Queue()
            completion_monitor = asyncio.create_task(
                self._completion_monitor(completion_queue, len(items))
            )
            
            # Colocar itens na fila do primeiro estágio
            for item in items:
                # Determinar primeiro estágio para o item
                first_stage = self._get_first_stage(item)
                if first_stage:
                    # Criar cópia do item com informações de rastreamento
                    tracked_item = self._create_tracked_item(item, completion_queue)
                    await self._stage_queues[first_stage].put(tracked_item)
            
            # Aguardar conclusão de todos os itens
            results = await completion_monitor
            
            # Cancelar workers
            for worker in workers:
                worker.cancel()
            
            # Registrar métricas
            pipeline_duration = time.time() - pipeline_start_time
            pipeline_success = all(r.success for r in results)
            record_operation(
                "pipeline_parallel_stages", 
                pipeline_success, 
                pipeline_duration,
                {
                    "total_items": len(items),
                    "successful_items": self._stats["success_count"],
                    "failed_items": self._stats["failure_count"],
                }
            )
            
            self._stats["end_time"] = datetime.now()
            self._log_stats(results)
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Erro no pipeline com estágios paralelos: {e}")
            raise PipelineError(f"Falha no pipeline com estágios paralelos: {e}") from e
    
    def _create_tracked_item(self, item: ProcessingItem, completion_queue: asyncio.Queue) -> Dict:
        """
        Cria item com informações de rastreamento para o pipeline
        """
        return {
            "item": item,
            "completion_queue": completion_queue,
            "start_time": time.time(),
            "stage_results": {},
            "current_stage": None,
            "next_stage": None,
            "errors": [],
            "warnings": []
        }
    
    def _get_first_stage(self, item: ProcessingItem) -> Optional[ProcessingStage]:
        """
        Determina o primeiro estágio para o item
        """
        for stage in ProcessingStage:
            if stage in self._stage_handlers and stage in item.stages:
                return stage
        return None
    
    def _get_next_stage(self, item: ProcessingItem, current_stage: ProcessingStage) -> Optional[ProcessingStage]:
        """
        Determina o próximo estágio para o item
        """
        stages = list(ProcessingStage)
        try:
            current_index = stages.index(current_stage)
            for i in range(current_index + 1, len(stages)):
                next_stage = stages[i]
                if next_stage in self._stage_handlers and next_stage in item.stages:
                    return next_stage
        except ValueError:
            pass
        return None
    
    async def _stage_worker(self, stage: ProcessingStage, queue: asyncio.Queue):
        """
        Worker para processar itens de um estágio específico
        """
        handler = self._stage_handlers[stage]
        
        while True:
            try:
                # Obter próximo item da fila
                tracked_item = await queue.get()
                item = tracked_item["item"]
                completion_queue = tracked_item["completion_queue"]
                
                # Atualizar informações de rastreamento
                tracked_item["current_stage"] = stage
                
                try:
                    # Processar estágio
                    self.logger.info(f"🔄 Processando estágio {stage.value} para item {item.id}")
                    stage_start_time = time.time()
                    
                    # Callback de progresso
                    if self._progress_callback:
                        stage_index = list(ProcessingStage).index(stage)
                        progress = stage_index / len(ProcessingStage)
                        self._progress_callback(item, stage, progress)
                    
                    # Executar handler do estágio
                    result = await handler.process(item)
                    
                    # Registrar resultado do estágio
                    tracked_item["stage_results"][stage] = result
                    
                    # Verificar se o estágio foi bem-sucedido
                    if not result.success:
                        # Registrar erro
                        if result.errors:
                            tracked_item["errors"].extend(result.errors)
                        else:
                            tracked_item["errors"].append(f"Falha no estágio {stage.value} sem mensagem de erro específica")
                        
                        # Item com erro, enviar para fila de conclusão
                        await completion_queue.put(self._create_error_result(tracked_item, Exception(f"Falha no estágio {stage.value}")))
                    else:
                        # Determinar próximo estágio
                        next_stage = self._get_next_stage(item, stage)
                        tracked_item["next_stage"] = next_stage
                        
                        if next_stage:
                            # Enviar para próximo estágio apenas se não houver erros
                            if not tracked_item["errors"]:
                                await self._stage_queues[next_stage].put(tracked_item)
                            else:
                                # Se houver erros, enviar para fila de conclusão
                                await completion_queue.put(self._create_error_result(tracked_item, Exception(f"Falha anterior ao estágio {next_stage.value}")))
                        else:
                            # Item concluído, enviar para fila de conclusão
                            await completion_queue.put(self._create_final_result(tracked_item))
                    
                    # Atualizar estatísticas
                    self._stats["stages_completed"] += 1
                    
                except Exception as e:
                    self.logger.error(f"❌ Erro no estágio {stage.value} para item {item.id}: {e}")
                    tracked_item["errors"].append(str(e))
                    
                    # Item com erro, enviar para fila de conclusão
                    await completion_queue.put(self._create_error_result(tracked_item, e))
                    
                    # Callback de erro
                    if self._error_callback:
                        self._error_callback(item, e)
                
                finally:
                    # Marcar tarefa como concluída
                    queue.task_done()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"❌ Erro não tratado no worker do estágio {stage.value}: {e}")
    
    async def _completion_monitor(self, queue: asyncio.Queue, total_items: int) -> List[ProcessingResult]:
        """
        Monitora a conclusão de itens
        """
        results = []
        completed = 0
        
        while completed < total_items:
            # Aguardar próximo item concluído
            result = await queue.get()
            results.append(result)
            completed += 1
            
            # Atualizar estatísticas
            self._stats["items_processed"] = completed
            
            # Log de progresso com contagem de sucessos e falhas
            progress = completed / total_items
            self.logger.info(f"📈 Progresso: {completed}/{total_items} ({progress:.1%}) - ✅ {self._stats['success_count']} sucessos, ❌ {self._stats['failure_count']} falhas")
            
            # Marcar tarefa como concluída
            queue.task_done()
        
        return results
    
    def _create_final_result(self, tracked_item: Dict) -> ProcessingResult:
        """
        Cria resultado final a partir dos resultados de estágios
        """
        item = tracked_item["item"]
        stage_results = tracked_item["stage_results"]
        warnings = tracked_item["warnings"]
        errors = tracked_item["errors"]
        
        # Calcular duração total
        duration = time.time() - tracked_item["start_time"]
        
        # Agregar estatísticas de todos os estágios
        files_processed = sum(r.files_processed for r in stage_results.values() if hasattr(r, 'files_processed'))
        bytes_processed = sum(r.bytes_processed for r in stage_results.values() if hasattr(r, 'bytes_processed'))
        
        # Agregar avisos de todos os estágios
        for result in stage_results.values():
            if hasattr(result, 'warnings') and result.warnings:
                warnings.extend(result.warnings)
            # Verificar se algum estágio falhou
            if hasattr(result, 'errors') and result.errors:
                errors.extend(result.errors)
        
        # Verificar se há erros
        success = len(errors) == 0
        
        # Incrementar contador de sucessos apenas se não houver erros
        if success:
            self._stats["success_count"] += 1
        else:
            # Se houver erros, incrementar contador de falhas
            self._stats["failure_count"] += 1
        
        return ProcessingResult(
            item=item,
            success=success,
            duration=duration,
            files_processed=files_processed,
            bytes_processed=bytes_processed,
            warnings=warnings,
            errors=errors
        )
    
    def _create_error_result(self, tracked_item: Dict, error: Exception) -> ProcessingResult:
        """
        Cria resultado de erro
        """
        item = tracked_item["item"]
        errors = tracked_item["errors"]
        warnings = tracked_item["warnings"]
        
        # Adicionar mensagem de erro da exceção se não estiver já nos erros
        error_msg = str(error)
        if error_msg not in errors:
            errors.append(error_msg)
            
        # Calcular duração até o erro
        duration = time.time() - tracked_item["start_time"]
        
        # Incrementar contador de falhas
        self._stats["failure_count"] += 1
        
        return ProcessingResult(
            item=item,
            success=False,
            duration=duration,
            files_processed=0,
            bytes_processed=0,
            errors=errors,
            warnings=warnings
        )
    
    def _log_stats(self, results: List[ProcessingResult]):
        """
        Registra estatísticas do pipeline
        """
        duration = (self._stats["end_time"] - self._stats["start_time"]).total_seconds()
        
        # Contar explicitamente os sucessos e falhas nos resultados
        actual_success_count = sum(1 for r in results if r.success)
        actual_failure_count = sum(1 for r in results if not r.success)
        
        # Atualizar estatísticas para garantir precisão
        self._stats["success_count"] = actual_success_count
        self._stats["failure_count"] = actual_failure_count
        
        # Calcular porcentagens com proteção contra divisão por zero
        success_percent = actual_success_count/len(results)*100 if results else 0
        failure_percent = actual_failure_count/len(results)*100 if results else 0
        
        self.logger.info(f"📊 Pipeline com estágios paralelos concluído")
        self.logger.info(f"⏱️ Tempo total: {duration:.2f}s")
        self.logger.info(f"📦 Itens processados: {self._stats['items_processed']}")
        self.logger.info(f"🔄 Estágios concluídos: {self._stats['stages_completed']}")
        self.logger.info(f"✅ Sucessos: {actual_success_count}/{len(results)} ({success_percent:.1f}%)")
        self.logger.info(f"❌ Falhas: {actual_failure_count}/{len(results)} ({failure_percent:.1f}%)")


def create_stage_parallel_pipeline(config: Optional[CAGEDConfig] = None) -> StageParallelPipeline:
    """
    Cria e configura um pipeline com estágios paralelos
    """
    from src.core.stage_handlers import create_stage_handlers
    
    # Criar pipeline
    pipeline = StageParallelPipeline(config)
    
    # Registrar handlers de estágio
    handlers = create_stage_handlers(config)
    for stage, handler in handlers.items():
        pipeline.register_stage_handler(stage, handler)
    
    return pipeline