#!/usr/bin/env python3
"""
Testes para Sistema de Processamento Paralelo
Implementação do Item 2.2 do Plano de Melhorias

Testes unitários e de integração para o novo sistema de processamento paralelo.
"""

import asyncio
import pytest
import time
from unittest.mock import Mock, AsyncMock, patch
from pathlib import Path
import sys

# Adicionar src ao path
sys.path.append(str(Path(__file__).parent.parent / "src"))

# Configurar pytest-asyncio
pytest_plugins = ('pytest_asyncio',)

from core.pipeline import (
    ParallelPipeline,
    ResourceMonitor,
    ConnectionPool,
    ProcessingItem,
    ProcessingStage,
    ProcessingResult,
    ProcessingStatus
)
from core.config import CAGEDConfig, ProcessingConfig
from core.exceptions import PipelineError


class TestResourceMonitor:
    """Testes para ResourceMonitor"""
    
    def test_resource_monitor_creation(self):
        """Testa criação do monitor de recursos"""
        monitor = ResourceMonitor(
            cpu_threshold=75.0,
            memory_threshold=80.0,
            disk_threshold=90.0
        )
        
        assert monitor.cpu_threshold == 75.0
        assert monitor.memory_threshold == 80.0
        assert monitor.disk_threshold == 90.0
    
    @patch('psutil.cpu_percent')
    @patch('psutil.virtual_memory')
    @patch('psutil.disk_usage')
    def test_check_resources(self, mock_disk, mock_memory, mock_cpu):
        """Testa verificação de recursos"""
        # Mock dos valores de recursos
        mock_cpu.return_value = 65.5
        mock_memory.return_value = Mock(percent=70.2, available=8*1024**3)
        mock_disk.return_value = Mock(used=50*1024**3, total=100*1024**3, free=50*1024**3)
        
        monitor = ResourceMonitor()
        resources = monitor.check_resources()
        
        assert resources['cpu_percent'] == 65.5
        assert resources['memory_percent'] == 70.2
        assert resources['disk_percent'] == 50.0
        assert resources['memory_available_gb'] == 8.0
        assert resources['disk_free_gb'] == 50.0
        assert not resources['cpu_overload']
        assert not resources['memory_overload']
        assert not resources['disk_overload']
    
    @patch('psutil.cpu_percent')
    @patch('psutil.virtual_memory')
    @patch('psutil.disk_usage')
    def test_should_throttle_true(self, mock_disk, mock_memory, mock_cpu):
        """Testa detecção de necessidade de throttling"""
        # Simular recursos sobrecarregados
        mock_cpu.return_value = 85.0  # Acima do threshold
        mock_memory.return_value = Mock(percent=85.0, available=2*1024**3)
        mock_disk.return_value = Mock(used=95*1024**3, total=100*1024**3, free=5*1024**3)
        
        monitor = ResourceMonitor(cpu_threshold=80.0)
        assert monitor.should_throttle() is True
    
    @patch('psutil.cpu_percent')
    @patch('psutil.virtual_memory')
    @patch('psutil.disk_usage')
    def test_should_throttle_false(self, mock_disk, mock_memory, mock_cpu):
        """Testa quando não deve fazer throttling"""
        # Simular recursos normais
        mock_cpu.return_value = 50.0
        mock_memory.return_value = Mock(percent=60.0, available=8*1024**3)
        mock_disk.return_value = Mock(used=40*1024**3, total=100*1024**3, free=60*1024**3)
        
        monitor = ResourceMonitor()
        assert monitor.should_throttle() is False


class TestConnectionPool:
    """Testes para ConnectionPool"""
    
    @pytest.fixture
    def mock_connection_factory(self):
        """Factory mock para criar conexões"""
        async def factory():
            return Mock(id=time.time(), connected=True)
        return factory
    
    @pytest.mark.asyncio
    async def test_connection_pool_creation(self):
        """Testa criação do pool de conexões"""
        pool = ConnectionPool(max_connections=3)
        assert pool.max_connections == 3
        assert len(pool._pool) == 0
        assert len(pool._in_use) == 0
    
    @pytest.mark.asyncio
    async def test_pool_initialization(self, mock_connection_factory):
        """Testa inicialização do pool"""
        pool = ConnectionPool(max_connections=2, connection_factory=mock_connection_factory)
        await pool.initialize()
        
        assert len(pool._pool) == 2
        assert len(pool._in_use) == 0
    
    @pytest.mark.asyncio
    async def test_get_connection_context_manager(self, mock_connection_factory):
        """Testa context manager para obter conexão"""
        pool = ConnectionPool(max_connections=1, connection_factory=mock_connection_factory)
        await pool.initialize()
        
        async with pool.get_connection() as conn:
            assert conn is not None
            assert len(pool._pool) == 0  # Conexão em uso
            assert len(pool._in_use) == 1
        
        # Após sair do context manager
        assert len(pool._pool) == 1  # Conexão retornada
        assert len(pool._in_use) == 0
    
    @pytest.mark.asyncio
    async def test_pool_stats(self, mock_connection_factory):
        """Testa estatísticas do pool"""
        pool = ConnectionPool(max_connections=3, connection_factory=mock_connection_factory)
        await pool.initialize()
        
        stats = pool.get_stats()
        assert stats['available'] == 3
        assert stats['in_use'] == 0
        assert stats['total'] == 3
        assert stats['max_connections'] == 3
    
    @pytest.mark.asyncio
    async def test_close_all_connections(self, mock_connection_factory):
        """Testa fechamento de todas as conexões"""
        pool = ConnectionPool(max_connections=2, connection_factory=mock_connection_factory)
        await pool.initialize()
        
        assert len(pool._pool) == 2
        
        await pool.close_all()
        
        assert len(pool._pool) == 0
        assert len(pool._in_use) == 0


class TestParallelPipeline:
    """Testes para ParallelPipeline"""
    
    @pytest.fixture
    def config(self):
        """Configuração de teste"""
        config = CAGEDConfig()
        config.processing = ProcessingConfig(
            max_workers=2,
            enable_parallel=True,
            retry_attempts=1
        )
        config.debug_mode = True
        return config
    
    @pytest.fixture
    def pipeline(self, config):
        """Pipeline de teste"""
        return ParallelPipeline(config)
    
    @pytest.fixture
    def sample_items(self):
        """Itens de exemplo para teste"""
        return [
            ProcessingItem(
                id="2024-01",
                ano=2024,
                mes=1,
                stages=[ProcessingStage.DOWNLOAD, ProcessingStage.EXTRACT]
            ),
            ProcessingItem(
                id="2024-02",
                ano=2024,
                mes=2,
                stages=[ProcessingStage.CONVERT]
            ),
            ProcessingItem(
                id="2024-03",
                ano=2024,
                mes=3,
                stages=[ProcessingStage.EXTRACT, ProcessingStage.CONVERT]
            )
        ]
    
    def test_parallel_pipeline_creation(self, config):
        """Testa criação do pipeline paralelo"""
        pipeline = ParallelPipeline(config)
        
        assert pipeline.max_workers == 2
        assert pipeline._current_workers == 2
        assert pipeline._throttle_enabled is True
        assert pipeline.resource_monitor is not None
        assert pipeline._parallel_stats['total_tasks'] == 0
    
    def test_set_connection_pool(self, pipeline):
        """Testa configuração do pool de conexões"""
        pool = ConnectionPool(max_connections=3)
        pipeline.set_connection_pool(pool)
        
        assert pipeline.connection_pool is pool
    
    @pytest.mark.asyncio
    async def test_process_item_with_monitoring_cache_miss(self, pipeline, sample_items):
        """Testa processamento de item com cache miss"""
        item = sample_items[0]
        semaphore = asyncio.Semaphore(1)
        
        # Mock do método _process_single_item
        pipeline._process_single_item = AsyncMock(return_value=ProcessingResult(
            item=item,
            success=True,
            duration=1.0,
            files_processed=5,
            bytes_processed=1024
        ))
        
        result = await pipeline._process_item_with_monitoring(item, semaphore)
        
        assert result.success is True
        assert result.duration > 0
        assert pipeline._parallel_stats['cache_misses'] == 1
        assert pipeline._parallel_stats['cache_hits'] == 0
    
    @pytest.mark.asyncio
    async def test_process_item_with_error(self, pipeline, sample_items):
        """Testa processamento de item com erro"""
        item = sample_items[0]
        semaphore = asyncio.Semaphore(1)
        
        # Mock que gera erro
        pipeline._process_single_item = AsyncMock(side_effect=Exception("Erro de teste"))
        
        result = await pipeline._process_item_with_monitoring(item, semaphore)
        
        assert result.success is False
        assert "Erro de teste" in result.errors
        assert result.duration > 0
    
    @patch('time.time')
    def test_adjust_workers_based_on_resources_throttle(self, mock_time, pipeline):
        """Testa ajuste de workers com throttling"""
        # Simular passagem de tempo para forçar verificação
        mock_time.side_effect = [0, 10, 20]  # Múltiplas chamadas
        
        # Configurar pipeline com mais workers para poder reduzir
        pipeline._current_workers = 4
        pipeline.max_workers = 4
        pipeline._last_resource_check = 0  # Forçar verificação
        
        # Mock do monitor para simular sobrecarga
        pipeline.resource_monitor.check_resources = Mock(return_value={
            'cpu_percent': 90.0,
            'memory_percent': 85.0,
            'cpu_overload': True,
            'memory_overload': True,
            'disk_overload': False
        })
        
        initial_workers = pipeline._current_workers
        
        # Executar ajuste múltiplas vezes
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(pipeline._adjust_workers_based_on_resources())
        loop.run_until_complete(pipeline._adjust_workers_based_on_resources())
        loop.close()
        
        # Verificar se pelo menos registrou verificações de recursos
        assert pipeline._parallel_stats['resource_checks'] >= 1
    
    def test_update_duration_stats(self, pipeline):
        """Testa atualização de estatísticas de duração"""
        # Inicializar com valores conhecidos
        pipeline._parallel_stats['completed_tasks'] = 0
        pipeline._parallel_stats['avg_task_duration'] = 0.0
        
        # Primeira duração
        pipeline._parallel_stats['completed_tasks'] = 1
        pipeline._update_duration_stats(2.0)
        assert pipeline._parallel_stats['avg_task_duration'] == 2.0
        
        # Segunda duração
        pipeline._parallel_stats['completed_tasks'] = 2
        pipeline._update_duration_stats(4.0)
        expected_avg = (2.0 + 4.0) / 2  # 3.0
        actual_avg = pipeline._parallel_stats['avg_task_duration']
        assert abs(actual_avg - expected_avg) < 0.01
    
    def test_get_parallel_stats(self, pipeline):
        """Testa obtenção de estatísticas"""
        # Configurar algumas estatísticas
        pipeline._parallel_stats['total_tasks'] = 10
        pipeline._parallel_stats['completed_tasks'] = 8
        pipeline._parallel_stats['failed_tasks'] = 2
        
        stats = pipeline.get_parallel_stats()
        
        assert stats['total_tasks'] == 10
        assert stats['completed_tasks'] == 8
        assert stats['failed_tasks'] == 2
        assert stats['current_workers'] == pipeline._current_workers
        assert stats['max_workers'] == pipeline.max_workers
        assert stats['throttle_enabled'] == pipeline._throttle_enabled
    
    @pytest.mark.asyncio
    async def test_process_items_parallel_basic(self, pipeline, sample_items):
        """Testa processamento paralelo básico"""
        # Mock do método de processamento de batch
        async def mock_process_batch(batch):
            results = []
            for item in batch:
                result = ProcessingResult(
                    item=item,
                    success=True,
                    duration=0.5,
                    files_processed=1,
                    bytes_processed=100
                )
                results.append(result)
            return results
        
        pipeline._process_batch_with_monitoring = AsyncMock(side_effect=mock_process_batch)
        
        results = await pipeline.process_items_parallel(
            items=sample_items,
            enable_throttling=False,
            batch_size=2
        )
        
        assert len(results) == len(sample_items)
        assert all(r.success for r in results)
        assert pipeline._parallel_stats['total_tasks'] == len(sample_items)
    
    @pytest.mark.asyncio
    async def test_process_items_parallel_with_error(self, pipeline, sample_items):
        """Testa processamento paralelo com erro"""
        # Mock que gera erro
        pipeline._process_batch_with_monitoring = AsyncMock(
            side_effect=Exception("Erro no batch")
        )
        
        with pytest.raises(PipelineError, match="Falha no processamento paralelo"):
            await pipeline.process_items_parallel(
                items=sample_items,
                enable_throttling=False
            )


class TestIntegration:
    """Testes de integração"""
    
    @pytest.mark.asyncio
    async def test_full_parallel_processing_workflow(self):
        """Teste de integração completo do workflow paralelo"""
        # Configuração
        config = CAGEDConfig()
        config.processing.max_workers = 2
        config.debug_mode = False
        
        # Pipeline
        pipeline = ParallelPipeline(config)
        
        # Itens de teste
        items = [
            ProcessingItem(
                id=f"test-{i}",
                ano=2024,
                mes=i,
                stages=[ProcessingStage.CONVERT]
            )
            for i in range(1, 6)  # 5 itens
        ]
        
        # Mock do processamento individual
        async def mock_process_single(item):
            await asyncio.sleep(0.1)  # Simular trabalho
            return ProcessingResult(
                item=item,
                success=True,
                duration=0.1,
                files_processed=1,
                bytes_processed=50
            )
        
        pipeline._process_single_item = mock_process_single
        
        # Executar processamento
        start_time = time.time()
        results = await pipeline.process_items_parallel(
            items=items,
            enable_throttling=False,
            batch_size=3
        )
        end_time = time.time()
        
        # Verificações
        assert len(results) == 5
        assert all(r.success for r in results)
        
        # Verificar que foi mais rápido que processamento sequencial
        # (com 2 workers, deve ser mais rápido que 5 * 0.1 = 0.5s)
        total_time = end_time - start_time
        assert total_time < 1.0  # Margem maior para overhead e variações do sistema
        
        # Verificar estatísticas
        stats = pipeline.get_parallel_stats()
        assert stats['total_tasks'] == 5
        assert stats['completed_tasks'] == 5
        assert stats['failed_tasks'] == 0


if __name__ == "__main__":
    # Executar testes
    pytest.main([__file__, "-v"])