#!/usr/bin/env python3
"""
Testes para o pipeline com estágios paralelos
"""

import pytest
import asyncio
from pathlib import Path
import tempfile
import shutil

from src.core.stage_parallel_pipeline import StageParallelPipeline
from src.core.pipeline import ProcessingItem, ProcessingStage
from src.core.config import CAGEDConfig


class MockStageHandler:
    """Mock handler para testes"""
    
    def __init__(self, delay=0.1):
        self.delay = delay
        self.processed_items = []
    
    async def process(self, item):
        await asyncio.sleep(self.delay)
        self.processed_items.append(item.id)
        return type('MockResult', (), {'files_processed': 1, 'bytes_processed': 1024})


@pytest.fixture
def temp_config():
    """Configuração temporária para testes"""
    config = CAGEDConfig()
    with tempfile.TemporaryDirectory() as temp_dir:
        config.cache.directory = str(Path(temp_dir) / "cache")
        config.output.directory = str(Path(temp_dir) / "output")
        yield config


@pytest.fixture
def mock_pipeline(temp_config):
    """Pipeline mock para testes"""
    pipeline = StageParallelPipeline(temp_config)
    
    # Registrar handlers mock
    for stage in [ProcessingStage.DOWNLOAD, ProcessingStage.EXTRACT, ProcessingStage.CONVERT]:
        handler = MockStageHandler(delay=0.01)  # Delay pequeno para testes
        pipeline.register_stage_handler(stage, handler)
    
    return pipeline


@pytest.mark.asyncio
async def test_stage_parallel_pipeline_basic(mock_pipeline):
    """Teste básico do pipeline com estágios paralelos"""
    
    # Criar itens de teste
    items = [
        ProcessingItem("202401", stages=[ProcessingStage.DOWNLOAD, ProcessingStage.EXTRACT, ProcessingStage.CONVERT]),
        ProcessingItem("202402", stages=[ProcessingStage.DOWNLOAD, ProcessingStage.EXTRACT, ProcessingStage.CONVERT]),
    ]
    
    # Executar pipeline
    results = await mock_pipeline.process_items_with_parallel_stages(items)
    
    # Verificar resultados
    assert len(results) == 2
    assert all(r.success for r in results)
    assert all(r.duration > 0 for r in results)


@pytest.mark.asyncio
async def test_stage_parallel_pipeline_single_item(mock_pipeline):
    """Teste com um único item"""
    
    items = [
        ProcessingItem("202401", stages=[ProcessingStage.DOWNLOAD, ProcessingStage.EXTRACT, ProcessingStage.CONVERT])
    ]
    
    results = await mock_pipeline.process_items_with_parallel_stages(items)
    
    assert len(results) == 1
    assert results[0].success
    assert results[0].item.id == "202401"


@pytest.mark.asyncio
async def test_stage_parallel_pipeline_partial_stages(mock_pipeline):
    """Teste com apenas alguns estágios"""
    
    items = [
        ProcessingItem("202401", stages=[ProcessingStage.DOWNLOAD, ProcessingStage.EXTRACT])
    ]
    
    results = await mock_pipeline.process_items_with_parallel_stages(items)
    
    assert len(results) == 1
    assert results[0].success


@pytest.mark.asyncio
async def test_stage_parallel_pipeline_empty_items(mock_pipeline):
    """Teste com lista vazia de itens"""
    
    results = await mock_pipeline.process_items_with_parallel_stages([])
    
    assert len(results) == 0


@pytest.mark.asyncio
async def test_stage_parallel_pipeline_error_handling(mock_pipeline):
    """Teste de tratamento de erros"""
    
    class ErrorHandler(MockStageHandler):
        async def process(self, item):
            if item.id == "202401":
                raise Exception("Erro simulado")
            return await super().process(item)
    
    # Substituir handler para causar erro
    pipeline = mock_pipeline
    pipeline.register_stage_handler(ProcessingStage.DOWNLOAD, ErrorHandler(delay=0.01))
    
    items = [
        ProcessingItem("202401", stages=[ProcessingStage.DOWNLOAD]),
        ProcessingItem("202402", stages=[ProcessingStage.DOWNLOAD])
    ]
    
    results = await pipeline.process_items_with_parallel_stages(items)
    
    assert len(results) == 2
    # Verificar que temos resultados para ambos os itens
    assert any(not r.success for r in results if r.item.id == "202401")
    assert any(r.success for r in results if r.item.id == "202402")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])