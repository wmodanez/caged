# -*- coding: utf-8 -*-
"""
🧪 Configurações Globais de Teste - CAGED

Arquivo de configuração pytest com fixtures globais,
configuração de ambiente e utilitários de teste.

Parte da FASE 3.3 do Plano de Melhorias CAGED.
"""

import pytest
import tempfile
import shutil
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Importações do projeto
from src.core.config import CAGEDConfig, ProcessingConfig, FTPConfig, CacheConfig
from src.utils.cache import CacheManager
from src.core.recovery import RecoveryManager
from src.utils.metrics import MetricsCollector


@pytest.fixture(scope="session")
def test_config():
    """Configuração de teste para toda a sessão."""
    config = CAGEDConfig()
    config.debug_mode = True
    config.ftp = FTPConfig(
        server="test.server.com",
        directory="/test/path",
        timeout=10,
        max_retries=1
    )
    config.processing = ProcessingConfig(
        max_workers=2,
        enable_parallel=True,
        retry_attempts=1,
        chunk_size=100
    )
    config.cache = CacheConfig(
        enabled=True,
        directory="test_cache",
        max_size_gb=1,
        expiry_days=1
    )
    return config


@pytest.fixture
def temp_dir():
    """Diretório temporário para testes."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def temp_cache_dir():
    """Diretório temporário para cache de testes."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_file(temp_dir):
    """Arquivo de exemplo para testes."""
    file_path = temp_dir / "sample.txt"
    file_path.write_text("Conteúdo de exemplo para teste", encoding="utf-8")
    return file_path


@pytest.fixture
def sample_csv_file(temp_dir):
    """Arquivo CSV de exemplo para testes."""
    file_path = temp_dir / "sample.csv"
    content = """ano,mes,municipio,admissoes,desligamentos
2024,1,São Paulo,1000,800
2024,1,Rio de Janeiro,750,600
2024,2,São Paulo,1100,850
"""
    file_path.write_text(content, encoding="utf-8")
    return file_path


@pytest.fixture
def sample_7z_file(temp_dir):
    """Arquivo 7z de exemplo para testes (mock)."""
    file_path = temp_dir / "sample.7z"
    # Criar arquivo vazio que simula um 7z
    file_path.write_bytes(b"PK\x03\x04")
    return file_path


@pytest.fixture
def cache_manager(temp_cache_dir):
    """Cache manager para testes."""
    return CacheManager(cache_dir=temp_cache_dir)


@pytest.fixture
def recovery_manager(temp_dir):
    """Recovery manager para testes."""
    state_file = temp_dir / "test_state.json"
    return RecoveryManager(state_file=state_file)


@pytest.fixture
def metrics_collector():
    """Metrics collector para testes."""
    return MetricsCollector()


@pytest.fixture
def mock_ftp():
    """Mock do cliente FTP."""
    with patch('ftplib.FTP') as mock_ftp_class:
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        mock_ftp.getwelcome.return_value = "Welcome to test FTP"
        mock_ftp.nlst.return_value = [
            "CAGEDMOV202401.7z",
            "CAGEDMOV202402.7z",
            "CAGEDEXC202401.7z"
        ]
        yield mock_ftp


@pytest.fixture
def mock_successful_download():
    """Mock de download bem-sucedido."""
    def _download_mock(local_file, callback=None):
        # Simular progresso
        if callback:
            for i in range(0, 101, 10):
                callback(i)
        # Criar arquivo local
        Path(local_file).write_bytes(b"Conteudo baixado")
        return True
    return _download_mock


@pytest.fixture
def mock_failed_download():
    """Mock de download que falha."""
    def _download_mock(local_file, callback=None):
        raise Exception("Falha simulada no download")
    return _download_mock


@pytest.fixture
def sample_processing_items():
    """Itens de processamento de exemplo."""
    from src.core.pipeline import ProcessingItem, ProcessingStage
    
    return [
        ProcessingItem(
            id="2024-01",
            ano=2024,
            mes=1,
            stages=[ProcessingStage.DOWNLOAD, ProcessingStage.EXTRACT, ProcessingStage.CONVERT]
        ),
        ProcessingItem(
            id="2024-02",
            ano=2024,
            mes=2,
            stages=[ProcessingStage.DOWNLOAD, ProcessingStage.EXTRACT]
        ),
        ProcessingItem(
            id="2024-03",
            ano=2024,
            mes=3,
            stages=[ProcessingStage.CONVERT]
        )
    ]


@pytest.fixture
def mock_system_resources():
    """Mock de recursos do sistema."""
    with patch('psutil.virtual_memory') as mock_memory, \
         patch('psutil.cpu_percent') as mock_cpu, \
         patch('psutil.disk_usage') as mock_disk:
        
        # Configurar mocks
        mock_memory.return_value.available = 8 * 1024 * 1024 * 1024  # 8GB
        mock_memory.return_value.percent = 50.0
        mock_cpu.return_value = 25.0
        mock_disk.return_value.free = 100 * 1024 * 1024 * 1024  # 100GB
        
        yield {
            'memory': mock_memory,
            'cpu': mock_cpu,
            'disk': mock_disk
        }


# Configurações do pytest
def pytest_configure(config):
    """Configuração inicial do pytest."""
    # Adicionar marcadores customizados
    config.addinivalue_line(
        "markers", "slow: marca testes que demoram para executar"
    )
    config.addinivalue_line(
        "markers", "integration: marca testes de integração"
    )
    config.addinivalue_line(
        "markers", "unit: marca testes unitários"
    )
    config.addinivalue_line(
        "markers", "ftp: marca testes que requerem conexão FTP"
    )


def pytest_collection_modifyitems(config, items):
    """Modificar coleta de testes."""
    # Adicionar marcador 'unit' para testes que não têm marcador
    for item in items:
        if not any(mark.name in ['integration', 'slow', 'ftp'] for mark in item.iter_markers()):
            item.add_marker(pytest.mark.unit)


# Utilitários de teste
class TestUtils:
    """Utilitários para testes."""
    
    @staticmethod
    def create_test_file(path: Path, content: str = "test content", size_mb: float = None):
        """Criar arquivo de teste com conteúdo específico."""
        if size_mb:
            # Criar arquivo com tamanho específico
            content = "x" * int(size_mb * 1024 * 1024)
        
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path
    
    @staticmethod
    def create_test_csv(path: Path, rows: int = 100):
        """Criar arquivo CSV de teste com número específico de linhas."""
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, 'w', encoding='utf-8') as f:
            f.write("ano,mes,municipio,admissoes,desligamentos\n")
            for i in range(rows):
                f.write(f"2024,{(i % 12) + 1},Município {i},{1000 + i},{800 + i}\n")
        
        return path
    
    @staticmethod
    def assert_file_exists(path: Path, message: str = None):
        """Verificar se arquivo existe."""
        if not path.exists():
            raise AssertionError(message or f"Arquivo não existe: {path}")
    
    @staticmethod
    def assert_file_not_empty(path: Path, message: str = None):
        """Verificar se arquivo não está vazio."""
        if not path.exists() or path.stat().st_size == 0:
            raise AssertionError(message or f"Arquivo vazio ou inexistente: {path}")


@pytest.fixture
def test_utils():
    """Fixture para utilitários de teste."""
    return TestUtils