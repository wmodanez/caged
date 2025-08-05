# -*- coding: utf-8 -*-
"""
🧪 Testes Unitários para Sistema de Métricas - CAGED

Testes para o módulo de métricas e monitoramento:
- MetricsCollector
- Coleta de métricas de performance
- Relatórios e estatísticas
- Persistência de dados

Parte da FASE 3.3 do Plano de Melhorias CAGED.
"""

import pytest
import tempfile
import json
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from src.utils.metrics import MetricsCollector, OperationMetrics, SystemMetrics


class TestMetricsCollector:
    """Testes para MetricsCollector"""
    
    @pytest.fixture
    def metrics_collector(self, temp_dir):
        """Instância do coletor de métricas"""
        metrics_file = temp_dir / "test_metrics.json"
        return MetricsCollector(metrics_file=str(metrics_file))
    
    def test_init(self, metrics_collector):
        """Testa inicialização do coletor"""
        assert metrics_collector is not None
        assert hasattr(metrics_collector, 'metrics')
        assert hasattr(metrics_collector, 'start_time')
        assert hasattr(metrics_collector, 'end_time')
    
    def test_start_operation(self, metrics_collector):
        """Testa início de operação"""
        operation_id = metrics_collector.start_operation("download", {"file": "test.7z"})
        
        assert operation_id is not None
        assert operation_id in metrics_collector.active_operations
        
        operation = metrics_collector.active_operations[operation_id]
        assert operation.operation_type == "download"
        assert operation.metadata["file"] == "test.7z"
        assert operation.start_time is not None
        assert operation.end_time is None
    
    def test_end_operation_success(self, metrics_collector):
        """Testa finalização bem-sucedida de operação"""
        operation_id = metrics_collector.start_operation("download", {"file": "test.7z"})
        
        # Simular algum tempo de execução
        import time
        time.sleep(0.1)
        
        metrics_collector.end_operation(operation_id, success=True)
        
        assert operation_id not in metrics_collector.active_operations
        assert operation_id in metrics_collector.completed_operations
        
        operation = metrics_collector.completed_operations[operation_id]
        assert operation.success is True
        assert operation.end_time is not None
        assert operation.duration > 0
    
    def test_end_operation_failure(self, metrics_collector):
        """Testa finalização com falha de operação"""
        operation_id = metrics_collector.start_operation("extract", {"file": "test.7z"})
        
        error_info = {"error": "Extraction failed", "code": "EXT001"}
        metrics_collector.end_operation(operation_id, success=False, error_info=error_info)
        
        operation = metrics_collector.completed_operations[operation_id]
        assert operation.success is False
        assert operation.error_info == error_info
    
    def test_record_system_metrics(self, metrics_collector):
        """Testa registro de métricas do sistema"""
        with patch('psutil.virtual_memory') as mock_memory, \
             patch('psutil.cpu_percent') as mock_cpu, \
             patch('psutil.disk_usage') as mock_disk:
            
            # Configurar mocks
            mock_memory.return_value.available = 8 * 1024 * 1024 * 1024  # 8GB
            mock_memory.return_value.percent = 50.0
            mock_cpu.return_value = 25.0
            mock_disk.return_value.free = 100 * 1024 * 1024 * 1024  # 100GB
            
            metrics_collector.record_system_metrics()
            
            assert len(metrics_collector.system_metrics) > 0
            latest_metric = metrics_collector.system_metrics[-1]
            
            assert latest_metric.memory_available_gb == 8.0
            assert latest_metric.memory_usage_percent == 50.0
            assert latest_metric.cpu_usage_percent == 25.0
            assert latest_metric.disk_free_gb == 100.0
    
    def test_record_cache_metrics(self, metrics_collector):
        """Testa registro de métricas de cache"""
        cache_stats = {
            "hits": 15,
            "misses": 5,
            "total_requests": 20,
            "hit_rate": 0.75,
            "cache_size_mb": 256.5
        }
        
        metrics_collector.record_cache_metrics(cache_stats)
        
        assert "cache" in metrics_collector.metrics
        cache_metrics = metrics_collector.metrics["cache"]
        
        assert cache_metrics["hits"] == 15
        assert cache_metrics["misses"] == 5
        assert cache_metrics["hit_rate"] == 0.75
    
    def test_get_operation_stats(self, metrics_collector):
        """Testa obtenção de estatísticas de operações"""
        # Simular várias operações
        operations = [
            ("download", True, 1.5),
            ("download", True, 2.0),
            ("download", False, 0.5),
            ("extract", True, 3.0),
            ("convert", True, 4.5)
        ]
        
        for op_type, success, duration in operations:
            op_id = metrics_collector.start_operation(op_type, {})
            # Simular duração
            operation = metrics_collector.active_operations[op_id]
            operation.start_time = datetime.now() - timedelta(seconds=duration)
            metrics_collector.end_operation(op_id, success=success)
        
        stats = metrics_collector.get_operation_stats()
        
        assert "download" in stats
        assert "extract" in stats
        assert "convert" in stats
        
        download_stats = stats["download"]
        assert download_stats["total"] == 3
        assert download_stats["success"] == 2
        assert download_stats["failed"] == 1
        assert download_stats["success_rate"] == 2/3
    
    def test_get_performance_summary(self, metrics_collector):
        """Testa obtenção de resumo de performance"""
        # Simular operações com diferentes durações
        durations = [1.0, 2.0, 3.0, 4.0, 5.0]
        
        for i, duration in enumerate(durations):
            op_id = metrics_collector.start_operation("test", {"index": i})
            operation = metrics_collector.active_operations[op_id]
            operation.start_time = datetime.now() - timedelta(seconds=duration)
            metrics_collector.end_operation(op_id, success=True)
        
        summary = metrics_collector.get_performance_summary()
        
        assert "total_operations" in summary
        assert "avg_duration" in summary
        assert "min_duration" in summary
        assert "max_duration" in summary
        assert "total_duration" in summary
        
        assert summary["total_operations"] == 5
        assert summary["avg_duration"] == 3.0  # (1+2+3+4+5)/5
        assert summary["min_duration"] == 1.0
        assert summary["max_duration"] == 5.0
    
    def test_generate_report_text(self, metrics_collector):
        """Testa geração de relatório em texto"""
        # Adicionar algumas operações
        op_id = metrics_collector.start_operation("download", {"file": "test.7z"})
        metrics_collector.end_operation(op_id, success=True)
        
        report = metrics_collector.generate_report(format="text")
        
        assert isinstance(report, str)
        assert "RELATÓRIO DE MÉTRICAS" in report
        assert "download" in report
        assert "Operações" in report
    
    def test_generate_report_json(self, metrics_collector):
        """Testa geração de relatório em JSON"""
        # Adicionar algumas operações
        op_id = metrics_collector.start_operation("extract", {"file": "test.7z"})
        metrics_collector.end_operation(op_id, success=True)
        
        report = metrics_collector.generate_report(format="json")
        
        assert isinstance(report, str)
        # Verificar se é JSON válido
        data = json.loads(report)
        assert "operations" in data
        assert "performance" in data
        assert "timestamp" in data
    
    def test_generate_report_dict(self, metrics_collector):
        """Testa geração de relatório como dicionário"""
        report = metrics_collector.generate_report(format="dict")
        
        assert isinstance(report, dict)
        assert "operations" in report
        assert "performance" in report
        assert "system" in report
        assert "timestamp" in report
    
    def test_save_metrics(self, metrics_collector, temp_dir):
        """Testa salvamento de métricas"""
        # Adicionar algumas métricas
        op_id = metrics_collector.start_operation("convert", {"file": "test.txt"})
        metrics_collector.end_operation(op_id, success=True)
        
        metrics_collector.save_metrics()
        
        # Verificar se arquivo foi criado
        metrics_file = Path(metrics_collector.metrics_file)
        assert metrics_file.exists()
        
        # Verificar conteúdo
        with open(metrics_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        assert "operations" in data
        assert "timestamp" in data
    
    def test_load_metrics(self, metrics_collector, temp_dir):
        """Testa carregamento de métricas"""
        # Criar arquivo de métricas
        metrics_data = {
            "operations": {
                "download": {"total": 5, "success": 4, "failed": 1}
            },
            "timestamp": datetime.now().isoformat()
        }
        
        metrics_file = Path(metrics_collector.metrics_file)
        with open(metrics_file, 'w', encoding='utf-8') as f:
            json.dump(metrics_data, f)
        
        metrics_collector.load_metrics()
        
        # Verificar se dados foram carregados
        assert "operations" in metrics_collector.metrics
    
    def test_reset_metrics(self, metrics_collector):
        """Testa reset de métricas"""
        # Adicionar algumas operações
        op_id = metrics_collector.start_operation("download", {})
        metrics_collector.end_operation(op_id, success=True)
        
        assert len(metrics_collector.completed_operations) > 0
        
        metrics_collector.reset_metrics()
        
        assert len(metrics_collector.completed_operations) == 0
        assert len(metrics_collector.active_operations) == 0
        assert len(metrics_collector.system_metrics) == 0
    
    def test_get_alerts(self, metrics_collector):
        """Testa geração de alertas"""
        # Simular operações com falhas
        for i in range(10):
            op_id = metrics_collector.start_operation("download", {"index": i})
            success = i < 5  # 50% de falhas
            metrics_collector.end_operation(op_id, success=success)
        
        alerts = metrics_collector.get_alerts()
        
        assert isinstance(alerts, list)
        # Deve haver alerta de alta taxa de falhas
        failure_alerts = [a for a in alerts if "falha" in a["message"].lower()]
        assert len(failure_alerts) > 0
    
    def test_get_trends(self, metrics_collector):
        """Testa análise de tendências"""
        # Simular operações ao longo do tempo
        base_time = datetime.now() - timedelta(hours=1)
        
        for i in range(10):
            op_id = metrics_collector.start_operation("download", {"index": i})
            operation = metrics_collector.active_operations[op_id]
            operation.start_time = base_time + timedelta(minutes=i*5)
            operation.end_time = operation.start_time + timedelta(seconds=30)
            operation.duration = 30.0
            metrics_collector.end_operation(op_id, success=True)
        
        trends = metrics_collector.get_trends()
        
        assert "operations_per_hour" in trends
        assert "avg_duration_trend" in trends
        assert "success_rate_trend" in trends
    
    def test_export_csv(self, metrics_collector, temp_dir):
        """Testa exportação para CSV"""
        # Adicionar algumas operações
        for i in range(3):
            op_id = metrics_collector.start_operation("test", {"index": i})
            metrics_collector.end_operation(op_id, success=True)
        
        csv_file = temp_dir / "metrics.csv"
        metrics_collector.export_csv(str(csv_file))
        
        assert csv_file.exists()
        
        # Verificar conteúdo básico
        content = csv_file.read_text(encoding='utf-8')
        assert "operation_type" in content
        assert "duration" in content
        assert "success" in content
    
    def test_memory_usage_tracking(self, metrics_collector):
        """Testa rastreamento de uso de memória"""
        initial_memory = metrics_collector.get_memory_usage()
        
        # Simular operação que usa memória
        op_id = metrics_collector.start_operation("memory_test", {})
        
        # Adicionar dados de uso de memória
        metrics_collector.track_memory_usage(op_id, 512.5)  # 512.5 MB
        
        metrics_collector.end_operation(op_id, success=True)
        
        operation = metrics_collector.completed_operations[op_id]
        assert hasattr(operation, 'peak_memory_mb')
        assert operation.peak_memory_mb == 512.5
    
    def test_concurrent_operations(self, metrics_collector):
        """Testa métricas de operações concorrentes"""
        # Iniciar várias operações sem finalizar
        op_ids = []
        for i in range(5):
            op_id = metrics_collector.start_operation("concurrent", {"index": i})
            op_ids.append(op_id)
        
        concurrent_count = metrics_collector.get_concurrent_operations_count()
        assert concurrent_count == 5
        
        # Finalizar algumas
        for op_id in op_ids[:3]:
            metrics_collector.end_operation(op_id, success=True)
        
        concurrent_count = metrics_collector.get_concurrent_operations_count()
        assert concurrent_count == 2


class TestOperationMetrics:
    """Testes para OperationMetrics"""
    
    def test_operation_metrics_creation(self):
        """Testa criação de métricas de operação"""
        start_time = datetime.now()
        metadata = {"file": "test.7z", "size": 1024}
        
        metrics = OperationMetrics(
            operation_id="test_123",
            operation_type="download",
            start_time=start_time,
            metadata=metadata
        )
        
        assert metrics.operation_id == "test_123"
        assert metrics.operation_type == "download"
        assert metrics.start_time == start_time
        assert metrics.metadata == metadata
        assert metrics.end_time is None
        assert metrics.success is None
    
    def test_operation_metrics_completion(self):
        """Testa finalização de métricas de operação"""
        start_time = datetime.now() - timedelta(seconds=5)
        end_time = datetime.now()
        
        metrics = OperationMetrics(
            operation_id="test_123",
            operation_type="extract",
            start_time=start_time
        )
        
        metrics.complete(success=True, end_time=end_time)
        
        assert metrics.success is True
        assert metrics.end_time == end_time
        assert metrics.duration == 5.0
    
    def test_operation_metrics_serialization(self):
        """Testa serialização de métricas de operação"""
        metrics = OperationMetrics(
            operation_id="test_123",
            operation_type="convert",
            start_time=datetime.now(),
            metadata={"input": "test.txt"}
        )
        
        data = metrics.to_dict()
        
        assert isinstance(data, dict)
        assert data["operation_id"] == "test_123"
        assert data["operation_type"] == "convert"
        assert "start_time" in data
        assert "metadata" in data


class TestSystemMetrics:
    """Testes para SystemMetrics"""
    
    def test_system_metrics_creation(self):
        """Testa criação de métricas do sistema"""
        timestamp = datetime.now()
        
        metrics = SystemMetrics(
            timestamp=timestamp,
            cpu_usage_percent=25.5,
            memory_usage_percent=60.0,
            memory_available_gb=8.0,
            disk_free_gb=100.0
        )
        
        assert metrics.timestamp == timestamp
        assert metrics.cpu_usage_percent == 25.5
        assert metrics.memory_usage_percent == 60.0
        assert metrics.memory_available_gb == 8.0
        assert metrics.disk_free_gb == 100.0
    
    def test_system_metrics_serialization(self):
        """Testa serialização de métricas do sistema"""
        metrics = SystemMetrics(
            timestamp=datetime.now(),
            cpu_usage_percent=30.0,
            memory_usage_percent=50.0,
            memory_available_gb=16.0,
            disk_free_gb=500.0
        )
        
        data = metrics.to_dict()
        
        assert isinstance(data, dict)
        assert data["cpu_usage_percent"] == 30.0
        assert data["memory_usage_percent"] == 50.0
        assert "timestamp" in data