"""Sistema de monitoramento e coleta de métricas."""

import time
import psutil
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from pathlib import Path
from dataclasses import dataclass, asdict
from threading import Lock

from .logger import setup_logger

logger = setup_logger(__name__)


@dataclass
class OperationMetric:
    """Métrica de uma operação individual."""
    operation: str
    success: bool
    duration: float
    timestamp: datetime
    details: Optional[Dict[str, Any]] = None


@dataclass
class ResourceMetric:
    """Métrica de recursos do sistema."""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    disk_usage_percent: float
    disk_free_gb: float


class MetricsCollector:
    """Coletor de métricas para monitoramento do sistema."""
    
    def __init__(self, metrics_file: Optional[Path] = None):
        """Inicializa o coletor de métricas.
        
        Args:
            metrics_file: Arquivo para persistir métricas
        """
        self._lock = Lock()
        self.metrics_file = metrics_file or Path("metrics.json")
        
        # Métricas de operações
        self.metrics = {
            "downloads": {"total": 0, "success": 0, "failed": 0, "total_duration": 0.0},
            "extractions": {"total": 0, "success": 0, "failed": 0, "total_duration": 0.0},
            "conversions": {"total": 0, "success": 0, "failed": 0, "total_duration": 0.0},
            "performance": {
                "session_start": datetime.now().isoformat(),
                "total_sessions": 0,
                "avg_session_duration": 0.0
            }
        }
        
        # Histórico de operações
        self.operation_history: List[OperationMetric] = []
        self.resource_history: List[ResourceMetric] = []
        
        # Controle de sessão
        self.session_start = time.time()
        self.last_resource_check = time.time()
        
        # Carregar métricas existentes
        self._load_metrics()
        
        logger.info("MetricsCollector inicializado")
    
    def record_operation(self, operation: str, success: bool, duration: float, 
                        details: Optional[Dict[str, Any]] = None) -> None:
        """Registra uma operação executada.
        
        Args:
            operation: Tipo de operação (download, extraction, conversion, etc.)
            success: Se a operação foi bem-sucedida
            duration: Duração da operação em segundos
            details: Detalhes adicionais da operação
        """
        with self._lock:
            # Normalizar nome da operação
            op_key = f"{operation}s" if not operation.endswith('s') else operation
            
            # Inicializar categoria se não existir
            if op_key not in self.metrics:
                self.metrics[op_key] = {"total": 0, "success": 0, "failed": 0, "total_duration": 0.0}
            
            # Atualizar contadores
            self.metrics[op_key]["total"] += 1
            self.metrics[op_key]["total_duration"] += duration
            
            if success:
                self.metrics[op_key]["success"] += 1
            else:
                self.metrics[op_key]["failed"] += 1
            
            # Adicionar ao histórico
            metric = OperationMetric(
                operation=operation,
                success=success,
                duration=duration,
                timestamp=datetime.now(),
                details=details
            )
            self.operation_history.append(metric)
            
            # Limitar histórico a últimas 1000 operações
            if len(self.operation_history) > 1000:
                self.operation_history = self.operation_history[-1000:]
            
            logger.debug(f"Operação registrada: {operation} - {'sucesso' if success else 'falha'} - {duration:.2f}s")
    

    
    def collect_resource_metrics(self) -> ResourceMetric:
        """Coleta métricas de recursos do sistema.
        
        Returns:
            Métricas de recursos coletadas
        """
        try:
            # CPU
            cpu_percent = psutil.cpu_percent(interval=0.1)
            
            # Memória
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_used_mb = memory.used / (1024 * 1024)
            
            # Disco
            disk = psutil.disk_usage('.')
            disk_usage_percent = (disk.used / disk.total) * 100
            disk_free_gb = disk.free / (1024 * 1024 * 1024)
            
            metric = ResourceMetric(
                timestamp=datetime.now(),
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                memory_used_mb=memory_used_mb,
                disk_usage_percent=disk_usage_percent,
                disk_free_gb=disk_free_gb
            )
            
            # Adicionar ao histórico (máximo 100 entradas)
            self.resource_history.append(metric)
            if len(self.resource_history) > 100:
                self.resource_history = self.resource_history[-100:]
            
            self.last_resource_check = time.time()
            return metric
            
        except Exception as e:
            logger.warning(f"Erro ao coletar métricas de recursos: {e}")
            return ResourceMetric(
                timestamp=datetime.now(),
                cpu_percent=0.0,
                memory_percent=0.0,
                memory_used_mb=0.0,
                disk_usage_percent=0.0,
                disk_free_gb=0.0
            )
    

    
    def get_success_rate(self, operation: str) -> float:
        """Calcula a taxa de sucesso de uma operação.
        
        Args:
            operation: Tipo de operação
            
        Returns:
            Taxa de sucesso (0.0 a 1.0)
        """
        op_key = f"{operation}s" if not operation.endswith('s') else operation
        
        if op_key not in self.metrics or self.metrics[op_key]["total"] == 0:
            return 0.0
        
        return self.metrics[op_key]["success"] / self.metrics[op_key]["total"]
    
    def get_average_duration(self, operation: str) -> float:
        """Calcula a duração média de uma operação.
        
        Args:
            operation: Tipo de operação
            
        Returns:
            Duração média em segundos
        """
        op_key = f"{operation}s" if not operation.endswith('s') else operation
        
        if op_key not in self.metrics or self.metrics[op_key]["total"] == 0:
            return 0.0
        
        return self.metrics[op_key]["total_duration"] / self.metrics[op_key]["total"]
    
    def generate_report(self) -> Dict[str, Any]:
        """Gera relatório completo de métricas.
        
        Returns:
            Relatório de métricas
        """
        current_time = datetime.now()
        session_duration = time.time() - self.session_start
        
        # Coletar métricas de recursos atuais
        current_resources = self.collect_resource_metrics()
        
        report = {
            "timestamp": current_time.isoformat(),
            "session_duration_minutes": session_duration / 60,
            "operations": {},
            "cache": {
                "hit_rate": self.get_cache_hit_rate(),
                "total_hits": self.metrics["cache_operations"]["hits"],
                "total_misses": self.metrics["cache_operations"]["misses"],
                "total_saves": self.metrics["cache_operations"]["saves"]
            },
            "resources": {
                "current": asdict(current_resources),
                "history_count": len(self.resource_history)
            },
            "summary": {
                "total_operations": sum(op["total"] for op in self.metrics.values() if isinstance(op, dict) and "total" in op),
                "total_successes": sum(op["success"] for op in self.metrics.values() if isinstance(op, dict) and "success" in op),
                "total_failures": sum(op["failed"] for op in self.metrics.values() if isinstance(op, dict) and "failed" in op)
            }
        }
        
        # Adicionar métricas por operação
        for op_name, op_data in self.metrics.items():
            if isinstance(op_data, dict) and "total" in op_data:
                operation_name = op_name.rstrip('s')  # Remover 's' do final
                report["operations"][operation_name] = {
                    "total": op_data["total"],
                    "success": op_data["success"],
                    "failed": op_data["failed"],
                    "success_rate": self.get_success_rate(operation_name),
                    "average_duration": self.get_average_duration(operation_name)
                }
        
        # Calcular taxa de sucesso geral
        total_ops = report["summary"]["total_operations"]
        if total_ops > 0:
            report["summary"]["overall_success_rate"] = report["summary"]["total_successes"] / total_ops
        else:
            report["summary"]["overall_success_rate"] = 0.0
        
        return report
    
    def get_recent_operations(self, hours: int = 24) -> List[OperationMetric]:
        """Obtém operações recentes.
        
        Args:
            hours: Número de horas para considerar como "recente"
            
        Returns:
            Lista de operações recentes
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [op for op in self.operation_history if op.timestamp >= cutoff_time]
    
    def get_performance_alerts(self) -> List[Dict[str, Any]]:
        """Identifica alertas de performance.
        
        Returns:
            Lista de alertas
        """
        alerts = []
        
        # Verificar taxa de sucesso baixa
        for op_name in ["download", "extraction", "conversion"]:
            success_rate = self.get_success_rate(op_name)
            if success_rate < 0.8 and self.metrics.get(f"{op_name}s", {}).get("total", 0) > 5:
                alerts.append({
                    "type": "low_success_rate",
                    "operation": op_name,
                    "success_rate": success_rate,
                    "severity": "warning" if success_rate > 0.5 else "critical"
                })
        
        # Verificar cache hit rate baixo
        cache_hit_rate = self.get_cache_hit_rate()
        total_cache_ops = self.metrics["cache_operations"]["hits"] + self.metrics["cache_operations"]["misses"]
        if cache_hit_rate < 0.3 and total_cache_ops > 10:
            alerts.append({
                "type": "low_cache_hit_rate",
                "hit_rate": cache_hit_rate,
                "severity": "warning"
            })
        
        # Verificar recursos do sistema
        if self.resource_history:
            latest_resources = self.resource_history[-1]
            if latest_resources.memory_percent > 90:
                alerts.append({
                    "type": "high_memory_usage",
                    "memory_percent": latest_resources.memory_percent,
                    "severity": "critical"
                })
            
            if latest_resources.disk_free_gb < 1.0:
                alerts.append({
                    "type": "low_disk_space",
                    "disk_free_gb": latest_resources.disk_free_gb,
                    "severity": "critical"
                })
        
        return alerts
    
    def save_metrics(self) -> None:
        """Salva métricas em arquivo."""
        try:
            def json_serializer(obj):
                """Serializa objetos datetime para JSON."""
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
            
            report = self.generate_report()
            with open(self.metrics_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=json_serializer)
            logger.debug(f"Métricas salvas em {self.metrics_file}")
        except Exception as e:
            logger.error(f"Erro ao salvar métricas: {e}")
    
    def _load_metrics(self) -> None:
        """Carrega métricas de arquivo existente."""
        if not self.metrics_file.exists():
            return
        
        try:
            with open(self.metrics_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Atualizar contador de sessões
            if "performance" in data:
                self.metrics["performance"]["total_sessions"] = data.get("performance", {}).get("total_sessions", 0) + 1
            
            logger.debug(f"Métricas carregadas de {self.metrics_file}")
        except Exception as e:
            logger.warning(f"Erro ao carregar métricas: {e}")
    
    def reset_metrics(self) -> None:
        """Reseta todas as métricas."""
        with self._lock:
            for category in self.metrics:
                if isinstance(self.metrics[category], dict):
                    if "total" in self.metrics[category]:
                        self.metrics[category] = {"total": 0, "success": 0, "failed": 0, "total_duration": 0.0}
                    elif category == "cache_operations":
                        self.metrics[category] = {"hits": 0, "misses": 0, "saves": 0}
            
            self.operation_history.clear()
            self.resource_history.clear()
            self.session_start = time.time()
            
            logger.info("Métricas resetadas")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - salva métricas automaticamente."""
        self.save_metrics()


# Instância global do coletor de métricas
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Obtém a instância global do coletor de métricas.
    
    Returns:
        Instância do MetricsCollector
    """
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


def record_operation(operation: str, success: bool, duration: float, 
                    details: Optional[Dict[str, Any]] = None) -> None:
    """Função de conveniência para registrar operação.
    
    Args:
        operation: Tipo de operação
        success: Se foi bem-sucedida
        duration: Duração em segundos
        details: Detalhes adicionais
    """
    get_metrics_collector().record_operation(operation, success, duration, details)


def record_cache_hit() -> None:
    """Registra um cache hit."""
    get_metrics_collector().record_cache_operation("hit")


def record_cache_miss() -> None:
    """Registra um cache miss."""
    get_metrics_collector().record_cache_operation("miss")


def record_cache_save() -> None:
    """Registra uma operação de save no cache."""
    get_metrics_collector().record_cache_operation("save")


def generate_metrics_report() -> Dict[str, Any]:
    """Gera relatório de métricas.
    
    Returns:
        Relatório completo de métricas
    """
    return get_metrics_collector().generate_report()