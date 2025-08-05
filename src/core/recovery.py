#!/usr/bin/env python3
"""
Sistema de Recovery para o CAGED
Implementação do Item 3.1 do Plano de Melhorias

Funcionalidades:
- Checkpoint automático de progresso
- Resume de operações interrompidas
- Rollback de operações com falha
- Validação de integridade de estado
- Persistência de estado em JSON

Autor: Sistema CAGED
Data: 2024
"""

import json
import time
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Any, List
from dataclasses import dataclass, asdict
from enum import Enum

from ..utils.logger import setup_logger


class OperationType(Enum):
    """Tipos de operação que podem ser recuperadas"""
    DOWNLOAD = "download"
    EXTRACT = "extract"
    CONVERT = "convert"
    PROCESS_BATCH = "process_batch"
    FULL_PIPELINE = "full_pipeline"


class OperationStatus(Enum):
    """Status de uma operação"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class CheckpointData:
    """Dados de um checkpoint"""
    operation_id: str
    operation_type: OperationType
    status: OperationStatus
    progress: float  # 0.0 a 1.0
    current_step: str
    total_steps: int
    completed_steps: int
    start_time: datetime
    last_update: datetime
    metadata: Dict[str, Any]
    error_info: Optional[Dict[str, str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário serializável"""
        data = asdict(self)
        data['operation_type'] = self.operation_type.value
        data['status'] = self.status.value
        data['start_time'] = self.start_time.isoformat()
        data['last_update'] = self.last_update.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CheckpointData':
        """Cria instância a partir de dicionário"""
        data['operation_type'] = OperationType(data['operation_type'])
        data['status'] = OperationStatus(data['status'])
        data['start_time'] = datetime.fromisoformat(data['start_time'])
        data['last_update'] = datetime.fromisoformat(data['last_update'])
        return cls(**data)


class RecoveryManager:
    """Gerenciador de recovery e checkpoints"""
    
    def __init__(self, state_file: Path = None, max_age_hours: int = 24):
        """
        Inicializa o gerenciador de recovery
        
        Args:
            state_file: Arquivo para persistir estado (padrão: state.json)
            max_age_hours: Idade máxima dos checkpoints em horas
        """
        self.state_file = state_file or Path("state.json")
        self.max_age_hours = max_age_hours
        self.logger = setup_logger("recovery")
        
        # Estado em memória
        self._state: Dict[str, CheckpointData] = {}
        
        # Carregar estado existente
        self._load_state()
        
        # Limpar checkpoints antigos
        self._cleanup_old_checkpoints()
    
    def _load_state(self) -> None:
        """Carrega estado do arquivo"""
        if not self.state_file.exists():
            self.logger.info("Arquivo de estado não encontrado, iniciando com estado limpo")
            return
        
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Converter dados para CheckpointData
            for op_id, checkpoint_data in data.items():
                try:
                    self._state[op_id] = CheckpointData.from_dict(checkpoint_data)
                except Exception as e:
                    self.logger.warning(f"Erro ao carregar checkpoint {op_id}: {e}")
            
            self.logger.info(f"Estado carregado: {len(self._state)} checkpoints")
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar estado: {e}")
            self._state = {}
    
    def _save_state(self) -> None:
        """Salva estado no arquivo"""
        try:
            # Criar diretório se não existir
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Converter para formato serializável
            data = {op_id: checkpoint.to_dict() 
                   for op_id, checkpoint in self._state.items()}
            
            # Salvar com backup
            backup_file = self.state_file.with_suffix('.json.bak')
            if self.state_file.exists():
                self.state_file.rename(backup_file)
            
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Remover backup se salvou com sucesso
            if backup_file.exists():
                backup_file.unlink()
            
            self.logger.debug(f"Estado salvo: {len(self._state)} checkpoints")
            
        except Exception as e:
            self.logger.error(f"Erro ao salvar estado: {e}")
            # Restaurar backup se existir
            backup_file = self.state_file.with_suffix('.json.bak')
            if backup_file.exists():
                backup_file.rename(self.state_file)
    
    def _cleanup_old_checkpoints(self) -> None:
        """Remove checkpoints antigos"""
        cutoff_time = datetime.now() - timedelta(hours=self.max_age_hours)
        old_checkpoints = []
        
        for op_id, checkpoint in self._state.items():
            if checkpoint.last_update < cutoff_time:
                old_checkpoints.append(op_id)
        
        for op_id in old_checkpoints:
            del self._state[op_id]
            self.logger.info(f"Checkpoint antigo removido: {op_id}")
        
        if old_checkpoints:
            self._save_state()
    
    def _generate_operation_id(self, operation_type: OperationType, 
                              metadata: Dict[str, Any]) -> str:
        """Gera ID único para operação"""
        # Criar hash baseado no tipo e metadados principais
        key_data = {
            'type': operation_type.value,
            'timestamp': datetime.now().strftime('%Y%m%d'),
            **{k: v for k, v in metadata.items() 
               if k in ['ano', 'mes', 'ano_inicio', 'ano_fim', 'files']}
        }
        
        hash_input = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(hash_input.encode()).hexdigest()[:12]
    
    def create_checkpoint(self, operation_type: OperationType, 
                         metadata: Dict[str, Any],
                         total_steps: int = 1) -> str:
        """Cria novo checkpoint para operação"""
        operation_id = self._generate_operation_id(operation_type, metadata)
        
        checkpoint = CheckpointData(
            operation_id=operation_id,
            operation_type=operation_type,
            status=OperationStatus.PENDING,
            progress=0.0,
            current_step="Iniciando",
            total_steps=total_steps,
            completed_steps=0,
            start_time=datetime.now(),
            last_update=datetime.now(),
            metadata=metadata.copy()
        )
        
        self._state[operation_id] = checkpoint
        self._save_state()
        
        self.logger.info(f"Checkpoint criado: {operation_id} ({operation_type.value})")
        return operation_id
    
    def update_checkpoint(self, operation_id: str, 
                         progress: float = None,
                         current_step: str = None,
                         completed_steps: int = None,
                         status: OperationStatus = None,
                         metadata_update: Dict[str, Any] = None) -> bool:
        """Atualiza checkpoint existente"""
        if operation_id not in self._state:
            self.logger.warning(f"Checkpoint não encontrado: {operation_id}")
            return False
        
        checkpoint = self._state[operation_id]
        
        # Atualizar campos fornecidos
        if progress is not None:
            checkpoint.progress = max(0.0, min(1.0, progress))
        if current_step is not None:
            checkpoint.current_step = current_step
        if completed_steps is not None:
            checkpoint.completed_steps = completed_steps
        if status is not None:
            checkpoint.status = status
        if metadata_update:
            checkpoint.metadata.update(metadata_update)
        
        checkpoint.last_update = datetime.now()
        
        # Salvar periodicamente (a cada 5% de progresso ou mudança de status)
        should_save = (
            status is not None or
            (progress is not None and int(progress * 20) != int((progress - 0.05) * 20))
        )
        
        if should_save:
            self._save_state()
        
        if progress is not None:
            self.logger.debug(f"Checkpoint atualizado: {operation_id} - {progress:.1%}")
        else:
            self.logger.debug(f"Checkpoint atualizado: {operation_id}")
        return True
    
    def complete_checkpoint(self, operation_id: str, 
                           success: bool = True,
                           error_info: Dict[str, str] = None) -> bool:
        """Marca checkpoint como completo"""
        if operation_id not in self._state:
            self.logger.warning(f"Checkpoint não encontrado: {operation_id}")
            return False
        
        checkpoint = self._state[operation_id]
        checkpoint.status = OperationStatus.COMPLETED if success else OperationStatus.FAILED
        checkpoint.progress = 1.0 if success else checkpoint.progress
        checkpoint.last_update = datetime.now()
        
        if error_info:
            checkpoint.error_info = error_info
        
        self._save_state()
        
        status_msg = "concluído" if success else "falhou"
        self.logger.info(f"Checkpoint {status_msg}: {operation_id}")
        return True
    
    def can_resume(self, operation_type: OperationType, 
                   metadata: Dict[str, Any]) -> bool:
        """Verifica se operação pode ser retomada"""
        operation_id = self._generate_operation_id(operation_type, metadata)
        
        if operation_id not in self._state:
            return False
        
        checkpoint = self._state[operation_id]
        
        # Pode retomar se está em execução ou falhou
        can_resume = checkpoint.status in [OperationStatus.RUNNING, OperationStatus.FAILED]
        
        # Verificar se não é muito antigo
        age_hours = (datetime.now() - checkpoint.last_update).total_seconds() / 3600
        if age_hours > self.max_age_hours:
            can_resume = False
        
        return can_resume
    
    def get_checkpoint(self, operation_type: OperationType, 
                      metadata: Dict[str, Any]) -> Optional[CheckpointData]:
        """Obtém checkpoint para operação"""
        operation_id = self._generate_operation_id(operation_type, metadata)
        return self._state.get(operation_id)
    
    def list_active_checkpoints(self) -> List[CheckpointData]:
        """Lista checkpoints ativos"""
        active_statuses = [OperationStatus.PENDING, OperationStatus.RUNNING]
        return [cp for cp in self._state.values() if cp.status in active_statuses]
    
    def list_failed_checkpoints(self) -> List[CheckpointData]:
        """Lista checkpoints que falharam"""
        return [cp for cp in self._state.values() if cp.status == OperationStatus.FAILED]
    
    def list_checkpoints(self, status_filter: Optional[OperationStatus] = None,
                        type_filter: Optional[OperationType] = None) -> List[CheckpointData]:
        """Lista checkpoints com filtros opcionais"""
        checkpoints = list(self._state.values())
        
        if status_filter is not None:
            checkpoints = [cp for cp in checkpoints if cp.status == status_filter]
        
        if type_filter is not None:
            checkpoints = [cp for cp in checkpoints if cp.operation_type == type_filter]
        
        # Ordenar por data de criação (mais recentes primeiro)
        checkpoints.sort(key=lambda cp: cp.start_time, reverse=True)
        
        return checkpoints
    
    def get_checkpoint_by_id(self, operation_id: str) -> Optional[CheckpointData]:
        """Obtém checkpoint pelo ID"""
        return self._state.get(operation_id)
    
    def clear_checkpoints_by_status(self, status: OperationStatus) -> int:
        """Remove checkpoints por status"""
        to_remove = [op_id for op_id, cp in self._state.items() if cp.status == status]
        
        for op_id in to_remove:
            del self._state[op_id]
        
        if to_remove:
            self._save_state()
            self.logger.info(f"Removidos {len(to_remove)} checkpoints com status {status.value}")
        
        return len(to_remove)
    
    def clear_checkpoint(self, operation_id: str) -> bool:
        """Remove checkpoint específico"""
        if operation_id in self._state:
            del self._state[operation_id]
            self._save_state()
            self.logger.info(f"Checkpoint removido: {operation_id}")
            return True
        return False
    
    def clear_all_checkpoints(self) -> int:
        """Remove todos os checkpoints"""
        count = len(self._state)
        self._state.clear()
        self._save_state()
        self.logger.info(f"Todos os checkpoints removidos: {count}")
        return count
    
    def get_recovery_info(self) -> Dict[str, Any]:
        """Obtém informações de recovery"""
        total = len(self._state)
        by_status = {}
        by_type = {}
        
        for checkpoint in self._state.values():
            # Por status
            status = checkpoint.status.value
            by_status[status] = by_status.get(status, 0) + 1
            
            # Por tipo
            op_type = checkpoint.operation_type.value
            by_type[op_type] = by_type.get(op_type, 0) + 1
        
        return {
            'total_checkpoints': total,
            'by_status': by_status,
            'by_type': by_type,
            'state_file': str(self.state_file),
            'max_age_hours': self.max_age_hours
        }


# Instância global para uso em todo o projeto
_recovery_manager: Optional[RecoveryManager] = None


def get_recovery_manager() -> RecoveryManager:
    """Obtém instância global do recovery manager"""
    global _recovery_manager
    if _recovery_manager is None:
        _recovery_manager = RecoveryManager()
    return _recovery_manager


def create_checkpoint(operation_type: OperationType, 
                     metadata: Dict[str, Any],
                     total_steps: int = 1) -> str:
    """Função de conveniência para criar checkpoint"""
    return get_recovery_manager().create_checkpoint(operation_type, metadata, total_steps)


def update_checkpoint(operation_id: str, **kwargs) -> bool:
    """Função de conveniência para atualizar checkpoint"""
    return get_recovery_manager().update_checkpoint(operation_id, **kwargs)


def complete_checkpoint(operation_id: str, success: bool = True, 
                       error_info: Dict[str, str] = None) -> bool:
    """Função de conveniência para completar checkpoint"""
    return get_recovery_manager().complete_checkpoint(operation_id, success, error_info)


def can_resume_operation(operation_type: OperationType, 
                        metadata: Dict[str, Any]) -> bool:
    """Função de conveniência para verificar se pode retomar"""
    return get_recovery_manager().can_resume(operation_type, metadata)