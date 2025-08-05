#!/usr/bin/env python3
"""
Testes para o Sistema de Recovery
Testa todas as funcionalidades do RecoveryManager

Autor: Sistema CAGED
Data: 2024
"""

import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.core.recovery import (
    RecoveryManager, CheckpointData, OperationType, OperationStatus,
    get_recovery_manager, create_checkpoint, update_checkpoint,
    complete_checkpoint, can_resume_operation
)


class TestCheckpointData(unittest.TestCase):
    """Testes para a classe CheckpointData"""
    
    def setUp(self):
        self.checkpoint_data = CheckpointData(
            operation_id="test_123",
            operation_type=OperationType.DOWNLOAD,
            status=OperationStatus.RUNNING,
            progress=0.5,
            current_step="Baixando arquivo",
            total_steps=10,
            completed_steps=5,
            start_time=datetime.now(),
            last_update=datetime.now(),
            metadata={"ano": 2024, "mes": 1}
        )
    
    def test_to_dict(self):
        """Testa conversão para dicionário"""
        data_dict = self.checkpoint_data.to_dict()
        
        self.assertIsInstance(data_dict, dict)
        self.assertEqual(data_dict['operation_id'], "test_123")
        self.assertEqual(data_dict['operation_type'], "download")
        self.assertEqual(data_dict['status'], "running")
        self.assertEqual(data_dict['progress'], 0.5)
        self.assertIsInstance(data_dict['start_time'], str)
        self.assertIsInstance(data_dict['last_update'], str)
    
    def test_from_dict(self):
        """Testa criação a partir de dicionário"""
        data_dict = self.checkpoint_data.to_dict()
        restored = CheckpointData.from_dict(data_dict)
        
        self.assertEqual(restored.operation_id, self.checkpoint_data.operation_id)
        self.assertEqual(restored.operation_type, self.checkpoint_data.operation_type)
        self.assertEqual(restored.status, self.checkpoint_data.status)
        self.assertEqual(restored.progress, self.checkpoint_data.progress)
        self.assertEqual(restored.metadata, self.checkpoint_data.metadata)
    
    def test_serialization_roundtrip(self):
        """Testa serialização completa (ida e volta)"""
        # Converter para dict e depois para JSON
        data_dict = self.checkpoint_data.to_dict()
        json_str = json.dumps(data_dict)
        
        # Restaurar do JSON
        restored_dict = json.loads(json_str)
        restored = CheckpointData.from_dict(restored_dict)
        
        # Verificar se os dados são iguais
        self.assertEqual(restored.operation_id, self.checkpoint_data.operation_id)
        self.assertEqual(restored.operation_type, self.checkpoint_data.operation_type)
        self.assertEqual(restored.status, self.checkpoint_data.status)
        self.assertEqual(restored.progress, self.checkpoint_data.progress)


class TestRecoveryManager(unittest.TestCase):
    """Testes para a classe RecoveryManager"""
    
    def setUp(self):
        # Usar arquivo temporário para testes
        self.temp_dir = tempfile.mkdtemp()
        self.state_file = Path(self.temp_dir) / "test_state.json"
        self.recovery_manager = RecoveryManager(
            state_file=self.state_file,
            max_age_hours=24
        )
    
    def tearDown(self):
        # Limpar arquivos temporários
        if self.state_file.exists():
            self.state_file.unlink()
        if self.state_file.with_suffix('.json.bak').exists():
            self.state_file.with_suffix('.json.bak').unlink()
    
    def test_create_checkpoint(self):
        """Testa criação de checkpoint"""
        metadata = {"ano": 2024, "mes": 1}
        operation_id = self.recovery_manager.create_checkpoint(
            OperationType.DOWNLOAD, metadata, total_steps=5
        )
        
        self.assertIsInstance(operation_id, str)
        self.assertEqual(len(operation_id), 12)  # Hash MD5 truncado
        
        # Verificar se checkpoint foi criado
        checkpoint = self.recovery_manager._state[operation_id]
        self.assertEqual(checkpoint.operation_type, OperationType.DOWNLOAD)
        self.assertEqual(checkpoint.status, OperationStatus.PENDING)
        self.assertEqual(checkpoint.total_steps, 5)
        self.assertEqual(checkpoint.metadata, metadata)
    
    def test_update_checkpoint(self):
        """Testa atualização de checkpoint"""
        metadata = {"ano": 2024, "mes": 1}
        operation_id = self.recovery_manager.create_checkpoint(
            OperationType.DOWNLOAD, metadata
        )
        
        # Atualizar checkpoint
        success = self.recovery_manager.update_checkpoint(
            operation_id,
            progress=0.7,
            current_step="Processando",
            completed_steps=7,
            status=OperationStatus.RUNNING
        )
        
        self.assertTrue(success)
        
        checkpoint = self.recovery_manager._state[operation_id]
        self.assertEqual(checkpoint.progress, 0.7)
        self.assertEqual(checkpoint.current_step, "Processando")
        self.assertEqual(checkpoint.completed_steps, 7)
        self.assertEqual(checkpoint.status, OperationStatus.RUNNING)
    
    def test_update_nonexistent_checkpoint(self):
        """Testa atualização de checkpoint inexistente"""
        success = self.recovery_manager.update_checkpoint(
            "nonexistent_id", progress=0.5
        )
        self.assertFalse(success)
    
    def test_complete_checkpoint(self):
        """Testa conclusão de checkpoint"""
        metadata = {"ano": 2024, "mes": 1}
        operation_id = self.recovery_manager.create_checkpoint(
            OperationType.DOWNLOAD, metadata
        )
        
        # Completar com sucesso
        success = self.recovery_manager.complete_checkpoint(operation_id, success=True)
        self.assertTrue(success)
        
        checkpoint = self.recovery_manager._state[operation_id]
        self.assertEqual(checkpoint.status, OperationStatus.COMPLETED)
        self.assertEqual(checkpoint.progress, 1.0)
    
    def test_complete_checkpoint_with_error(self):
        """Testa conclusão de checkpoint com erro"""
        metadata = {"ano": 2024, "mes": 1}
        operation_id = self.recovery_manager.create_checkpoint(
            OperationType.DOWNLOAD, metadata
        )
        
        error_info = {"error": "Connection timeout", "code": "TIMEOUT"}
        success = self.recovery_manager.complete_checkpoint(
            operation_id, success=False, error_info=error_info
        )
        self.assertTrue(success)
        
        checkpoint = self.recovery_manager._state[operation_id]
        self.assertEqual(checkpoint.status, OperationStatus.FAILED)
        self.assertEqual(checkpoint.error_info, error_info)
    
    def test_can_resume(self):
        """Testa verificação de possibilidade de retomada"""
        metadata = {"ano": 2024, "mes": 1}
        
        # Operação nova - não pode retomar
        can_resume = self.recovery_manager.can_resume(OperationType.DOWNLOAD, metadata)
        self.assertFalse(can_resume)
        
        # Criar checkpoint em execução
        operation_id = self.recovery_manager.create_checkpoint(
            OperationType.DOWNLOAD, metadata
        )
        self.recovery_manager.update_checkpoint(
            operation_id, status=OperationStatus.RUNNING
        )
        
        # Agora pode retomar
        can_resume = self.recovery_manager.can_resume(OperationType.DOWNLOAD, metadata)
        self.assertTrue(can_resume)
        
        # Completar operação - não pode mais retomar
        self.recovery_manager.complete_checkpoint(operation_id, success=True)
        can_resume = self.recovery_manager.can_resume(OperationType.DOWNLOAD, metadata)
        self.assertFalse(can_resume)
    
    def test_get_checkpoint(self):
        """Testa obtenção de checkpoint"""
        metadata = {"ano": 2024, "mes": 1}
        operation_id = self.recovery_manager.create_checkpoint(
            OperationType.DOWNLOAD, metadata
        )
        
        # Obter checkpoint existente
        checkpoint = self.recovery_manager.get_checkpoint(
            OperationType.DOWNLOAD, metadata
        )
        self.assertIsNotNone(checkpoint)
        self.assertEqual(checkpoint.operation_id, operation_id)
        
        # Tentar obter checkpoint inexistente
        checkpoint = self.recovery_manager.get_checkpoint(
            OperationType.EXTRACT, {"ano": 2023, "mes": 12}
        )
        self.assertIsNone(checkpoint)
    
    def test_list_active_checkpoints(self):
        """Testa listagem de checkpoints ativos"""
        # Criar checkpoints com diferentes status
        metadata1 = {"ano": 2024, "mes": 1}
        metadata2 = {"ano": 2024, "mes": 2}
        metadata3 = {"ano": 2024, "mes": 3}
        
        op1 = self.recovery_manager.create_checkpoint(OperationType.DOWNLOAD, metadata1)
        op2 = self.recovery_manager.create_checkpoint(OperationType.EXTRACT, metadata2)
        op3 = self.recovery_manager.create_checkpoint(OperationType.CONVERT, metadata3)
        
        # Atualizar status
        self.recovery_manager.update_checkpoint(op1, status=OperationStatus.RUNNING)
        self.recovery_manager.update_checkpoint(op2, status=OperationStatus.PENDING)
        self.recovery_manager.complete_checkpoint(op3, success=True)
        
        # Listar ativos (PENDING e RUNNING)
        active = self.recovery_manager.list_active_checkpoints()
        self.assertEqual(len(active), 2)
        
        active_ids = [cp.operation_id for cp in active]
        self.assertIn(op1, active_ids)
        self.assertIn(op2, active_ids)
        self.assertNotIn(op3, active_ids)
    
    def test_list_failed_checkpoints(self):
        """Testa listagem de checkpoints que falharam"""
        metadata1 = {"ano": 2024, "mes": 1}
        metadata2 = {"ano": 2024, "mes": 2}
        
        op1 = self.recovery_manager.create_checkpoint(OperationType.DOWNLOAD, metadata1)
        op2 = self.recovery_manager.create_checkpoint(OperationType.EXTRACT, metadata2)
        
        # Falhar uma operação
        self.recovery_manager.complete_checkpoint(op1, success=False)
        self.recovery_manager.complete_checkpoint(op2, success=True)
        
        failed = self.recovery_manager.list_failed_checkpoints()
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0].operation_id, op1)
    
    def test_clear_checkpoint(self):
        """Testa remoção de checkpoint específico"""
        metadata = {"ano": 2024, "mes": 1}
        operation_id = self.recovery_manager.create_checkpoint(
            OperationType.DOWNLOAD, metadata
        )
        
        # Verificar que existe
        self.assertIn(operation_id, self.recovery_manager._state)
        
        # Remover
        success = self.recovery_manager.clear_checkpoint(operation_id)
        self.assertTrue(success)
        self.assertNotIn(operation_id, self.recovery_manager._state)
        
        # Tentar remover novamente
        success = self.recovery_manager.clear_checkpoint(operation_id)
        self.assertFalse(success)
    
    def test_clear_all_checkpoints(self):
        """Testa remoção de todos os checkpoints"""
        # Criar alguns checkpoints
        for i in range(3):
            metadata = {"ano": 2024, "mes": i + 1}
            self.recovery_manager.create_checkpoint(OperationType.DOWNLOAD, metadata)
        
        self.assertEqual(len(self.recovery_manager._state), 3)
        
        # Limpar todos
        count = self.recovery_manager.clear_all_checkpoints()
        self.assertEqual(count, 3)
        self.assertEqual(len(self.recovery_manager._state), 0)
    
    def test_state_persistence(self):
        """Testa persistência de estado"""
        metadata = {"ano": 2024, "mes": 1}
        operation_id = self.recovery_manager.create_checkpoint(
            OperationType.DOWNLOAD, metadata, total_steps=10
        )
        
        self.recovery_manager.update_checkpoint(
            operation_id, progress=0.5, current_step="Meio do caminho"
        )
        
        # Verificar que arquivo foi criado
        self.assertTrue(self.state_file.exists())
        
        # Criar novo manager com mesmo arquivo
        new_manager = RecoveryManager(state_file=self.state_file)
        
        # Verificar que estado foi carregado
        self.assertEqual(len(new_manager._state), 1)
        checkpoint = new_manager._state[operation_id]
        self.assertEqual(checkpoint.progress, 0.5)
        self.assertEqual(checkpoint.current_step, "Meio do caminho")
        self.assertEqual(checkpoint.total_steps, 10)
    
    def test_cleanup_old_checkpoints(self):
        """Testa limpeza de checkpoints antigos"""
        # Criar manager com idade máxima muito baixa
        manager = RecoveryManager(
            state_file=self.state_file,
            max_age_hours=0.001  # ~3.6 segundos
        )
        
        metadata = {"ano": 2024, "mes": 1}
        operation_id = manager.create_checkpoint(OperationType.DOWNLOAD, metadata)
        
        # Simular checkpoint antigo
        checkpoint = manager._state[operation_id]
        checkpoint.last_update = datetime.now() - timedelta(hours=1)
        
        # Executar limpeza
        manager._cleanup_old_checkpoints()
        
        # Verificar que foi removido
        self.assertEqual(len(manager._state), 0)
    
    def test_get_recovery_info(self):
        """Testa obtenção de informações de recovery"""
        # Criar checkpoints com diferentes tipos e status
        metadata1 = {"ano": 2024, "mes": 1}
        metadata2 = {"ano": 2024, "mes": 2}
        metadata3 = {"ano": 2024, "mes": 3}
        
        op1 = self.recovery_manager.create_checkpoint(OperationType.DOWNLOAD, metadata1)
        op2 = self.recovery_manager.create_checkpoint(OperationType.EXTRACT, metadata2)
        op3 = self.recovery_manager.create_checkpoint(OperationType.CONVERT, metadata3)
        
        self.recovery_manager.update_checkpoint(op1, status=OperationStatus.RUNNING)
        self.recovery_manager.complete_checkpoint(op2, success=True)
        self.recovery_manager.complete_checkpoint(op3, success=False)
        
        info = self.recovery_manager.get_recovery_info()
        
        self.assertEqual(info['total_checkpoints'], 3)
        self.assertEqual(info['by_status']['running'], 1)
        self.assertEqual(info['by_status']['completed'], 1)
        self.assertEqual(info['by_status']['failed'], 1)
        self.assertEqual(info['by_type']['download'], 1)
        self.assertEqual(info['by_type']['extract'], 1)
        self.assertEqual(info['by_type']['convert'], 1)
        self.assertEqual(info['max_age_hours'], 24)


class TestGlobalFunctions(unittest.TestCase):
    """Testes para funções globais de conveniência"""
    
    def setUp(self):
        # Limpar instância global
        import src.core.recovery
        src.core.recovery._recovery_manager = None
    
    def test_get_recovery_manager(self):
        """Testa obtenção da instância global"""
        manager1 = get_recovery_manager()
        manager2 = get_recovery_manager()
        
        # Deve retornar a mesma instância
        self.assertIs(manager1, manager2)
        self.assertIsInstance(manager1, RecoveryManager)
    
    def test_convenience_functions(self):
        """Testa funções de conveniência"""
        metadata = {"ano": 2024, "mes": 1}
        
        # Criar checkpoint
        operation_id = create_checkpoint(OperationType.DOWNLOAD, metadata, total_steps=5)
        self.assertIsInstance(operation_id, str)
        
        # Atualizar checkpoint
        success = update_checkpoint(operation_id, progress=0.5, current_step="Meio")
        self.assertTrue(success)
        
        # Verificar se pode retomar
        can_resume = can_resume_operation(OperationType.DOWNLOAD, metadata)
        self.assertFalse(can_resume)  # Status ainda é PENDING
        
        # Completar checkpoint
        success = complete_checkpoint(operation_id, success=True)
        self.assertTrue(success)
        
        # Verificar que não pode mais retomar
        can_resume = can_resume_operation(OperationType.DOWNLOAD, metadata)
        self.assertFalse(can_resume)


if __name__ == '__main__':
    unittest.main()