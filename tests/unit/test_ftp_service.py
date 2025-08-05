# -*- coding: utf-8 -*-
"""
🧪 Testes Unitários para Serviço FTP - CAGED

Testes para o módulo de serviços FTP:
- FTPService
- Operações de download
- Listagem de arquivos
- Tratamento de erros

Parte da FASE 3.3 do Plano de Melhorias CAGED.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from datetime import datetime

from src.services.ftp_service import FTPService
from src.core.config import FTPConfig
from src.core.exceptions import FTPConnectionError, FTPDownloadError


class TestFTPService:
    """Testes para FTPService"""
    
    @pytest.fixture
    def ftp_config(self):
        """Configuração FTP para testes"""
        return FTPConfig(
            server="test.ftp.server.com",
            directory="/test/path",
            timeout=30,
            max_retries=3
        )
    
    @pytest.fixture
    def ftp_service(self, ftp_config):
        """Instância do serviço FTP"""
        return FTPService(ftp_config)
    
    def test_init(self, ftp_service, ftp_config):
        """Testa inicialização do serviço"""
        assert ftp_service.config == ftp_config
        assert ftp_service._connection is None
        assert ftp_service._connected is False
    
    @patch('ftplib.FTP')
    def test_connect_success(self, mock_ftp_class, ftp_service):
        """Testa conexão bem-sucedida"""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        mock_ftp.getwelcome.return_value = "Welcome"
        
        result = ftp_service.connect()
        
        assert result is True
        assert ftp_service._connected is True
        mock_ftp.connect.assert_called_once_with(
            ftp_service.config.server, 
            timeout=ftp_service.config.timeout
        )
        mock_ftp.login.assert_called_once()
        mock_ftp.cwd.assert_called_once_with(ftp_service.config.directory)
    
    @patch('ftplib.FTP')
    def test_connect_failure(self, mock_ftp_class, ftp_service):
        """Testa falha na conexão"""
        mock_ftp_class.side_effect = Exception("Connection failed")
        
        with pytest.raises(FTPConnectionError):
            ftp_service.connect()
        
        assert ftp_service._connected is False
    
    @patch('ftplib.FTP')
    def test_disconnect(self, mock_ftp_class, ftp_service):
        """Testa desconexão"""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        
        # Conectar primeiro
        ftp_service.connect()
        assert ftp_service._connected is True
        
        # Desconectar
        ftp_service.disconnect()
        
        assert ftp_service._connected is False
        mock_ftp.quit.assert_called_once()
    
    @patch('ftplib.FTP')
    def test_list_files_success(self, mock_ftp_class, ftp_service):
        """Testa listagem de arquivos bem-sucedida"""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        
        # Configurar retorno da listagem
        expected_files = [
            "CAGEDMOV202401.7z",
            "CAGEDMOV202402.7z",
            "CAGEDEXC202401.7z"
        ]
        mock_ftp.nlst.return_value = expected_files
        
        # Conectar e listar
        ftp_service.connect()
        files = ftp_service.list_files()
        
        assert files == expected_files
        mock_ftp.nlst.assert_called_once()
    
    @patch('ftplib.FTP')
    def test_list_files_with_pattern(self, mock_ftp_class, ftp_service):
        """Testa listagem com padrão de filtro"""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        
        all_files = [
            "CAGEDMOV202401.7z",
            "CAGEDMOV202402.7z",
            "CAGEDEXC202401.7z",
            "OTHER202401.txt"
        ]
        mock_ftp.nlst.return_value = all_files
        
        ftp_service.connect()
        files = ftp_service.list_files(pattern="CAGEDMOV*.7z")
        
        expected = ["CAGEDMOV202401.7z", "CAGEDMOV202402.7z"]
        assert files == expected
    
    def test_list_files_not_connected(self, ftp_service):
        """Testa listagem sem conexão"""
        with pytest.raises(FTPConnectionError):
            ftp_service.list_files()
    
    @patch('ftplib.FTP')
    def test_download_file_success(self, mock_ftp_class, ftp_service, temp_dir):
        """Testa download bem-sucedido"""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        
        # Configurar mock para simular download
        def mock_retrbinary(cmd, callback):
            # Simular dados sendo baixados
            data = b"Dados do arquivo baixado"
            callback(data)
        
        mock_ftp.retrbinary.side_effect = mock_retrbinary
        mock_ftp.size.return_value = 1024
        
        # Conectar e baixar
        ftp_service.connect()
        local_file = temp_dir / "downloaded_file.7z"
        
        progress_calls = []
        def progress_callback(progress):
            progress_calls.append(progress)
        
        result = ftp_service.download_file(
            "CAGEDMOV202401.7z",
            str(local_file),
            progress_callback=progress_callback
        )
        
        assert result is True
        assert local_file.exists()
        assert len(progress_calls) > 0
        mock_ftp.retrbinary.assert_called_once()
    
    @patch('ftplib.FTP')
    def test_download_file_failure(self, mock_ftp_class, ftp_service, temp_dir):
        """Testa falha no download"""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        mock_ftp.retrbinary.side_effect = Exception("Download failed")
        
        ftp_service.connect()
        local_file = temp_dir / "failed_download.7z"
        
        with pytest.raises(FTPDownloadError):
            ftp_service.download_file("CAGEDMOV202401.7z", str(local_file))
    
    def test_download_file_not_connected(self, ftp_service, temp_dir):
        """Testa download sem conexão"""
        local_file = temp_dir / "test_file.7z"
        
        with pytest.raises(FTPConnectionError):
            ftp_service.download_file("CAGEDMOV202401.7z", str(local_file))
    
    @patch('ftplib.FTP')
    def test_get_file_size_success(self, mock_ftp_class, ftp_service):
        """Testa obtenção de tamanho de arquivo"""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        mock_ftp.size.return_value = 1048576  # 1MB
        
        ftp_service.connect()
        size = ftp_service.get_file_size("CAGEDMOV202401.7z")
        
        assert size == 1048576
        mock_ftp.size.assert_called_once_with("CAGEDMOV202401.7z")
    
    @patch('ftplib.FTP')
    def test_get_file_size_not_found(self, mock_ftp_class, ftp_service):
        """Testa obtenção de tamanho para arquivo inexistente"""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        mock_ftp.size.side_effect = Exception("File not found")
        
        ftp_service.connect()
        size = ftp_service.get_file_size("arquivo_inexistente.7z")
        
        assert size is None
    
    @patch('ftplib.FTP')
    def test_file_exists_true(self, mock_ftp_class, ftp_service):
        """Testa verificação de existência de arquivo (existe)"""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        mock_ftp.nlst.return_value = ["CAGEDMOV202401.7z"]
        
        ftp_service.connect()
        exists = ftp_service.file_exists("CAGEDMOV202401.7z")
        
        assert exists is True
    
    @patch('ftplib.FTP')
    def test_file_exists_false(self, mock_ftp_class, ftp_service):
        """Testa verificação de existência de arquivo (não existe)"""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        mock_ftp.nlst.return_value = []
        
        ftp_service.connect()
        exists = ftp_service.file_exists("arquivo_inexistente.7z")
        
        assert exists is False
    
    @patch('ftplib.FTP')
    def test_context_manager(self, mock_ftp_class, ftp_config):
        """Testa uso como context manager"""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        
        with FTPService(ftp_config) as ftp_service:
            assert ftp_service._connected is True
            mock_ftp.connect.assert_called_once()
            mock_ftp.login.assert_called_once()
        
        # Verificar se desconectou ao sair do contexto
        mock_ftp.quit.assert_called_once()
    
    @patch('ftplib.FTP')
    def test_retry_mechanism(self, mock_ftp_class, ftp_service, temp_dir):
        """Testa mecanismo de retry em downloads"""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        
        # Configurar para falhar 2 vezes e depois suceder
        call_count = 0
        def mock_retrbinary(cmd, callback):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise Exception("Temporary failure")
            else:
                callback(b"Success data")
        
        mock_ftp.retrbinary.side_effect = mock_retrbinary
        mock_ftp.size.return_value = 1024
        
        ftp_service.connect()
        local_file = temp_dir / "retry_test.7z"
        
        result = ftp_service.download_file(
            "CAGEDMOV202401.7z",
            str(local_file),
            max_retries=3
        )
        
        assert result is True
        assert call_count == 3  # 2 falhas + 1 sucesso
        assert local_file.exists()
    
    @patch('ftplib.FTP')
    def test_progress_callback(self, mock_ftp_class, ftp_service, temp_dir):
        """Testa callback de progresso detalhado"""
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        
        # Simular download com múltiplos chunks
        def mock_retrbinary(cmd, callback):
            chunks = [b"chunk1", b"chunk2", b"chunk3", b"chunk4"]
            for chunk in chunks:
                callback(chunk)
        
        mock_ftp.retrbinary.side_effect = mock_retrbinary
        mock_ftp.size.return_value = 24  # 6 bytes por chunk
        
        ftp_service.connect()
        local_file = temp_dir / "progress_test.7z"
        
        progress_values = []
        def progress_callback(progress):
            progress_values.append(progress)
        
        ftp_service.download_file(
            "CAGEDMOV202401.7z",
            str(local_file),
            progress_callback=progress_callback
        )
        
        # Verificar se houve progresso
        assert len(progress_values) > 0
        assert all(0 <= p <= 100 for p in progress_values)
        assert progress_values[-1] == 100  # Último deve ser 100%