# -*- coding: utf-8 -*-
"""
🧪 Testes Unitários para Serviço de Extração - CAGED

Testes para o módulo de extração de arquivos:
- ExtractService
- Extração de arquivos 7z
- Validação de arquivos
- Tratamento de erros

Parte da FASE 3.3 do Plano de Melhorias CAGED.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open
import py7zr

from src.services.extract_service import ExtractService
from src.core.exceptions import ExtractionError, FileValidationError


class TestExtractService:
    """Testes para ExtractService"""
    
    @pytest.fixture
    def extract_service(self):
        """Instância do serviço de extração"""
        return ExtractService()
    
    def test_init(self, extract_service):
        """Testa inicialização do serviço"""
        assert extract_service is not None
        assert hasattr(extract_service, 'extract_7z')
        assert hasattr(extract_service, 'validate_7z_file')
    
    def test_validate_7z_file_exists(self, extract_service, sample_file):
        """Testa validação de arquivo existente"""
        # Criar arquivo que simula um 7z válido
        sample_file.write_bytes(b"7z\xBC\xAF\x27\x1C")  # Magic bytes do 7z
        
        result = extract_service.validate_7z_file(str(sample_file))
        assert result is True
    
    def test_validate_7z_file_not_exists(self, extract_service):
        """Testa validação de arquivo inexistente"""
        with pytest.raises(FileValidationError):
            extract_service.validate_7z_file("/path/to/nonexistent/file.7z")
    
    def test_validate_7z_file_invalid_extension(self, extract_service, sample_file):
        """Testa validação de arquivo com extensão inválida"""
        txt_file = sample_file.parent / "test.txt"
        txt_file.write_text("Not a 7z file")
        
        with pytest.raises(FileValidationError):
            extract_service.validate_7z_file(str(txt_file))
    
    def test_validate_7z_file_empty(self, extract_service, temp_dir):
        """Testa validação de arquivo vazio"""
        empty_file = temp_dir / "empty.7z"
        empty_file.touch()
        
        with pytest.raises(FileValidationError):
            extract_service.validate_7z_file(str(empty_file))
    
    @patch('py7zr.SevenZipFile')
    def test_extract_7z_success(self, mock_7z_class, extract_service, temp_dir):
        """Testa extração bem-sucedida"""
        # Configurar mock
        mock_7z = MagicMock()
        mock_7z_class.return_value.__enter__.return_value = mock_7z
        
        # Simular arquivos extraídos
        extracted_files = [
            "CAGEDMOV202401.txt",
            "CAGEDEXC202401.txt"
        ]
        mock_7z.getnames.return_value = extracted_files
        
        # Criar arquivo 7z de teste
        archive_file = temp_dir / "test.7z"
        archive_file.write_bytes(b"7z\xBC\xAF\x27\x1C")
        
        output_dir = temp_dir / "extracted"
        
        progress_calls = []
        def progress_callback(progress):
            progress_calls.append(progress)
        
        result = extract_service.extract_7z(
            str(archive_file),
            str(output_dir),
            progress_callback=progress_callback
        )
        
        assert result == extracted_files
        assert len(progress_calls) > 0
        mock_7z.extractall.assert_called_once_with(str(output_dir))
    
    @patch('py7zr.SevenZipFile')
    def test_extract_7z_failure(self, mock_7z_class, extract_service, temp_dir):
        """Testa falha na extração"""
        # Configurar mock para falhar
        mock_7z_class.side_effect = py7zr.Bad7zFile("Invalid 7z file")
        
        archive_file = temp_dir / "invalid.7z"
        archive_file.write_bytes(b"invalid data")
        
        output_dir = temp_dir / "extracted"
        
        with pytest.raises(ExtractionError):
            extract_service.extract_7z(str(archive_file), str(output_dir))
    
    def test_extract_7z_invalid_file(self, extract_service, temp_dir):
        """Testa extração de arquivo inválido"""
        # Arquivo inexistente
        with pytest.raises(FileValidationError):
            extract_service.extract_7z(
                "/path/to/nonexistent.7z",
                str(temp_dir)
            )
    
    @patch('py7zr.SevenZipFile')
    def test_extract_7z_with_filter(self, mock_7z_class, extract_service, temp_dir):
        """Testa extração com filtro de arquivos"""
        mock_7z = MagicMock()
        mock_7z_class.return_value.__enter__.return_value = mock_7z
        
        # Simular vários arquivos
        all_files = [
            "CAGEDMOV202401.txt",
            "CAGEDEXC202401.txt",
            "README.txt",
            "other_file.csv"
        ]
        mock_7z.getnames.return_value = all_files
        
        archive_file = temp_dir / "test.7z"
        archive_file.write_bytes(b"7z\xBC\xAF\x27\x1C")
        
        output_dir = temp_dir / "extracted"
        
        # Extrair apenas arquivos CAGED
        result = extract_service.extract_7z(
            str(archive_file),
            str(output_dir),
            file_filter=lambda name: name.startswith("CAGED")
        )
        
        expected_files = ["CAGEDMOV202401.txt", "CAGEDEXC202401.txt"]
        assert result == expected_files
    
    @patch('py7zr.SevenZipFile')
    def test_extract_7z_progress_callback(self, mock_7z_class, extract_service, temp_dir):
        """Testa callback de progresso detalhado"""
        mock_7z = MagicMock()
        mock_7z_class.return_value.__enter__.return_value = mock_7z
        
        # Simular extração com progresso
        def mock_extractall(path, callback=None):
            if callback:
                # Simular progresso em etapas
                for progress in [0, 25, 50, 75, 100]:
                    callback(progress)
        
        mock_7z.extractall.side_effect = mock_extractall
        mock_7z.getnames.return_value = ["test.txt"]
        
        archive_file = temp_dir / "test.7z"
        archive_file.write_bytes(b"7z\xBC\xAF\x27\x1C")
        
        output_dir = temp_dir / "extracted"
        
        progress_values = []
        def progress_callback(progress):
            progress_values.append(progress)
        
        extract_service.extract_7z(
            str(archive_file),
            str(output_dir),
            progress_callback=progress_callback
        )
        
        # Verificar se houve progresso
        assert len(progress_values) > 0
        assert all(0 <= p <= 100 for p in progress_values)
    
    @patch('py7zr.SevenZipFile')
    def test_get_archive_info(self, mock_7z_class, extract_service, temp_dir):
        """Testa obtenção de informações do arquivo"""
        mock_7z = MagicMock()
        mock_7z_class.return_value.__enter__.return_value = mock_7z
        
        # Configurar informações do arquivo
        mock_file_info = MagicMock()
        mock_file_info.filename = "CAGEDMOV202401.txt"
        mock_file_info.uncompressed = 1048576  # 1MB
        mock_file_info.compressed = 524288     # 512KB
        
        mock_7z.list.return_value = [mock_file_info]
        
        archive_file = temp_dir / "test.7z"
        archive_file.write_bytes(b"7z\xBC\xAF\x27\x1C")
        
        info = extract_service.get_archive_info(str(archive_file))
        
        assert "files" in info
        assert "total_uncompressed" in info
        assert "total_compressed" in info
        assert "compression_ratio" in info
        
        assert len(info["files"]) == 1
        assert info["total_uncompressed"] == 1048576
        assert info["total_compressed"] == 524288
    
    def test_get_archive_info_invalid_file(self, extract_service):
        """Testa obtenção de info de arquivo inválido"""
        with pytest.raises(FileValidationError):
            extract_service.get_archive_info("/path/to/nonexistent.7z")
    
    @patch('py7zr.SevenZipFile')
    def test_test_archive_integrity(self, mock_7z_class, extract_service, temp_dir):
        """Testa verificação de integridade do arquivo"""
        mock_7z = MagicMock()
        mock_7z_class.return_value.__enter__.return_value = mock_7z
        
        # Simular teste bem-sucedido
        mock_7z.testzip.return_value = None  # None significa OK
        
        archive_file = temp_dir / "test.7z"
        archive_file.write_bytes(b"7z\xBC\xAF\x27\x1C")
        
        result = extract_service.test_archive_integrity(str(archive_file))
        
        assert result is True
        mock_7z.testzip.assert_called_once()
    
    @patch('py7zr.SevenZipFile')
    def test_test_archive_integrity_corrupted(self, mock_7z_class, extract_service, temp_dir):
        """Testa verificação de arquivo corrompido"""
        mock_7z = MagicMock()
        mock_7z_class.return_value.__enter__.return_value = mock_7z
        
        # Simular arquivo corrompido
        mock_7z.testzip.return_value = "corrupted_file.txt"
        
        archive_file = temp_dir / "corrupted.7z"
        archive_file.write_bytes(b"7z\xBC\xAF\x27\x1C")
        
        result = extract_service.test_archive_integrity(str(archive_file))
        
        assert result is False
    
    @patch('py7zr.SevenZipFile')
    def test_extract_specific_files(self, mock_7z_class, extract_service, temp_dir):
        """Testa extração de arquivos específicos"""
        mock_7z = MagicMock()
        mock_7z_class.return_value.__enter__.return_value = mock_7z
        
        # Configurar arquivos disponíveis
        available_files = [
            "CAGEDMOV202401.txt",
            "CAGEDEXC202401.txt",
            "README.txt"
        ]
        mock_7z.getnames.return_value = available_files
        
        archive_file = temp_dir / "test.7z"
        archive_file.write_bytes(b"7z\xBC\xAF\x27\x1C")
        
        output_dir = temp_dir / "extracted"
        
        # Extrair apenas arquivos específicos
        target_files = ["CAGEDMOV202401.txt", "CAGEDEXC202401.txt"]
        result = extract_service.extract_specific_files(
            str(archive_file),
            str(output_dir),
            target_files
        )
        
        assert result == target_files
        # Verificar se extract foi chamado com os arquivos corretos
        mock_7z.extract.assert_called()
    
    @patch('py7zr.SevenZipFile')
    def test_extract_specific_files_not_found(self, mock_7z_class, extract_service, temp_dir):
        """Testa extração de arquivos que não existem no arquivo"""
        mock_7z = MagicMock()
        mock_7z_class.return_value.__enter__.return_value = mock_7z
        
        # Configurar arquivos disponíveis
        mock_7z.getnames.return_value = ["other_file.txt"]
        
        archive_file = temp_dir / "test.7z"
        archive_file.write_bytes(b"7z\xBC\xAF\x27\x1C")
        
        output_dir = temp_dir / "extracted"
        
        # Tentar extrair arquivo inexistente
        target_files = ["nonexistent.txt"]
        
        with pytest.raises(ExtractionError):
            extract_service.extract_specific_files(
                str(archive_file),
                str(output_dir),
                target_files
            )
    
    def test_get_supported_formats(self, extract_service):
        """Testa obtenção de formatos suportados"""
        formats = extract_service.get_supported_formats()
        
        assert isinstance(formats, list)
        assert ".7z" in formats
        assert len(formats) > 0
    
    def test_is_supported_format(self, extract_service):
        """Testa verificação de formato suportado"""
        assert extract_service.is_supported_format("test.7z") is True
        assert extract_service.is_supported_format("test.zip") is False
        assert extract_service.is_supported_format("test.txt") is False
    
    @patch('py7zr.SevenZipFile')
    def test_extract_with_password(self, mock_7z_class, extract_service, temp_dir):
        """Testa extração com senha"""
        mock_7z = MagicMock()
        mock_7z_class.return_value.__enter__.return_value = mock_7z
        mock_7z.getnames.return_value = ["protected_file.txt"]
        
        archive_file = temp_dir / "protected.7z"
        archive_file.write_bytes(b"7z\xBC\xAF\x27\x1C")
        
        output_dir = temp_dir / "extracted"
        
        result = extract_service.extract_7z(
            str(archive_file),
            str(output_dir),
            password="secret123"
        )
        
        assert result == ["protected_file.txt"]
        # Verificar se a senha foi passada para o SevenZipFile
        mock_7z_class.assert_called_with(archive_file, mode="r", password="secret123")