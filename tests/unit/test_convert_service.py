# -*- coding: utf-8 -*-
"""
🧪 Testes Unitários para Serviço de Conversão - CAGED

Testes para o módulo de conversão de arquivos:
- ConvertService
- Conversão TXT para Parquet
- Validação de dados
- Tratamento de erros

Parte da FASE 3.3 do Plano de Melhorias CAGED.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch
import polars as pl
import pandas as pd

from src.services.convert_service import ConvertService
from src.core.exceptions import ConversionError, FileValidationError


class TestConvertService:
    """Testes para ConvertService"""
    
    @pytest.fixture
    def convert_service(self):
        """Instância do serviço de conversão"""
        return ConvertService()
    
    @pytest.fixture
    def sample_txt_content(self):
        """Conteúdo de exemplo para arquivo TXT"""
        return """ano|mes|municipio|admissoes|desligamentos
2024|1|São Paulo|1000|800
2024|1|Rio de Janeiro|750|600
2024|2|São Paulo|1100|850
2024|2|Rio de Janeiro|800|650
"""
    
    @pytest.fixture
    def sample_csv_content(self):
        """Conteúdo de exemplo para arquivo CSV"""
        return """ano,mes,municipio,admissoes,desligamentos
2024,1,São Paulo,1000,800
2024,1,Rio de Janeiro,750,600
2024,2,São Paulo,1100,850
2024,2,Rio de Janeiro,800,650
"""
    
    def test_init(self, convert_service):
        """Testa inicialização do serviço"""
        assert convert_service is not None
        assert hasattr(convert_service, 'convert_to_parquet')
        assert hasattr(convert_service, 'validate_input_file')
    
    def test_detect_delimiter_pipe(self, convert_service, temp_dir, sample_txt_content):
        """Testa detecção de delimitador pipe"""
        txt_file = temp_dir / "test.txt"
        txt_file.write_text(sample_txt_content, encoding="utf-8")
        
        delimiter = convert_service.detect_delimiter(str(txt_file))
        assert delimiter == "|"
    
    def test_detect_delimiter_comma(self, convert_service, temp_dir, sample_csv_content):
        """Testa detecção de delimitador vírgula"""
        csv_file = temp_dir / "test.csv"
        csv_file.write_text(sample_csv_content, encoding="utf-8")
        
        delimiter = convert_service.detect_delimiter(str(csv_file))
        assert delimiter == ","
    
    def test_detect_delimiter_semicolon(self, convert_service, temp_dir):
        """Testa detecção de delimitador ponto e vírgula"""
        content = """ano;mes;municipio;admissoes;desligamentos
2024;1;São Paulo;1000;800
"""
        csv_file = temp_dir / "test.csv"
        csv_file.write_text(content, encoding="utf-8")
        
        delimiter = convert_service.detect_delimiter(str(csv_file))
        assert delimiter == ";"
    
    def test_validate_input_file_exists(self, convert_service, temp_dir, sample_txt_content):
        """Testa validação de arquivo existente"""
        txt_file = temp_dir / "test.txt"
        txt_file.write_text(sample_txt_content, encoding="utf-8")
        
        result = convert_service.validate_input_file(str(txt_file))
        assert result is True
    
    def test_validate_input_file_not_exists(self, convert_service):
        """Testa validação de arquivo inexistente"""
        with pytest.raises(FileValidationError):
            convert_service.validate_input_file("/path/to/nonexistent.txt")
    
    def test_validate_input_file_empty(self, convert_service, temp_dir):
        """Testa validação de arquivo vazio"""
        empty_file = temp_dir / "empty.txt"
        empty_file.touch()
        
        with pytest.raises(FileValidationError):
            convert_service.validate_input_file(str(empty_file))
    
    def test_validate_input_file_invalid_extension(self, convert_service, temp_dir):
        """Testa validação de arquivo com extensão inválida"""
        invalid_file = temp_dir / "test.pdf"
        invalid_file.write_text("Some content", encoding="utf-8")
        
        with pytest.raises(FileValidationError):
            convert_service.validate_input_file(str(invalid_file))
    
    @patch('polars.read_csv')
    def test_convert_to_parquet_success(self, mock_read_csv, convert_service, temp_dir, sample_txt_content):
        """Testa conversão bem-sucedida para Parquet"""
        # Configurar mock do polars
        mock_df = MagicMock()
        mock_read_csv.return_value = mock_df
        
        # Criar arquivo de entrada
        input_file = temp_dir / "input.txt"
        input_file.write_text(sample_txt_content, encoding="utf-8")
        
        output_file = temp_dir / "output.parquet"
        
        progress_calls = []
        def progress_callback(progress):
            progress_calls.append(progress)
        
        result = convert_service.convert_to_parquet(
            str(input_file),
            str(output_file),
            progress_callback=progress_callback
        )
        
        assert result is True
        assert len(progress_calls) > 0
        mock_read_csv.assert_called_once()
        mock_df.write_parquet.assert_called_once_with(str(output_file))
    
    @patch('polars.read_csv')
    def test_convert_to_parquet_with_schema(self, mock_read_csv, convert_service, temp_dir, sample_txt_content):
        """Testa conversão com schema específico"""
        mock_df = MagicMock()
        mock_read_csv.return_value = mock_df
        
        input_file = temp_dir / "input.txt"
        input_file.write_text(sample_txt_content, encoding="utf-8")
        
        output_file = temp_dir / "output.parquet"
        
        schema = {
            "ano": pl.Int32,
            "mes": pl.Int32,
            "municipio": pl.Utf8,
            "admissoes": pl.Int64,
            "desligamentos": pl.Int64
        }
        
        convert_service.convert_to_parquet(
            str(input_file),
            str(output_file),
            schema=schema
        )
        
        # Verificar se schema foi aplicado
        mock_read_csv.assert_called_once()
        call_args = mock_read_csv.call_args
        assert "schema" in call_args.kwargs or "dtypes" in call_args.kwargs
    
    @patch('polars.read_csv')
    def test_convert_to_parquet_failure(self, mock_read_csv, convert_service, temp_dir, sample_txt_content):
        """Testa falha na conversão"""
        # Configurar mock para falhar
        mock_read_csv.side_effect = Exception("Conversion failed")
        
        input_file = temp_dir / "input.txt"
        input_file.write_text(sample_txt_content, encoding="utf-8")
        
        output_file = temp_dir / "output.parquet"
        
        with pytest.raises(ConversionError):
            convert_service.convert_to_parquet(str(input_file), str(output_file))
    
    def test_convert_to_parquet_invalid_input(self, convert_service, temp_dir):
        """Testa conversão com arquivo de entrada inválido"""
        output_file = temp_dir / "output.parquet"
        
        with pytest.raises(FileValidationError):
            convert_service.convert_to_parquet(
                "/path/to/nonexistent.txt",
                str(output_file)
            )
    
    @patch('polars.read_csv')
    def test_convert_with_encoding_detection(self, mock_read_csv, convert_service, temp_dir):
        """Testa conversão com detecção de encoding"""
        mock_df = MagicMock()
        mock_read_csv.return_value = mock_df
        
        # Criar arquivo com encoding específico
        content = "ano|mes|município|admissões|desligamentos\n2024|1|São Paulo|1000|800"
        input_file = temp_dir / "input.txt"
        input_file.write_text(content, encoding="latin-1")
        
        output_file = temp_dir / "output.parquet"
        
        convert_service.convert_to_parquet(
            str(input_file),
            str(output_file),
            auto_detect_encoding=True
        )
        
        mock_read_csv.assert_called_once()
        # Verificar se encoding foi detectado e usado
        call_args = mock_read_csv.call_args
        assert "encoding" in call_args.kwargs
    
    @patch('polars.read_csv')
    def test_convert_with_compression(self, mock_read_csv, convert_service, temp_dir, sample_txt_content):
        """Testa conversão com compressão"""
        mock_df = MagicMock()
        mock_read_csv.return_value = mock_df
        
        input_file = temp_dir / "input.txt"
        input_file.write_text(sample_txt_content, encoding="utf-8")
        
        output_file = temp_dir / "output.parquet"
        
        convert_service.convert_to_parquet(
            str(input_file),
            str(output_file),
            compression="snappy"
        )
        
        mock_df.write_parquet.assert_called_once()
        call_args = mock_df.write_parquet.call_args
        assert "compression" in call_args.kwargs
        assert call_args.kwargs["compression"] == "snappy"
    
    @patch('polars.read_csv')
    def test_convert_with_chunk_processing(self, mock_read_csv, convert_service, temp_dir):
        """Testa conversão com processamento em chunks"""
        # Simular arquivo grande
        large_content = "ano|mes|municipio|admissoes|desligamentos\n"
        for i in range(10000):
            large_content += f"2024|1|Município {i}|{1000+i}|{800+i}\n"
        
        input_file = temp_dir / "large_input.txt"
        input_file.write_text(large_content, encoding="utf-8")
        
        output_file = temp_dir / "output.parquet"
        
        # Configurar mock para retornar chunks
        mock_chunks = [MagicMock() for _ in range(3)]
        mock_read_csv.return_value = mock_chunks[0]  # Primeiro chunk
        
        convert_service.convert_to_parquet(
            str(input_file),
            str(output_file),
            chunk_size=5000
        )
        
        mock_read_csv.assert_called()
    
    def test_get_file_info(self, convert_service, temp_dir, sample_txt_content):
        """Testa obtenção de informações do arquivo"""
        input_file = temp_dir / "input.txt"
        input_file.write_text(sample_txt_content, encoding="utf-8")
        
        info = convert_service.get_file_info(str(input_file))
        
        assert "size_bytes" in info
        assert "line_count" in info
        assert "delimiter" in info
        assert "encoding" in info
        assert "estimated_rows" in info
        
        assert info["line_count"] == 5  # 4 linhas de dados + cabeçalho
        assert info["delimiter"] == "|"
    
    def test_get_file_info_invalid_file(self, convert_service):
        """Testa obtenção de info de arquivo inválido"""
        with pytest.raises(FileValidationError):
            convert_service.get_file_info("/path/to/nonexistent.txt")
    
    @patch('polars.read_csv')
    def test_preview_data(self, mock_read_csv, convert_service, temp_dir, sample_txt_content):
        """Testa preview dos dados"""
        # Configurar mock para retornar dados de preview
        mock_df = MagicMock()
        mock_df.head.return_value = mock_df
        mock_df.to_pandas.return_value = pd.DataFrame({
            "ano": [2024, 2024],
            "mes": [1, 1],
            "municipio": ["São Paulo", "Rio de Janeiro"],
            "admissoes": [1000, 750],
            "desligamentos": [800, 600]
        })
        mock_read_csv.return_value = mock_df
        
        input_file = temp_dir / "input.txt"
        input_file.write_text(sample_txt_content, encoding="utf-8")
        
        preview = convert_service.preview_data(str(input_file), rows=2)
        
        assert isinstance(preview, pd.DataFrame)
        assert len(preview) == 2
        mock_df.head.assert_called_once_with(2)
    
    def test_validate_data_quality(self, convert_service, temp_dir):
        """Testa validação de qualidade dos dados"""
        # Criar arquivo com dados problemáticos
        problematic_content = """ano|mes|municipio|admissoes|desligamentos
2024|1|São Paulo|1000|800
2024||Rio de Janeiro|750|600
2024|13|Belo Horizonte|abc|def
|2|Salvador|1100|850
"""
        
        input_file = temp_dir / "problematic.txt"
        input_file.write_text(problematic_content, encoding="utf-8")
        
        issues = convert_service.validate_data_quality(str(input_file))
        
        assert "missing_values" in issues
        assert "invalid_types" in issues
        assert "invalid_ranges" in issues
        
        # Deve detectar problemas
        assert len(issues["missing_values"]) > 0
        assert len(issues["invalid_types"]) > 0
    
    def test_get_supported_formats(self, convert_service):
        """Testa obtenção de formatos suportados"""
        input_formats = convert_service.get_supported_input_formats()
        output_formats = convert_service.get_supported_output_formats()
        
        assert isinstance(input_formats, list)
        assert isinstance(output_formats, list)
        
        assert ".txt" in input_formats
        assert ".csv" in input_formats
        assert ".parquet" in output_formats
    
    def test_estimate_conversion_time(self, convert_service, temp_dir, sample_txt_content):
        """Testa estimativa de tempo de conversão"""
        input_file = temp_dir / "input.txt"
        input_file.write_text(sample_txt_content, encoding="utf-8")
        
        estimate = convert_service.estimate_conversion_time(str(input_file))
        
        assert "estimated_seconds" in estimate
        assert "file_size_mb" in estimate
        assert "estimated_rows" in estimate
        
        assert estimate["estimated_seconds"] > 0
    
    @patch('polars.read_csv')
    def test_convert_with_data_validation(self, mock_read_csv, convert_service, temp_dir, sample_txt_content):
        """Testa conversão com validação de dados"""
        mock_df = MagicMock()
        mock_read_csv.return_value = mock_df
        
        input_file = temp_dir / "input.txt"
        input_file.write_text(sample_txt_content, encoding="utf-8")
        
        output_file = temp_dir / "output.parquet"
        
        # Definir regras de validação
        validation_rules = {
            "ano": {"min": 2020, "max": 2030},
            "mes": {"min": 1, "max": 12},
            "admissoes": {"min": 0},
            "desligamentos": {"min": 0}
        }
        
        result = convert_service.convert_to_parquet(
            str(input_file),
            str(output_file),
            validation_rules=validation_rules
        )
        
        assert result is True
        mock_read_csv.assert_called_once()
    
    def test_batch_convert(self, convert_service, temp_dir, sample_txt_content):
        """Testa conversão em lote"""
        # Criar múltiplos arquivos de entrada
        input_files = []
        for i in range(3):
            input_file = temp_dir / f"input_{i}.txt"
            input_file.write_text(sample_txt_content, encoding="utf-8")
            input_files.append(str(input_file))
        
        output_dir = temp_dir / "output"
        output_dir.mkdir()
        
        progress_calls = []
        def progress_callback(file_index, file_progress):
            progress_calls.append((file_index, file_progress))
        
        with patch('polars.read_csv') as mock_read_csv:
            mock_df = MagicMock()
            mock_read_csv.return_value = mock_df
            
            results = convert_service.batch_convert(
                input_files,
                str(output_dir),
                progress_callback=progress_callback
            )
        
        assert len(results) == 3
        assert all(result["success"] for result in results)
        assert len(progress_calls) > 0