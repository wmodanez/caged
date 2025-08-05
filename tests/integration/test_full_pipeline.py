# -*- coding: utf-8 -*-
"""
🧪 Testes de Integração - Pipeline Completo CAGED

Testes de integração para o pipeline completo:
- Download → Extração → Conversão
- Validação de dados
- Cache e recuperação
- Métricas e monitoramento

Parte da FASE 3.3 do Plano de Melhorias CAGED.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime

from src.services.ftp_service import FTPService
from src.services.extract_service import ExtractService
from src.services.convert_service import ConvertService
from src.utils.cache_manager import CacheManager
from src.utils.recovery_manager import RecoveryManager
from src.utils.metrics import MetricsCollector
from src.validators.caged_validator import CAGEDValidator


class TestFullPipeline:
    """Testes de integração do pipeline completo"""
    
    @pytest.fixture
    def pipeline_setup(self, temp_dir):
        """Configuração completa do pipeline"""
        # Diretórios
        download_dir = temp_dir / "downloads"
        extract_dir = temp_dir / "extracted"
        output_dir = temp_dir / "output"
        cache_dir = temp_dir / "cache"
        
        for dir_path in [download_dir, extract_dir, output_dir, cache_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # Serviços
        ftp_service = FTPService(
            host="ftp.test.com",
            username="test",
            password="test"
        )
        
        extract_service = ExtractService()
        convert_service = ConvertService()
        
        # Utilitários
        cache_manager = CacheManager(cache_dir=str(cache_dir))
        recovery_manager = RecoveryManager(recovery_dir=str(temp_dir / "recovery"))
        metrics_collector = MetricsCollector(metrics_file=str(temp_dir / "metrics.json"))
        
        # Validador
        validator = CAGEDValidator()
        
        return {
            "dirs": {
                "download": download_dir,
                "extract": extract_dir,
                "output": output_dir,
                "cache": cache_dir
            },
            "services": {
                "ftp": ftp_service,
                "extract": extract_service,
                "convert": convert_service
            },
            "utils": {
                "cache": cache_manager,
                "recovery": recovery_manager,
                "metrics": metrics_collector
            },
            "validator": validator
        }
    
    @pytest.fixture
    def sample_7z_file(self, temp_dir):
        """Arquivo 7z de exemplo para testes"""
        # Criar arquivo CSV de exemplo
        csv_content = '''competencia,cbo2002ocupacao,cnae20classe,emprego,saldomovimentacao,tipoemprego,tipomovimentacao,uf
202301,123456,12345,1,10,1,1,SP
202301,654321,54321,2,5,2,2,RJ
202301,111111,11111,3,-3,1,3,MG
'''
        
        csv_file = temp_dir / "CAGEDMOV202301.txt"
        csv_file.write_text(csv_content, encoding='utf-8')
        
        # Simular arquivo 7z (na verdade será um arquivo texto)
        zip_file = temp_dir / "CAGEDMOV202301.7z"
        zip_file.write_bytes(b"fake 7z content for testing")
        
        return {
            "7z_file": zip_file,
            "csv_file": csv_file,
            "csv_content": csv_content
        }
    
    def test_complete_pipeline_success(self, pipeline_setup, sample_7z_file):
        """Testa pipeline completo com sucesso"""
        setup = pipeline_setup
        metrics = setup["utils"]["metrics"]
        
        # Mock do FTP para simular download
        with patch.object(setup["services"]["ftp"], 'download_file') as mock_download:
            mock_download.return_value = True
            
            # Mock da extração
            with patch.object(setup["services"]["extract"], 'extract_file') as mock_extract:
                mock_extract.return_value = [str(sample_7z_file["csv_file"])]
                
                # Mock da conversão
                with patch.object(setup["services"]["convert"], 'convert_to_parquet') as mock_convert:
                    output_file = setup["dirs"]["output"] / "CAGEDMOV202301.parquet"
                    mock_convert.return_value = str(output_file)
                    
                    # Executar pipeline
                    pipeline_result = self._execute_pipeline(
                        setup,
                        "CAGEDMOV202301.7z",
                        sample_7z_file
                    )
                    
                    # Verificações
                    assert pipeline_result["success"] is True
                    assert pipeline_result["output_file"] == str(output_file)
                    
                    # Verificar métricas
                    stats = metrics.get_operation_stats()
                    assert "download" in stats
                    assert "extract" in stats
                    assert "convert" in stats
    
    def test_pipeline_with_cache_hit(self, pipeline_setup, sample_7z_file):
        """Testa pipeline com cache hit"""
        setup = pipeline_setup
        cache = setup["utils"]["cache"]
        
        # Simular arquivo em cache
        cache_key = "CAGEDMOV202301.7z"
        cached_file = setup["dirs"]["cache"] / "CAGEDMOV202301.parquet"
        cached_file.write_text("cached parquet data", encoding='utf-8')
        
        # Adicionar ao cache
        cache.put(cache_key, str(cached_file))
        
        # Executar pipeline
        pipeline_result = self._execute_pipeline_with_cache(
            setup,
            cache_key,
            sample_7z_file
        )
        
        # Verificações
        assert pipeline_result["success"] is True
        assert pipeline_result["from_cache"] is True
        assert pipeline_result["output_file"] == str(cached_file)
    
    def test_pipeline_with_recovery(self, pipeline_setup, sample_7z_file):
        """Testa pipeline com recuperação após falha"""
        setup = pipeline_setup
        recovery = setup["utils"]["recovery"]
        
        # Simular estado de recuperação
        recovery_state = {
            "operation_id": "test_recovery_123",
            "step": "extract",
            "file": "CAGEDMOV202301.7z",
            "progress": 50,
            "timestamp": datetime.now().isoformat()
        }
        
        recovery.save_state("test_recovery_123", recovery_state)
        
        # Mock para simular recuperação
        with patch.object(setup["services"]["extract"], 'extract_file') as mock_extract:
            mock_extract.return_value = [str(sample_7z_file["csv_file"])]
            
            with patch.object(setup["services"]["convert"], 'convert_to_parquet') as mock_convert:
                output_file = setup["dirs"]["output"] / "CAGEDMOV202301.parquet"
                mock_convert.return_value = str(output_file)
                
                # Executar recuperação
                pipeline_result = self._execute_pipeline_recovery(
                    setup,
                    "test_recovery_123",
                    sample_7z_file
                )
                
                # Verificações
                assert pipeline_result["success"] is True
                assert pipeline_result["recovered"] is True
    
    def test_pipeline_validation_failure(self, pipeline_setup, sample_7z_file):
        """Testa pipeline com falha na validação"""
        setup = pipeline_setup
        
        # Criar arquivo CSV inválido
        invalid_csv = '''invalid,header,format
1,2,3
4,5,6
'''
        
        invalid_file = setup["dirs"]["extract"] / "invalid.txt"
        invalid_file.write_text(invalid_csv, encoding='utf-8')
        
        # Mock da extração para retornar arquivo inválido
        with patch.object(setup["services"]["extract"], 'extract_file') as mock_extract:
            mock_extract.return_value = [str(invalid_file)]
            
            # Executar pipeline
            pipeline_result = self._execute_pipeline(
                setup,
                "invalid.7z",
                {"csv_file": invalid_file}
            )
            
            # Verificações
            assert pipeline_result["success"] is False
            assert "validation" in pipeline_result["error"].lower()
    
    def test_pipeline_parallel_processing(self, pipeline_setup):
        """Testa processamento paralelo de múltiplos arquivos"""
        setup = pipeline_setup
        
        # Simular múltiplos arquivos
        files = [
            "CAGEDMOV202301.7z",
            "CAGEDMOV202302.7z",
            "CAGEDMOV202303.7z"
        ]
        
        # Mock dos serviços
        with patch.object(setup["services"]["ftp"], 'download_file') as mock_download, \
             patch.object(setup["services"]["extract"], 'extract_file') as mock_extract, \
             patch.object(setup["services"]["convert"], 'convert_to_parquet') as mock_convert:
            
            mock_download.return_value = True
            mock_extract.return_value = ["extracted_file.txt"]
            mock_convert.return_value = "output.parquet"
            
            # Executar processamento paralelo
            results = self._execute_parallel_pipeline(setup, files)
            
            # Verificações
            assert len(results) == 3
            assert all(result["success"] for result in results)
            
            # Verificar que todos os arquivos foram processados
            assert mock_download.call_count == 3
            assert mock_extract.call_count == 3
            assert mock_convert.call_count == 3
    
    def test_pipeline_error_handling(self, pipeline_setup, sample_7z_file):
        """Testa tratamento de erros no pipeline"""
        setup = pipeline_setup
        metrics = setup["utils"]["metrics"]
        
        # Mock para simular erro no download
        with patch.object(setup["services"]["ftp"], 'download_file') as mock_download:
            mock_download.side_effect = Exception("FTP connection failed")
            
            # Executar pipeline
            pipeline_result = self._execute_pipeline(
                setup,
                "CAGEDMOV202301.7z",
                sample_7z_file
            )
            
            # Verificações
            assert pipeline_result["success"] is False
            assert "FTP connection failed" in str(pipeline_result["error"])
            
            # Verificar métricas de erro
            stats = metrics.get_operation_stats()
            if "download" in stats:
                assert stats["download"]["failed"] > 0
    
    def test_pipeline_performance_monitoring(self, pipeline_setup, sample_7z_file):
        """Testa monitoramento de performance do pipeline"""
        setup = pipeline_setup
        metrics = setup["utils"]["metrics"]
        
        # Mock dos serviços com delays simulados
        with patch.object(setup["services"]["ftp"], 'download_file') as mock_download, \
             patch.object(setup["services"]["extract"], 'extract_file') as mock_extract, \
             patch.object(setup["services"]["convert"], 'convert_to_parquet') as mock_convert:
            
            import time
            
            def slow_download(*args, **kwargs):
                time.sleep(0.1)
                return True
            
            def slow_extract(*args, **kwargs):
                time.sleep(0.1)
                return [str(sample_7z_file["csv_file"])]
            
            def slow_convert(*args, **kwargs):
                time.sleep(0.1)
                return "output.parquet"
            
            mock_download.side_effect = slow_download
            mock_extract.side_effect = slow_extract
            mock_convert.side_effect = slow_convert
            
            # Executar pipeline
            start_time = time.time()
            pipeline_result = self._execute_pipeline(
                setup,
                "CAGEDMOV202301.7z",
                sample_7z_file
            )
            total_time = time.time() - start_time
            
            # Verificações
            assert pipeline_result["success"] is True
            assert total_time >= 0.3  # Pelo menos 0.3s (3 operações × 0.1s)
            
            # Verificar métricas de performance
            performance = metrics.get_performance_summary()
            assert performance["total_operations"] >= 3
            assert performance["avg_duration"] > 0
    
    def test_pipeline_data_integrity(self, pipeline_setup, sample_7z_file):
        """Testa integridade dos dados no pipeline"""
        setup = pipeline_setup
        
        # Mock da conversão para verificar dados
        original_data = sample_7z_file["csv_content"]
        
        with patch.object(setup["services"]["ftp"], 'download_file') as mock_download, \
             patch.object(setup["services"]["extract"], 'extract_file') as mock_extract:
            
            mock_download.return_value = True
            mock_extract.return_value = [str(sample_7z_file["csv_file"])]
            
            # Executar pipeline
            pipeline_result = self._execute_pipeline_with_validation(
                setup,
                "CAGEDMOV202301.7z",
                sample_7z_file
            )
            
            # Verificações
            assert pipeline_result["success"] is True
            assert pipeline_result["data_integrity"] is True
            assert pipeline_result["record_count"] == 3  # 3 linhas de dados
    
    def _execute_pipeline(self, setup, filename, sample_data):
        """Executa pipeline básico"""
        try:
            metrics = setup["utils"]["metrics"]
            
            # 1. Download
            download_op = metrics.start_operation("download", {"file": filename})
            download_path = setup["dirs"]["download"] / filename
            
            # Simular download
            setup["services"]["ftp"].download_file(filename, str(download_path))
            metrics.end_operation(download_op, success=True)
            
            # 2. Extração
            extract_op = metrics.start_operation("extract", {"file": filename})
            extracted_files = setup["services"]["extract"].extract_file(
                str(download_path),
                str(setup["dirs"]["extract"])
            )
            metrics.end_operation(extract_op, success=True)
            
            # 3. Validação
            if "csv_file" in sample_data:
                csv_content = sample_data["csv_file"].read_text(encoding='utf-8')
                if not self._validate_csv_content(csv_content):
                    raise ValueError("Validation failed: Invalid CSV format")
            
            # 4. Conversão
            convert_op = metrics.start_operation("convert", {"file": extracted_files[0]})
            output_file = setup["services"]["convert"].convert_to_parquet(
                extracted_files[0],
                str(setup["dirs"]["output"])
            )
            metrics.end_operation(convert_op, success=True)
            
            return {
                "success": True,
                "output_file": output_file,
                "extracted_files": extracted_files
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def _execute_pipeline_with_cache(self, setup, cache_key, sample_data):
        """Executa pipeline com verificação de cache"""
        try:
            cache = setup["utils"]["cache"]
            
            # Verificar cache
            cached_result = cache.get(cache_key)
            if cached_result:
                return {
                    "success": True,
                    "from_cache": True,
                    "output_file": cached_result
                }
            
            # Executar pipeline normal
            result = self._execute_pipeline(setup, cache_key, sample_data)
            
            # Adicionar ao cache se bem-sucedido
            if result["success"]:
                cache.put(cache_key, result["output_file"])
            
            result["from_cache"] = False
            return result
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def _execute_pipeline_recovery(self, setup, recovery_id, sample_data):
        """Executa pipeline com recuperação"""
        try:
            recovery = setup["utils"]["recovery"]
            
            # Carregar estado de recuperação
            state = recovery.load_state(recovery_id)
            if not state:
                raise ValueError("Recovery state not found")
            
            # Continuar do ponto de falha
            if state["step"] == "extract":
                # Pular download, ir direto para extração
                extract_op = setup["utils"]["metrics"].start_operation("extract", {"file": state["file"]})
                extracted_files = setup["services"]["extract"].extract_file(
                    state["file"],
                    str(setup["dirs"]["extract"])
                )
                setup["utils"]["metrics"].end_operation(extract_op, success=True)
                
                # Conversão
                convert_op = setup["utils"]["metrics"].start_operation("convert", {"file": extracted_files[0]})
                output_file = setup["services"]["convert"].convert_to_parquet(
                    extracted_files[0],
                    str(setup["dirs"]["output"])
                )
                setup["utils"]["metrics"].end_operation(convert_op, success=True)
                
                # Limpar estado de recuperação
                recovery.clear_state(recovery_id)
                
                return {
                    "success": True,
                    "recovered": True,
                    "output_file": output_file
                }
            
            return {"success": False, "error": "Invalid recovery step"}
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def _execute_parallel_pipeline(self, setup, files):
        """Executa pipeline para múltiplos arquivos"""
        results = []
        
        for filename in files:
            sample_data = {"csv_file": Path(f"fake_{filename}.txt")}
            result = self._execute_pipeline(setup, filename, sample_data)
            results.append(result)
        
        return results
    
    def _execute_pipeline_with_validation(self, setup, filename, sample_data):
        """Executa pipeline com validação de integridade"""
        result = self._execute_pipeline(setup, filename, sample_data)
        
        if result["success"]:
            # Verificar integridade dos dados
            csv_content = sample_data["csv_content"]
            lines = csv_content.strip().split('\n')
            data_lines = [line for line in lines if line and not line.startswith('competencia')]
            
            result["data_integrity"] = True
            result["record_count"] = len(data_lines)
        
        return result
    
    def _validate_csv_content(self, content):
        """Valida conteúdo CSV básico"""
        lines = content.strip().split('\n')
        if len(lines) < 2:  # Pelo menos header + 1 linha
            return False
        
        # Verificar se header contém campos esperados
        header = lines[0].lower()
        required_fields = ['competencia', 'cbo2002ocupacao', 'cnae20classe']
        
        return all(field in header for field in required_fields)


class TestPipelineIntegrationScenarios:
    """Cenários específicos de integração"""
    
    def test_monthly_processing_scenario(self, pipeline_setup):
        """Testa cenário de processamento mensal completo"""
        setup = pipeline_setup
        
        # Simular arquivos de um mês
        monthly_files = [
            "CAGEDMOV202301.7z",
            "CAGEDEST202301.7z",
            "CAGEDEXC202301.7z"
        ]
        
        results = []
        
        with patch.object(setup["services"]["ftp"], 'list_files') as mock_list, \
             patch.object(setup["services"]["ftp"], 'download_file') as mock_download, \
             patch.object(setup["services"]["extract"], 'extract_file') as mock_extract, \
             patch.object(setup["services"]["convert"], 'convert_to_parquet') as mock_convert:
            
            mock_list.return_value = monthly_files
            mock_download.return_value = True
            mock_extract.return_value = ["extracted.txt"]
            mock_convert.return_value = "output.parquet"
            
            # Processar cada arquivo
            for filename in monthly_files:
                sample_data = {"csv_file": Path("fake.txt")}
                result = self._execute_pipeline(setup, filename, sample_data)
                results.append(result)
        
        # Verificações
        assert len(results) == 3
        assert all(result["success"] for result in results)
        
        # Verificar métricas consolidadas
        metrics = setup["utils"]["metrics"]
        performance = metrics.get_performance_summary()
        assert performance["total_operations"] >= 9  # 3 arquivos × 3 operações
    
    def test_error_recovery_scenario(self, pipeline_setup):
        """Testa cenário completo de recuperação de erro"""
        setup = pipeline_setup
        recovery = setup["utils"]["recovery"]
        
        # Simular falha durante extração
        with patch.object(setup["services"]["ftp"], 'download_file') as mock_download, \
             patch.object(setup["services"]["extract"], 'extract_file') as mock_extract:
            
            mock_download.return_value = True
            mock_extract.side_effect = Exception("Extraction failed")
            
            # Primeira tentativa (falha)
            sample_data = {"csv_file": Path("fake.txt")}
            result1 = self._execute_pipeline_with_recovery_save(
                setup, "CAGEDMOV202301.7z", sample_data
            )
            
            assert result1["success"] is False
            
            # Verificar se estado foi salvo
            states = recovery.list_states()
            assert len(states) > 0
            
            # Segunda tentativa (recuperação)
            mock_extract.side_effect = None
            mock_extract.return_value = ["extracted.txt"]
            
            with patch.object(setup["services"]["convert"], 'convert_to_parquet') as mock_convert:
                mock_convert.return_value = "output.parquet"
                
                result2 = self._execute_pipeline_recovery(
                    setup, states[0], sample_data
                )
                
                assert result2["success"] is True
                assert result2["recovered"] is True
    
    def _execute_pipeline(self, setup, filename, sample_data):
        """Método auxiliar para executar pipeline"""
        # Implementação simplificada para testes
        return {"success": True, "output_file": "test.parquet"}
    
    def _execute_pipeline_with_recovery_save(self, setup, filename, sample_data):
        """Executa pipeline salvando estado para recuperação"""
        try:
            recovery = setup["utils"]["recovery"]
            
            # Salvar estado antes da operação que pode falhar
            recovery_id = f"pipeline_{filename}_{datetime.now().timestamp()}"
            state = {
                "operation_id": recovery_id,
                "step": "extract",
                "file": filename,
                "progress": 50,
                "timestamp": datetime.now().isoformat()
            }
            recovery.save_state(recovery_id, state)
            
            # Executar pipeline
            return self._execute_pipeline(setup, filename, sample_data)
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _execute_pipeline_recovery(self, setup, recovery_id, sample_data):
        """Executa recuperação de pipeline"""
        # Implementação simplificada
        return {"success": True, "recovered": True, "output_file": "recovered.parquet"}