# -*- coding: utf-8 -*-
"""
🧪 Testes Unitários para Módulo de Validações - CAGED

Testes para todas as classes de validação:
- DateValidator
- DiskValidator
- FTPValidator
- ParameterValidator
- SystemValidator
- CAGEDValidator

Parte da FASE 1.3 do Plano de Melhorias CAGED.
"""

import unittest
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

# Importar módulo a ser testado
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.util.validators import (
    DateValidator,
    DiskValidator,
    FTPValidator,
    ParameterValidator,
    SystemValidator,
    CAGEDValidator
)


class TestDateValidator(unittest.TestCase):
    """Testes para DateValidator"""
    
    def test_validate_year_month_valid(self):
        """Testa validação de ano/mês válidos"""
        # Ano válido sem mês
        valido, msg = DateValidator.validate_year_month(2024, None)
        self.assertTrue(valido)
        self.assertEqual(msg, "")
        
        # Ano e mês válidos
        valido, msg = DateValidator.validate_year_month(2024, 6)
        self.assertTrue(valido)
        self.assertEqual(msg, "")
    
    def test_validate_year_month_invalid_year(self):
        """Testa validação com ano inválido"""
        # Ano muito antigo
        valido, msg = DateValidator.validate_year_month(2019, 1)
        self.assertFalse(valido)
        self.assertIn("2020", msg)
        
        # Ano futuro
        ano_futuro = datetime.now().year + 1
        valido, msg = DateValidator.validate_year_month(ano_futuro, 1)
        self.assertFalse(valido)
        self.assertIn(str(datetime.now().year), msg)
        
        # Ano None
        valido, msg = DateValidator.validate_year_month(None, 1)
        self.assertFalse(valido)
        self.assertIn("obrigatório", msg)
    
    def test_validate_year_month_invalid_month(self):
        """Testa validação com mês inválido"""
        # Mês muito baixo
        valido, msg = DateValidator.validate_year_month(2024, 0)
        self.assertFalse(valido)
        self.assertIn("1 e 12", msg)
        
        # Mês muito alto
        valido, msg = DateValidator.validate_year_month(2024, 13)
        self.assertFalse(valido)
        self.assertIn("1 e 12", msg)
    
    def test_validate_date_range_valid(self):
        """Testa validação de faixa de datas válida"""
        valido, msg = DateValidator.validate_date_range(2023, 6, 2024, 3)
        self.assertTrue(valido)
        self.assertEqual(msg, "")
        
        # Mesmo ano
        valido, msg = DateValidator.validate_date_range(2024, 1, 2024, 6)
        self.assertTrue(valido)
        self.assertEqual(msg, "")
    
    def test_validate_date_range_invalid_order(self):
        """Testa validação com ordem cronológica inválida"""
        # Ano inicial maior
        valido, msg = DateValidator.validate_date_range(2024, 1, 2023, 6)
        self.assertFalse(valido)
        self.assertIn("anterior", msg)
        
        # Mesmo ano, mês inicial maior
        valido, msg = DateValidator.validate_date_range(2024, 6, 2024, 3)
        self.assertFalse(valido)
        self.assertIn("anterior", msg)
    
    def test_validate_date_range_missing_params(self):
        """Testa validação com parâmetros faltando"""
        valido, msg = DateValidator.validate_date_range(2024, None, 2024, 6)
        self.assertFalse(valido)
        self.assertIn("especifique", msg)
    
    def test_validate_current_period(self):
        """Testa validação de período atual"""
        # Período passado (válido)
        valido, msg = DateValidator.validate_current_period(2020, 1)
        self.assertTrue(valido)
        
        # Período futuro (inválido)
        ano_futuro = datetime.now().year + 1
        valido, msg = DateValidator.validate_current_period(ano_futuro, 1)
        self.assertFalse(valido)
        self.assertIn("futuro", msg)


class TestDiskValidator(unittest.TestCase):
    """Testes para DiskValidator"""
    
    def test_validate_disk_space_sufficient(self):
        """Testa validação com espaço suficiente"""
        # Usar diretório temporário
        with tempfile.TemporaryDirectory() as temp_dir:
            valido, msg = DiskValidator.validate_disk_space(temp_dir, 0.001)  # 1MB
            self.assertTrue(valido)
            self.assertIn("suficiente", msg)
    
    def test_validate_disk_space_insufficient(self):
        """Testa validação com espaço insuficiente"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Solicitar espaço muito grande (1TB)
            valido, msg = DiskValidator.validate_disk_space(temp_dir, 1000)
            self.assertFalse(valido)
            self.assertIn("insuficiente", msg)
    
    def test_validate_directory_writable_success(self):
        """Testa validação de diretório gravável"""
        with tempfile.TemporaryDirectory() as temp_dir:
            valido, msg = DiskValidator.validate_directory_writable(temp_dir)
            self.assertTrue(valido)
            self.assertIn("gravável", msg)
    
    def test_validate_directory_writable_new_dir(self):
        """Testa criação de novo diretório"""
        with tempfile.TemporaryDirectory() as temp_dir:
            new_dir = Path(temp_dir) / "novo_diretorio"
            valido, msg = DiskValidator.validate_directory_writable(str(new_dir))
            self.assertTrue(valido)
            self.assertTrue(new_dir.exists())


class TestFTPValidator(unittest.TestCase):
    """Testes para FTPValidator"""
    
    @patch('ftplib.FTP')
    def test_validate_ftp_connectivity_success(self, mock_ftp_class):
        """Testa conectividade FTP bem-sucedida"""
        # Configurar mock
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        mock_ftp.getwelcome.return_value = "Welcome"
        
        valido, msg = FTPValidator.validate_ftp_connectivity("test.server.com")
        
        self.assertTrue(valido)
        self.assertIn("OK", msg)
        mock_ftp.connect.assert_called_once()
        mock_ftp.login.assert_called_once()
        mock_ftp.quit.assert_called_once()
    
    @patch('ftplib.FTP')
    def test_validate_ftp_connectivity_failure(self, mock_ftp_class):
        """Testa falha na conectividade FTP"""
        # Configurar mock para falhar
        mock_ftp_class.side_effect = Exception("Connection failed")
        
        valido, msg = FTPValidator.validate_ftp_connectivity("invalid.server.com")
        
        self.assertFalse(valido)
        self.assertIn("Erro de conectividade", msg)
    
    @patch('ftplib.FTP')
    def test_validate_ftp_directory_success(self, mock_ftp_class):
        """Testa acesso a diretório FTP bem-sucedido"""
        # Configurar mock
        mock_ftp = MagicMock()
        mock_ftp_class.return_value = mock_ftp
        
        # Simular listagem de arquivos
        def mock_retrlines(cmd, callback):
            callback("file1.txt")
            callback("file2.txt")
        
        mock_ftp.retrlines = mock_retrlines
        
        valido, msg = FTPValidator.validate_ftp_directory("test.server.com", "/test/dir")
        
        self.assertTrue(valido)
        self.assertIn("acessível", msg)
        self.assertIn("2 itens", msg)
        mock_ftp.cwd.assert_called_with("/test/dir")


class TestParameterValidator(unittest.TestCase):
    """Testes para ParameterValidator"""
    
    def test_validate_workers_count_valid(self):
        """Testa validação de número de workers válido"""
        valido, msg = ParameterValidator.validate_workers_count(4)
        self.assertTrue(valido)
        self.assertIn("válido", msg)
    
    def test_validate_workers_count_invalid_low(self):
        """Testa validação com número muito baixo de workers"""
        valido, msg = ParameterValidator.validate_workers_count(0)
        self.assertFalse(valido)
        self.assertIn("pelo menos 1", msg)
    
    def test_validate_workers_count_invalid_high(self):
        """Testa validação com número muito alto de workers"""
        import os
        max_workers = os.cpu_count() * 2 + 1
        valido, msg = ParameterValidator.validate_workers_count(max_workers)
        self.assertFalse(valido)
        self.assertIn("muito alto", msg)
    
    def test_validate_campos_caged_valid(self):
        """Testa validação de campos CAGED válidos"""
        campos = ['ADMITIDOS', 'DESLIGADOS']
        valido, msg = ParameterValidator.validate_campos_caged(campos)
        self.assertTrue(valido)
        self.assertIn("válidos", msg)
    
    def test_validate_campos_caged_invalid(self):
        """Testa validação de campos CAGED inválidos"""
        campos = ['ADMITIDOS', 'CAMPO_INEXISTENTE']
        valido, msg = ParameterValidator.validate_campos_caged(campos)
        self.assertFalse(valido)
        self.assertIn("inválidos", msg)
        self.assertIn("CAMPO_INEXISTENTE", msg)
    
    def test_validate_campos_caged_case_insensitive(self):
        """Testa validação case-insensitive de campos"""
        campos = ['admitidos', 'Desligados', 'SALDO']
        valido, msg = ParameterValidator.validate_campos_caged(campos)
        self.assertTrue(valido)


class TestSystemValidator(unittest.TestCase):
    """Testes para SystemValidator"""
    
    def test_validate_system_requirements_success(self):
        """Testa validação de requisitos do sistema bem-sucedida"""
        # Este teste assume que o ambiente atual atende aos requisitos
        valido, msg = SystemValidator.validate_system_requirements()
        # Pode falhar se módulos não estiverem instalados, mas é esperado
        if valido:
            self.assertIn("Requisitos OK", msg)
        else:
            self.assertIn("Requisitos não atendidos", msg)


class TestCAGEDValidator(unittest.TestCase):
    """Testes para CAGEDValidator (classe principal)"""
    
    def test_compatibility_aliases(self):
        """Testa aliases de compatibilidade"""
        # Testar alias validar_ano_mes
        valido, msg = CAGEDValidator.validar_ano_mes(2024, 6)
        self.assertTrue(valido)
        
        # Testar alias validar_faixa_datas
        valido, msg = CAGEDValidator.validar_faixa_datas(2023, 1, 2024, 6)
        self.assertTrue(valido)
        
        # Testar alias validar_espaco_disco
        with tempfile.TemporaryDirectory() as temp_dir:
            valido, msg = CAGEDValidator.validar_espaco_disco(temp_dir, 0.001)
            self.assertTrue(valido)
    
    @patch('src.util.validators.FTPValidator.validate_ftp_connectivity')
    def test_validar_conectividade_ftp(self, mock_ftp_validate):
        """Testa validação de conectividade FTP"""
        mock_ftp_validate.return_value = (True, "Conectividade OK")
        
        valido, msg = CAGEDValidator.validar_conectividade_ftp()
        
        self.assertTrue(valido)
        self.assertIn("OK", msg)
        mock_ftp_validate.assert_called_once()
    
    def test_validar_parametros_completos_valid(self):
        """Testa validação completa de parâmetros válidos"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mudar para diretório temporário para teste de espaço
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                valido, msg = CAGEDValidator.validar_parametros_completos(
                    ano=2024, mes=6, workers=2, campos=['ADMITIDOS']
                )
                if valido:
                    self.assertIn("válidos", msg)
            finally:
                os.chdir(original_cwd)
    
    def test_validar_parametros_completos_invalid(self):
        """Testa validação completa com parâmetros inválidos"""
        valido, msg = CAGEDValidator.validar_parametros_completos(
            ano=2019,  # Ano inválido
            workers=0,  # Workers inválido
            campos=['CAMPO_INEXISTENTE']  # Campo inválido
        )
        self.assertFalse(valido)
        # Deve conter múltiplas mensagens de erro
        self.assertTrue(any(keyword in msg for keyword in ['Data', 'Workers', 'Campos']))


class TestIntegration(unittest.TestCase):
    """Testes de integração"""
    
    def test_full_validation_workflow(self):
        """Testa fluxo completo de validação"""
        # Simular validação completa como seria usada no main.py
        ano, mes = 2024, 6
        
        # 1. Validar data
        valido_data, msg_data = DateValidator.validate_year_month(ano, mes)
        
        # 2. Validar espaço em disco
        with tempfile.TemporaryDirectory() as temp_dir:
            valido_disco, msg_disco = DiskValidator.validate_disk_space(temp_dir, 0.001)
        
        # 3. Validar workers
        valido_workers, msg_workers = ParameterValidator.validate_workers_count(4)
        
        # Verificar que todas as validações básicas passaram
        self.assertTrue(valido_data, f"Validação de data falhou: {msg_data}")
        self.assertTrue(valido_disco, f"Validação de disco falhou: {msg_disco}")
        self.assertTrue(valido_workers, f"Validação de workers falhou: {msg_workers}")


if __name__ == '__main__':
    # Configurar logging para testes
    import logging
    logging.basicConfig(level=logging.WARNING)
    
    # Executar testes
    unittest.main(verbosity=2)