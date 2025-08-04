# -*- coding: utf-8 -*-
"""
🔍 Módulo de Validações Centralizadas - CAGED

Este módulo centraliza todas as validações do sistema CAGED, incluindo:
- Validações de data (ano/mês, faixas)
- Validações de espaço em disco
- Validações de conectividade FTP
- Validações de parâmetros de entrada

Parte da FASE 1.3 do Plano de Melhorias CAGED.
"""

import ftplib
import shutil
from datetime import datetime
from typing import Tuple, Optional, List
from pathlib import Path

from .logger_config import setup_logger

# Configurar logger
logger = setup_logger("validators", "INFO")


class DateValidator:
    """
    Validador para datas e períodos
    """
    
    @staticmethod
    def validate_year_month(ano: int, mes: Optional[int] = None) -> Tuple[bool, str]:
        """
        Valida ano e mês
        
        Args:
            ano: Ano a ser validado
            mes: Mês a ser validado (opcional)
            
        Returns:
            Tuple[bool, str]: (válido, mensagem de erro)
        """
        if ano is None:
            return False, "Ano é obrigatório"
            
        # Validar ano (CAGED disponível a partir de 2020)
        if ano < 2020 or ano > datetime.now().year:
            return False, f"Ano deve estar entre 2020 e {datetime.now().year}"
            
        # Validar mês se fornecido
        if mes is not None and (mes < 1 or mes > 12):
            return False, "Mês deve estar entre 1 e 12"
            
        return True, ""
    
    @staticmethod
    def validate_date_range(ano_inicio: int, mes_inicio: int, 
                          ano_fim: int, mes_fim: int) -> Tuple[bool, str]:
        """
        Valida faixa de datas
        
        Args:
            ano_inicio: Ano inicial
            mes_inicio: Mês inicial
            ano_fim: Ano final
            mes_fim: Mês final
            
        Returns:
            Tuple[bool, str]: (válido, mensagem de erro)
        """
        # Verificar se todos os parâmetros foram fornecidos
        if any(x is None for x in [ano_inicio, mes_inicio, ano_fim, mes_fim]):
            return False, "Para faixa de datas, especifique --ano-inicio, --mes-inicio, --ano-fim e --mes-fim"
        
        # Validar anos individuais
        for ano in [ano_inicio, ano_fim]:
            valido, msg = DateValidator.validate_year_month(ano, None)
            if not valido:
                return False, msg
        
        # Validar meses individuais
        for mes in [mes_inicio, mes_fim]:
            if mes < 1 or mes > 12:
                return False, "Meses devem estar entre 1 e 12"
        
        # Validar ordem cronológica
        if ano_inicio > ano_fim or (ano_inicio == ano_fim and mes_inicio > mes_fim):
            return False, "Data inicial deve ser anterior à data final"
        
        return True, ""
    
    @staticmethod
    def validate_current_period(ano: int, mes: int) -> Tuple[bool, str]:
        """
        Valida se o período não é futuro
        
        Args:
            ano: Ano
            mes: Mês
            
        Returns:
            Tuple[bool, str]: (válido, mensagem de erro)
        """
        agora = datetime.now()
        
        if ano > agora.year or (ano == agora.year and mes > agora.month):
            return False, f"Período {mes:02d}/{ano} é futuro. Dados ainda não disponíveis."
            
        return True, ""


class DiskValidator:
    """
    Validador para espaço em disco e sistema de arquivos
    """
    
    @staticmethod
    def validate_disk_space(diretorio: str = ".", min_gb: float = 1.0) -> Tuple[bool, str]:
        """
        Valida espaço disponível em disco
        
        Args:
            diretorio: Diretório para verificar espaço
            min_gb: Espaço mínimo necessário em GB
            
        Returns:
            Tuple[bool, str]: (válido, mensagem de erro)
        """
        try:
            total, used, free = shutil.disk_usage(diretorio)
            free_gb = free / (1024**3)
            total_gb = total / (1024**3)
            used_gb = used / (1024**3)
            
            if free_gb < min_gb:
                return False, (
                    f"Espaço insuficiente. "
                    f"Disponível: {free_gb:.1f}GB, "
                    f"Necessário: {min_gb}GB, "
                    f"Total: {total_gb:.1f}GB, "
                    f"Usado: {used_gb:.1f}GB"
                )
                
            return True, f"Espaço suficiente: {free_gb:.1f}GB disponível"
            
        except Exception as e:
            return False, f"Erro ao verificar espaço em disco: {e}"
    
    @staticmethod
    def validate_directory_writable(diretorio: str) -> Tuple[bool, str]:
        """
        Valida se o diretório é gravável
        
        Args:
            diretorio: Caminho do diretório
            
        Returns:
            Tuple[bool, str]: (válido, mensagem de erro)
        """
        try:
            path = Path(diretorio)
            
            # Criar diretório se não existir
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
            
            # Testar escrita
            test_file = path / ".test_write"
            test_file.write_text("test")
            test_file.unlink()
            
            return True, f"Diretório {diretorio} é gravável"
            
        except Exception as e:
            return False, f"Diretório {diretorio} não é gravável: {e}"


class FTPValidator:
    """
    Validador para conectividade FTP
    """
    
    @staticmethod
    def validate_ftp_connectivity(servidor: str, timeout: int = 30) -> Tuple[bool, str]:
        """
        Valida conectividade com servidor FTP
        
        Args:
            servidor: Endereço do servidor FTP
            timeout: Timeout em segundos
            
        Returns:
            Tuple[bool, str]: (válido, mensagem de erro)
        """
        try:
            logger.debug(f"🔍 Testando conectividade FTP: {servidor}")
            
            # Tentar conexão
            ftp = ftplib.FTP(timeout=timeout)
            ftp.connect(servidor)
            ftp.login()  # Login anônimo
            
            # Testar comando básico
            welcome = ftp.getwelcome()
            ftp.pwd()
            
            ftp.quit()
            
            return True, f"Conectividade FTP OK: {servidor}"
            
        except ftplib.error_perm as e:
            return False, f"Erro de permissão FTP: {e}"
        except ftplib.error_temp as e:
            return False, f"Erro temporário FTP: {e}"
        except Exception as e:
            return False, f"Erro de conectividade FTP: {e}"
    
    @staticmethod
    def validate_ftp_directory(servidor: str, diretorio: str, timeout: int = 30) -> Tuple[bool, str]:
        """
        Valida se um diretório específico existe no servidor FTP
        
        Args:
            servidor: Endereço do servidor FTP
            diretorio: Caminho do diretório
            timeout: Timeout em segundos
            
        Returns:
            Tuple[bool, str]: (válido, mensagem de erro)
        """
        try:
            logger.debug(f"🔍 Testando diretório FTP: {servidor}{diretorio}")
            
            ftp = ftplib.FTP(timeout=timeout)
            ftp.connect(servidor)
            ftp.login()
            
            # Tentar navegar para o diretório
            ftp.cwd(diretorio)
            
            # Listar conteúdo para confirmar acesso
            arquivos = []
            ftp.retrlines('LIST', arquivos.append)
            
            ftp.quit()
            
            return True, f"Diretório FTP acessível: {diretorio} ({len(arquivos)} itens)"
            
        except ftplib.error_perm as e:
            return False, f"Diretório FTP inacessível: {diretorio} - {e}"
        except Exception as e:
            return False, f"Erro ao acessar diretório FTP: {e}"


class ParameterValidator:
    """
    Validador para parâmetros de entrada
    """
    
    @staticmethod
    def validate_workers_count(workers: int) -> Tuple[bool, str]:
        """
        Valida número de workers para processamento paralelo
        
        Args:
            workers: Número de workers
            
        Returns:
            Tuple[bool, str]: (válido, mensagem de erro)
        """
        import os
        
        if workers < 1:
            return False, "Número de workers deve ser pelo menos 1"
            
        max_workers = os.cpu_count() * 2  # Máximo recomendado
        if workers > max_workers:
            return False, f"Número de workers muito alto. Máximo recomendado: {max_workers}"
            
        return True, f"Número de workers válido: {workers}"
    
    @staticmethod
    def validate_campos_caged(campos: List[str]) -> Tuple[bool, str]:
        """
        Valida campos específicos do CAGED
        
        Args:
            campos: Lista de campos a validar
            
        Returns:
            Tuple[bool, str]: (válido, mensagem de erro)
        """
        campos_validos = {
            'ADMITIDOS', 'DESLIGADOS', 'SALDO', 
            'MOVIMENTACAO', 'INDICADORES', 'TODOS'
        }
        
        campos_upper = [c.upper() for c in campos]
        campos_invalidos = [c for c in campos_upper if c not in campos_validos]
        
        if campos_invalidos:
            return False, (
                f"Campos inválidos: {', '.join(campos_invalidos)}. "
                f"Campos válidos: {', '.join(sorted(campos_validos))}"
            )
            
        return True, f"Campos válidos: {', '.join(campos_upper)}"


class SystemValidator:
    """
    Validador para requisitos do sistema
    """
    
    @staticmethod
    def validate_system_requirements() -> Tuple[bool, str]:
        """
        Valida requisitos básicos do sistema
        
        Returns:
            Tuple[bool, str]: (válido, mensagem de erro)
        """
        import sys
        import platform
        
        issues = []
        
        # Verificar versão do Python
        if sys.version_info < (3, 8):
            issues.append(f"Python 3.8+ necessário. Atual: {sys.version}")
        
        # Verificar módulos essenciais
        required_modules = ['polars', 'click', 'py7zr']
        for module in required_modules:
            try:
                __import__(module)
            except ImportError:
                issues.append(f"Módulo necessário não encontrado: {module}")
        
        if issues:
            return False, "Requisitos não atendidos: " + "; ".join(issues)
            
        return True, f"Requisitos OK - Python {sys.version_info.major}.{sys.version_info.minor} em {platform.system()}"


# ============================================================================
# CLASSE PRINCIPAL DE VALIDAÇÃO
# ============================================================================

class CAGEDValidator:
    """
    Classe principal que agrupa todos os validadores
    Mantém compatibilidade com ValidadorCaged do main.py
    """
    
    # Aliases para compatibilidade
    @staticmethod
    def validar_ano_mes(ano: int, mes: Optional[int] = None) -> Tuple[bool, str]:
        """Alias para DateValidator.validate_year_month"""
        return DateValidator.validate_year_month(ano, mes)
    
    @staticmethod
    def validar_faixa_datas(ano_inicio: int, mes_inicio: int, 
                           ano_fim: int, mes_fim: int) -> Tuple[bool, str]:
        """Alias para DateValidator.validate_date_range"""
        return DateValidator.validate_date_range(ano_inicio, mes_inicio, ano_fim, mes_fim)
    
    @staticmethod
    def validar_espaco_disco(diretorio: str = ".", min_gb: float = 1.0) -> Tuple[bool, str]:
        """Alias para DiskValidator.validate_disk_space"""
        return DiskValidator.validate_disk_space(diretorio, min_gb)
    
    # Novos métodos
    @staticmethod
    def validar_conectividade_ftp(servidor: str = "ftp.mtps.gov.br", timeout: int = 30) -> Tuple[bool, str]:
        """Valida conectividade com servidor FTP do CAGED"""
        return FTPValidator.validate_ftp_connectivity(servidor, timeout)
    
    @staticmethod
    def validar_diretorio_ftp(servidor: str = "ftp.mtps.gov.br", 
                             diretorio: str = "/pdet/microdados/NOVO CAGED",
                             timeout: int = 30) -> Tuple[bool, str]:
        """Valida acesso ao diretório específico do CAGED"""
        return FTPValidator.validate_ftp_directory(servidor, diretorio, timeout)
    
    @staticmethod
    def validar_sistema() -> Tuple[bool, str]:
        """Valida requisitos básicos do sistema"""
        return SystemValidator.validate_system_requirements()
    
    @staticmethod
    def validar_parametros_completos(ano: Optional[int] = None, 
                                   mes: Optional[int] = None,
                                   workers: int = 4,
                                   campos: Optional[List[str]] = None) -> Tuple[bool, str]:
        """
        Validação completa de parâmetros de entrada
        
        Args:
            ano: Ano dos dados
            mes: Mês dos dados
            workers: Número de workers
            campos: Lista de campos específicos
            
        Returns:
            Tuple[bool, str]: (válido, mensagem de erro)
        """
        validacoes = []
        
        # Validar data se fornecida
        if ano is not None:
            valido, msg = DateValidator.validate_year_month(ano, mes)
            if not valido:
                validacoes.append(f"Data: {msg}")
        
        # Validar workers
        valido, msg = ParameterValidator.validate_workers_count(workers)
        if not valido:
            validacoes.append(f"Workers: {msg}")
        
        # Validar campos se fornecidos
        if campos:
            valido, msg = ParameterValidator.validate_campos_caged(campos)
            if not valido:
                validacoes.append(f"Campos: {msg}")
        
        # Validar espaço em disco
        valido, msg = DiskValidator.validate_disk_space(min_gb=2.0)
        if not valido:
            validacoes.append(f"Disco: {msg}")
        
        if validacoes:
            return False, "; ".join(validacoes)
            
        return True, "Todos os parâmetros são válidos"


# Alias para compatibilidade com código existente
ValidadorCaged = CAGEDValidator