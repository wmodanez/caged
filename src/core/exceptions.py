#!/usr/bin/env python3
"""
Exceções Customizadas do Sistema CAGED
Implementação do Item 1.4 do Plano de Melhorias

Este módulo define exceções específicas para diferentes componentes do sistema.
"""

from typing import Optional, Dict, Any


class CAGEDException(Exception):
    """Exceção base para o sistema CAGED"""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class ValidationError(CAGEDException):
    """Erro de validação de dados ou parâmetros"""
    pass


class FileValidationError(ValidationError):
    """Erro de validação de arquivos"""
    pass


class FTPError(CAGEDException):
    """Erro relacionado a operações FTP"""
    pass


class FTPConnectionError(FTPError):
    """Erro de conexão FTP"""
    pass


class FTPRetryError(FTPError):
    """Erro após múltiplas tentativas FTP"""
    pass


class ExtractionError(CAGEDException):
    """Erro durante extração de arquivos"""
    pass


class CompressionError(ExtractionError):
    """Erro relacionado a compressão/descompressão"""
    pass


class ConversionError(CAGEDException):
    """Erro durante conversão de dados"""
    pass


class ParquetError(ConversionError):
    """Erro específico de conversão para Parquet"""
    pass


class ConfigurationError(CAGEDException):
    """Erro de configuração do sistema"""
    pass


class CacheError(CAGEDException):
    """Erro relacionado ao sistema de cache"""
    pass


class PipelineError(CAGEDException):
    """Erro no pipeline de processamento"""
    pass


class ResourceError(CAGEDException):
    """Erro relacionado a recursos do sistema (memória, disco, etc.)"""
    pass


class DiskSpaceError(ResourceError):
    """Erro de espaço insuficiente em disco"""
    pass


class MemoryError(ResourceError):
    """Erro de memória insuficiente"""
    pass