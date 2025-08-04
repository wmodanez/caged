#!/usr/bin/env python3
"""
Sistema de Logging Centralizado para o CAGED
Implementação do Item 1.2 do Plano de Melhorias

Funcionalidades:
- Configuração centralizada de logs
- Rotação automática de arquivos de log
- Suporte a logs estruturados (JSON)
- Configuração de diferentes níveis por módulo
- Formatação personalizada com emojis (opcional)
"""

import logging
import logging.handlers
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Union, List, Any


class LoggerConfig:
    """
    Classe para configuração centralizada de logging no projeto CAGED
    """
    
    # Configurações padrão
    DEFAULT_LOG_LEVEL = "INFO"
    DEFAULT_LOG_FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    DEFAULT_LOG_DIR = "logs"
    DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
    DEFAULT_BACKUP_COUNT = 5
    
    # Handler global compartilhado
    _shared_file_handler = None
    _shared_json_handler = None
    _log_file_path = None
    
    # Mapeamento de níveis de log
    LOG_LEVELS = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
        # Aliases
        "warn": logging.WARNING,
        "err": logging.ERROR,
    }
    
    # Emojis para diferentes níveis de log
    EMOJIS = {
        "debug": "🔍",
        "info": "ℹ️",
        "warning": "⚠️",
        "error": "❌",
        "critical": "🚨",
        # Categorias específicas
        "success": "✅",
        "download": "📥",
        "extract": "📦",
        "convert": "🔄",
        "validate": "✓",
        "cache": "💾",
        "performance": "⚡",
        "metric": "📊",
    }
    
    @classmethod
    def setup_logger(
        cls,
        name: str = "caged",
        level: str = DEFAULT_LOG_LEVEL,
        enable_file: bool = True,
        enable_console: bool = True,
        log_dir: Optional[Union[str, Path]] = None,
        use_emojis: bool = True,
        enable_json: bool = False,
        module_levels: Optional[Dict[str, str]] = None,
    ) -> logging.Logger:
        """
        Configura e retorna um logger com as configurações especificadas
        
        Args:
            name: Nome do logger
            level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            enable_file: Se deve habilitar log em arquivo
            enable_console: Se deve habilitar log no console
            log_dir: Diretório para armazenar logs (padrão: ./logs)
            use_emojis: Se deve usar emojis nas mensagens de log
            enable_json: Se deve formatar logs como JSON para análise
            module_levels: Dicionário com níveis específicos por módulo
            
        Returns:
            Logger configurado
        """
        # Obter o logger pelo nome
        logger = logging.getLogger(name)
        
        # Converter nível de log para maiúsculas e obter valor numérico
        level = level.lower()
        numeric_level = cls.LOG_LEVELS.get(level, logging.INFO)
        logger.setLevel(numeric_level)
        
        # Evitar duplicação de handlers
        if logger.handlers:
            return logger
        
        # Criar diretório de logs se necessário
        if enable_file:
            log_directory = Path(log_dir or cls.DEFAULT_LOG_DIR)
            log_directory.mkdir(parents=True, exist_ok=True)
        
        # Definir formatador padrão
        formatter = logging.Formatter(
            cls.DEFAULT_LOG_FORMAT,
            datefmt=cls.DEFAULT_DATE_FORMAT
        )
        
        # Adicionar handler para arquivo com rotação (centralizado)
        if enable_file:
            # Criar handler compartilhado se ainda não existir
            if cls._shared_file_handler is None:
                # Usar um único arquivo centralizado para todos os módulos
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]  # Remove últimos 3 dígitos dos microssegundos
                
                # Nome do arquivo centralizado baseado no logger raiz
                root_name = name.split('.')[0] if '.' in name else name
                cls._log_file_path = log_directory / f"{root_name}_{timestamp}.log"
                
                # Criar handler compartilhado
                cls._shared_file_handler = logging.handlers.RotatingFileHandler(
                    cls._log_file_path,
                    maxBytes=cls.DEFAULT_MAX_BYTES,
                    backupCount=cls.DEFAULT_BACKUP_COUNT,
                    encoding='utf-8'
                )
                cls._shared_file_handler.setFormatter(formatter)
                
                # Criar handler JSON compartilhado se solicitado
                if enable_json:
                    json_log_file = log_directory / f"{root_name}_{timestamp}_json.log"
                    cls._shared_json_handler = logging.handlers.RotatingFileHandler(
                        json_log_file,
                        maxBytes=cls.DEFAULT_MAX_BYTES,
                        backupCount=cls.DEFAULT_BACKUP_COUNT,
                        encoding='utf-8'
                    )
                    cls._shared_json_handler.setFormatter(cls.JsonFormatter())
            
            # Adicionar o handler compartilhado ao logger atual
            logger.addHandler(cls._shared_file_handler)
            
            # Adicionar handler JSON se existir e foi solicitado
            if enable_json and cls._shared_json_handler:
                logger.addHandler(cls._shared_json_handler)
        
        # Adicionar handler para console com encoding UTF-8
        if enable_console:
            # Verificar se já existe um console handler para evitar duplicação
            has_console_handler = any(
                isinstance(handler, logging.StreamHandler) and 
                hasattr(handler.stream, 'name') and 
                handler.stream.name in ['<stdout>', '<stderr>']
                for handler in logger.handlers
            )
            
            if not has_console_handler:
                import sys
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setFormatter(formatter)
                
                # Configurar encoding UTF-8 para o console no Windows
                if hasattr(console_handler.stream, 'reconfigure'):
                    try:
                        console_handler.stream.reconfigure(encoding='utf-8')
                    except Exception:
                        pass
                
                logger.addHandler(console_handler)
        
        # Configurar níveis específicos por módulo
        if module_levels:
            for module_name, module_level in module_levels.items():
                module_logger = logging.getLogger(f"{name}.{module_name}")
                module_level_value = cls.LOG_LEVELS.get(module_level.lower(), logging.INFO)
                module_logger.setLevel(module_level_value)
        
        # Registrar inicialização do logger
        logger.info(f"Logger '{name}' configurado com nível {level.upper()}")
        
        return logger
    
    @classmethod
    def get_emoji(cls, category: str) -> str:
        """
        Retorna o emoji para a categoria especificada
        
        Args:
            category: Categoria do log (debug, info, warning, error, critical, etc.)
            
        Returns:
            Emoji correspondente ou string vazia
        """
        return cls.EMOJIS.get(category.lower(), "")
    
    @classmethod
    def log_with_emoji(cls, logger: logging.Logger, level: str, message: str, category: Optional[str] = None, **kwargs):
        """
        Registra uma mensagem com emoji
        
        Args:
            logger: Logger para registrar a mensagem
            level: Nível do log (debug, info, warning, error, critical)
            message: Mensagem a ser registrada
            category: Categoria específica para emoji (opcional)
            **kwargs: Argumentos adicionais para o logger
        """
        emoji = cls.get_emoji(category or level)
        log_method = getattr(logger, level.lower())
        
        if emoji:
            log_method(f"{emoji} {message}", **kwargs)
        else:
            log_method(message, **kwargs)
    
    class JsonFormatter(logging.Formatter):
        """
        Formatador para logs em formato JSON estruturado
        """
        def format(self, record):
            log_data = {
                "timestamp": self.formatTime(record, self.datefmt or "%Y-%m-%d %H:%M:%S"),
                "name": record.name,
                "level": record.levelname,
                "message": record.getMessage(),
                "module": record.module,
                "lineno": record.lineno,
            }
            
            # Adicionar exceção se existir
            if record.exc_info:
                log_data["exception"] = self.formatException(record.exc_info)
            
            # Adicionar dados extras se existirem
            if hasattr(record, "data") and isinstance(record.data, dict):
                log_data.update(record.data)
            
            return json.dumps(log_data, ensure_ascii=False)


# Função auxiliar para uso rápido
def setup_logger(
    name: str = "caged",
    level: str = "INFO",
    enable_file: bool = True,
    enable_console: bool = True,
    use_emojis: bool = True,
    enable_json: bool = False,
    module_levels: Optional[Dict[str, str]] = None,
) -> logging.Logger:
    """
    Função auxiliar para configurar logger rapidamente
    
    Args:
        name: Nome do logger
        level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        enable_file: Se deve habilitar log em arquivo
        enable_console: Se deve habilitar log no console
        use_emojis: Se deve usar emojis nas mensagens de log
        enable_json: Se deve formatar logs como JSON para análise
        module_levels: Dicionário com níveis específicos por módulo
        
    Returns:
        Logger configurado
    """
    return LoggerConfig.setup_logger(
        name=name,
        level=level,
        enable_file=enable_file,
        enable_console=enable_console,
        use_emojis=use_emojis,
        enable_json=enable_json,
        module_levels=module_levels,
    )


# Função para log estruturado
def log_structured(
    logger: logging.Logger,
    level: str,
    message: str,
    data: Optional[Dict[str, Any]] = None,
    category: Optional[str] = None,
):
    """
    Registra uma mensagem com dados estruturados
    
    Args:
        logger: Logger para registrar a mensagem
        level: Nível do log (debug, info, warning, error, critical)
        message: Mensagem a ser registrada
        data: Dados estruturados adicionais
        category: Categoria específica para emoji
    """
    # Criar cópia do record para adicionar dados estruturados
    extra = {"data": data or {}}
    
    # Registrar com emoji se categoria for fornecida
    if category:
        LoggerConfig.log_with_emoji(logger, level, message, category, extra=extra)
    else:
        log_method = getattr(logger, level.lower())
        log_method(message, extra=extra)