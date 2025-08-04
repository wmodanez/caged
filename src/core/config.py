#!/usr/bin/env python3
"""
Sistema de Configuração do CAGED
Implementação do Item 1.4 do Plano de Melhorias

Este módulo gerencia configurações centralizadas do sistema.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, Union
from dataclasses import dataclass, field

from .exceptions import ConfigurationError


@dataclass
class FTPConfig:
    """Configurações FTP"""
    server: str = "ftp.mtps.gov.br"
    directory: str = "/pdet/microdados/NOVO CAGED"
    timeout: int = 30
    max_retries: int = 3
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass
class CacheConfig:
    """Configurações de Cache"""
    enabled: bool = True
    directory: str = "cache"
    max_size_gb: int = 10
    expiry_days: int = 30
    cleanup_on_startup: bool = False


@dataclass
class ProcessingConfig:
    """Configurações de Processamento"""
    max_workers: int = 4
    chunk_size: int = 1000
    memory_limit_gb: int = 8
    enable_parallel: bool = True
    retry_attempts: int = 3


@dataclass
class OutputConfig:
    """Configurações de Saída"""
    format: str = "parquet"
    compression: str = "snappy"
    directory: str = "output"
    create_subdirs: bool = True
    overwrite_existing: bool = False


@dataclass
class LoggingConfig:
    """Configurações de Logging"""
    level: str = "INFO"
    enable_file: bool = True
    enable_console: bool = True
    directory: str = "logs"
    use_emojis: bool = True
    enable_json: bool = False
    max_file_size_mb: int = 10
    backup_count: int = 5


@dataclass
class CAGEDConfig:
    """Configuração principal do sistema CAGED"""
    ftp: FTPConfig = field(default_factory=FTPConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    
    # Configurações gerais
    project_root: Optional[Path] = None
    temp_directory: str = "temp"
    debug_mode: bool = False
    profile: str = "default"


class ConfigManager:
    """Gerenciador de configurações do sistema"""
    
    DEFAULT_CONFIG_FILE = "config.yaml"
    ENV_PREFIX = "CAGED_"
    
    def __init__(self, config_file: Optional[Union[str, Path]] = None):
        self.config_file = Path(config_file) if config_file else Path(self.DEFAULT_CONFIG_FILE)
        self._config: Optional[CAGEDConfig] = None
    
    def load_config(self, profile: str = "default") -> CAGEDConfig:
        """Carrega configuração do arquivo e variáveis de ambiente"""
        try:
            # Carregar configuração padrão
            config = CAGEDConfig()
            config.profile = profile
            
            # Carregar do arquivo se existir
            if self.config_file.exists():
                config = self._load_from_file(config, profile)
            
            # Sobrescrever com variáveis de ambiente
            config = self._load_from_env(config)
            
            # Definir project_root se não definido
            if config.project_root is None:
                config.project_root = Path.cwd()
            
            # Validar configuração
            self._validate_config(config)
            
            self._config = config
            return config
            
        except Exception as e:
            raise ConfigurationError(f"Erro ao carregar configuração: {e}")
    
    def _load_from_file(self, config: CAGEDConfig, profile: str) -> CAGEDConfig:
        """Carrega configuração do arquivo YAML"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            # Buscar configuração do profile específico
            if 'profiles' in data and profile in data['profiles']:
                profile_data = data['profiles'][profile]
            else:
                profile_data = data.get('caged', {})
            
            # Atualizar configuração com dados do arquivo
            self._update_config_from_dict(config, profile_data)
            
            return config
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Erro ao parsear arquivo YAML: {e}")
        except FileNotFoundError:
            # Arquivo não existe, usar configuração padrão
            return config
    
    def _load_from_env(self, config: CAGEDConfig) -> CAGEDConfig:
        """Carrega configuração de variáveis de ambiente"""
        env_mappings = {
            f"{self.ENV_PREFIX}FTP_SERVER": ("ftp", "server"),
            f"{self.ENV_PREFIX}FTP_DIRECTORY": ("ftp", "directory"),
            f"{self.ENV_PREFIX}FTP_TIMEOUT": ("ftp", "timeout"),
            f"{self.ENV_PREFIX}CACHE_ENABLED": ("cache", "enabled"),
            f"{self.ENV_PREFIX}CACHE_DIRECTORY": ("cache", "directory"),
            f"{self.ENV_PREFIX}MAX_WORKERS": ("processing", "max_workers"),
            f"{self.ENV_PREFIX}OUTPUT_FORMAT": ("output", "format"),
            f"{self.ENV_PREFIX}LOG_LEVEL": ("logging", "level"),
            f"{self.ENV_PREFIX}DEBUG_MODE": ("debug_mode",),
        }
        
        for env_var, config_path in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                self._set_config_value(config, config_path, value)
        
        return config
    
    def _update_config_from_dict(self, config: CAGEDConfig, data: Dict[str, Any]):
        """Atualiza configuração com dados de dicionário"""
        for section, values in data.items():
            if hasattr(config, section) and isinstance(values, dict):
                section_config = getattr(config, section)
                for key, value in values.items():
                    if hasattr(section_config, key):
                        setattr(section_config, key, value)
            elif hasattr(config, section):
                setattr(config, section, values)
    
    def _set_config_value(self, config: CAGEDConfig, path: tuple, value: str):
        """Define valor de configuração usando caminho"""
        try:
            if len(path) == 1:
                # Configuração de nível raiz
                attr_name = path[0]
                if hasattr(config, attr_name):
                    # Converter tipo se necessário
                    current_value = getattr(config, attr_name)
                    if isinstance(current_value, bool):
                        value = value.lower() in ('true', '1', 'yes', 'on')
                    elif isinstance(current_value, int):
                        value = int(value)
                    elif isinstance(current_value, float):
                        value = float(value)
                    
                    setattr(config, attr_name, value)
            
            elif len(path) == 2:
                # Configuração de seção
                section_name, attr_name = path
                if hasattr(config, section_name):
                    section = getattr(config, section_name)
                    if hasattr(section, attr_name):
                        # Converter tipo se necessário
                        current_value = getattr(section, attr_name)
                        if isinstance(current_value, bool):
                            value = value.lower() in ('true', '1', 'yes', 'on')
                        elif isinstance(current_value, int):
                            value = int(value)
                        elif isinstance(current_value, float):
                            value = float(value)
                        
                        setattr(section, attr_name, value)
        
        except (ValueError, TypeError) as e:
            raise ConfigurationError(f"Erro ao converter valor de configuração: {e}")
    
    def _validate_config(self, config: CAGEDConfig):
        """Valida configuração carregada"""
        # Validar FTP
        if not config.ftp.server:
            raise ConfigurationError("Servidor FTP não pode estar vazio")
        
        if config.ftp.timeout <= 0:
            raise ConfigurationError("Timeout FTP deve ser positivo")
        
        # Validar processamento
        if config.processing.max_workers <= 0:
            raise ConfigurationError("Número de workers deve ser positivo")
        
        if config.processing.memory_limit_gb <= 0:
            raise ConfigurationError("Limite de memória deve ser positivo")
        
        # Validar cache
        if config.cache.max_size_gb <= 0:
            raise ConfigurationError("Tamanho máximo do cache deve ser positivo")
        
        # Validar output
        if config.output.format not in ['parquet', 'csv', 'json']:
            raise ConfigurationError(f"Formato de saída inválido: {config.output.format}")
    
    def save_config(self, config: CAGEDConfig, profile: str = "default"):
        """Salva configuração no arquivo"""
        try:
            # Carregar configuração existente se houver
            existing_data = {}
            if self.config_file.exists():
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    existing_data = yaml.safe_load(f) or {}
            
            # Preparar dados para salvar
            if 'profiles' not in existing_data:
                existing_data['profiles'] = {}
            
            existing_data['profiles'][profile] = self._config_to_dict(config)
            
            # Salvar arquivo
            with open(self.config_file, 'w', encoding='utf-8') as f:
                yaml.dump(existing_data, f, default_flow_style=False, allow_unicode=True)
                
        except Exception as e:
            raise ConfigurationError(f"Erro ao salvar configuração: {e}")
    
    def _config_to_dict(self, config: CAGEDConfig) -> Dict[str, Any]:
        """Converte configuração para dicionário"""
        return {
            'ftp': {
                'server': config.ftp.server,
                'directory': config.ftp.directory,
                'timeout': config.ftp.timeout,
                'max_retries': config.ftp.max_retries,
            },
            'cache': {
                'enabled': config.cache.enabled,
                'directory': config.cache.directory,
                'max_size_gb': config.cache.max_size_gb,
                'expiry_days': config.cache.expiry_days,
            },
            'processing': {
                'max_workers': config.processing.max_workers,
                'chunk_size': config.processing.chunk_size,
                'memory_limit_gb': config.processing.memory_limit_gb,
                'enable_parallel': config.processing.enable_parallel,
            },
            'output': {
                'format': config.output.format,
                'compression': config.output.compression,
                'directory': config.output.directory,
            },
            'logging': {
                'level': config.logging.level,
                'enable_file': config.logging.enable_file,
                'enable_console': config.logging.enable_console,
                'use_emojis': config.logging.use_emojis,
            },
            'debug_mode': config.debug_mode,
        }
    
    def get_config(self) -> CAGEDConfig:
        """Retorna configuração atual"""
        if self._config is None:
            return self.load_config()
        return self._config
    
    def create_default_config_file(self, profile: str = "default"):
        """Cria arquivo de configuração padrão"""
        default_config = CAGEDConfig()
        self.save_config(default_config, profile)


# Instância global do gerenciador de configuração
config_manager = ConfigManager()


def get_config(profile: str = "default") -> CAGEDConfig:
    """Função utilitária para obter configuração"""
    return config_manager.load_config(profile)


def save_config(config: CAGEDConfig, profile: str = "default"):
    """Função utilitária para salvar configuração"""
    config_manager.save_config(config, profile)