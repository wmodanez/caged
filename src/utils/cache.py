"""Sistema de Cache Inteligente para o projeto CAGED.

Este módulo implementa um sistema de cache robusto que:
- Evita re-downloads desnecessários
- Mantém arquivos extraídos válidos
- Armazena resultados de conversão
- Gerencia metadados de arquivos processados
- Implementa verificação de integridade
- Suporte a expiração automática
"""

import hashlib
import json
import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union

from ..utils.logger import setup_logger

logger = setup_logger(__name__)


class CacheMetadata:
    """Classe para gerenciar metadados de cache."""
    
    def __init__(self, file_path: Path, checksum: str, created_at: datetime, 
                 expires_at: Optional[datetime] = None, file_size: int = 0):
        self.file_path = file_path
        self.checksum = checksum
        self.created_at = created_at
        self.expires_at = expires_at
        self.file_size = file_size
        self.access_count = 0
        self.last_accessed = created_at
    
    def to_dict(self) -> Dict:
        """Converte metadados para dicionário."""
        return {
            'file_path': str(self.file_path),
            'checksum': self.checksum,
            'created_at': self.created_at.isoformat(),
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'file_size': self.file_size,
            'access_count': self.access_count,
            'last_accessed': self.last_accessed.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'CacheMetadata':
        """Cria instância a partir de dicionário."""
        metadata = cls(
            file_path=Path(data['file_path']),
            checksum=data['checksum'],
            created_at=datetime.fromisoformat(data['created_at']),
            expires_at=datetime.fromisoformat(data['expires_at']) if data['expires_at'] else None,
            file_size=data['file_size']
        )
        metadata.access_count = data.get('access_count', 0)
        metadata.last_accessed = datetime.fromisoformat(data.get('last_accessed', data['created_at']))
        return metadata
    
    def is_expired(self) -> bool:
        """Verifica se o cache expirou."""
        if self.expires_at is None:
            return False
        return datetime.now() > self.expires_at
    
    def is_valid(self) -> bool:
        """Verifica se o arquivo ainda é válido."""
        if self.is_expired():
            return False
        
        if not self.file_path.exists():
            return False
        
        # Verificar se o tamanho do arquivo mudou
        current_size = self.file_path.stat().st_size
        if current_size != self.file_size:
            return False
        
        return True
    
    def update_access(self):
        """Atualiza informações de acesso."""
        self.access_count += 1
        self.last_accessed = datetime.now()


class CacheManager:
    """Gerenciador de cache inteligente."""
    
    def __init__(self, cache_dir: Path = Path("cache"), max_size_gb: float = 10.0, 
                 default_expiry_days: int = 30):
        self.cache_dir = Path(cache_dir)
        self.max_size_gb = max_size_gb
        self.default_expiry_days = default_expiry_days
        self.metadata_file = self.cache_dir / "metadata.json"
        self.metadata: Dict[str, CacheMetadata] = {}
        
        # Criar diretório de cache se não existir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Carregar metadados existentes
        self._load_metadata()
        
        logger.info(f"Cache inicializado em: {self.cache_dir}")
        logger.info(f"Tamanho máximo: {self.max_size_gb}GB")
        logger.info(f"Expiração padrão: {self.default_expiry_days} dias")
    
    def _load_metadata(self):
        """Carrega metadados do arquivo."""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                for key, metadata_dict in data.items():
                    self.metadata[key] = CacheMetadata.from_dict(metadata_dict)
                
                logger.info(f"Carregados {len(self.metadata)} itens de cache")
            except Exception as e:
                logger.warning(f"Erro ao carregar metadados: {e}")
                self.metadata = {}
    
    def _save_metadata(self):
        """Salva metadados no arquivo."""
        try:
            data = {key: metadata.to_dict() for key, metadata in self.metadata.items()}
            
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.debug("Metadados salvos com sucesso")
        except Exception as e:
            logger.error(f"Erro ao salvar metadados: {e}")
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """Calcula checksum MD5 do arquivo."""
        hash_md5 = hashlib.md5()
        
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Erro ao calcular checksum de {file_path}: {e}")
            return ""
    
    def _get_cache_size_gb(self) -> float:
        """Calcula tamanho total do cache em GB."""
        total_size = 0
        
        for metadata in self.metadata.values():
            if metadata.file_path.exists():
                total_size += metadata.file_size
        
        return total_size / (1024 ** 3)  # Converter para GB
    
    def _cleanup_expired(self):
        """Remove arquivos expirados do cache."""
        expired_keys = []
        
        for key, metadata in self.metadata.items():
            if metadata.is_expired() or not metadata.file_path.exists():
                expired_keys.append(key)
                
                # Remover arquivo se existir
                if metadata.file_path.exists():
                    try:
                        metadata.file_path.unlink()
                        logger.debug(f"Arquivo expirado removido: {metadata.file_path}")
                    except Exception as e:
                        logger.warning(f"Erro ao remover arquivo expirado {metadata.file_path}: {e}")
        
        # Remover metadados dos arquivos expirados
        for key in expired_keys:
            del self.metadata[key]
        
        if expired_keys:
            logger.info(f"Removidos {len(expired_keys)} itens expirados do cache")
            self._save_metadata()
    
    def _cleanup_by_size(self):
        """Remove arquivos mais antigos se o cache exceder o tamanho máximo."""
        current_size = self._get_cache_size_gb()
        
        if current_size <= self.max_size_gb:
            return
        
        logger.info(f"Cache excedeu tamanho máximo ({current_size:.2f}GB > {self.max_size_gb}GB)")
        
        # Ordenar por último acesso (mais antigos primeiro)
        sorted_items = sorted(
            self.metadata.items(),
            key=lambda x: x[1].last_accessed
        )
        
        removed_count = 0
        for key, metadata in sorted_items:
            if current_size <= self.max_size_gb:
                break
            
            # Remover arquivo
            if metadata.file_path.exists():
                try:
                    file_size_gb = metadata.file_size / (1024 ** 3)
                    metadata.file_path.unlink()
                    current_size -= file_size_gb
                    removed_count += 1
                    logger.debug(f"Arquivo removido por tamanho: {metadata.file_path}")
                except Exception as e:
                    logger.warning(f"Erro ao remover arquivo {metadata.file_path}: {e}")
            
            # Remover metadados
            del self.metadata[key]
        
        if removed_count > 0:
            logger.info(f"Removidos {removed_count} itens por excesso de tamanho")
            self._save_metadata()
    
    def get_cache_key(self, identifier: str, category: str = "default") -> str:
        """Gera chave de cache baseada no identificador e categoria."""
        return f"{category}_{hashlib.md5(identifier.encode()).hexdigest()}"
    
    def get_cached_file(self, key: str) -> Optional[Path]:
        """Recupera arquivo do cache se válido."""
        if key not in self.metadata:
            return None
        
        metadata = self.metadata[key]
        
        if not metadata.is_valid():
            # Remover entrada inválida
            del self.metadata[key]
            self._save_metadata()
            return None
        
        # Atualizar estatísticas de acesso
        metadata.update_access()
        self._save_metadata()
        
        logger.debug(f"Cache hit: {key} -> {metadata.file_path}")
        return metadata.file_path
    
    def cache_file(self, key: str, source_file: Path, 
                   expiry_days: Optional[int] = None) -> bool:
        """Armazena arquivo no cache."""
        if not source_file.exists():
            logger.error(f"Arquivo fonte não existe: {source_file}")
            return False
        
        try:
            # Definir caminho de destino no cache
            cache_file_path = self.cache_dir / f"{key}_{source_file.name}"
            
            # Copiar arquivo para cache
            shutil.copy2(source_file, cache_file_path)
            
            # Calcular checksum e metadados
            checksum = self._calculate_checksum(cache_file_path)
            file_size = cache_file_path.stat().st_size
            created_at = datetime.now()
            
            # Definir expiração
            expiry_days = expiry_days or self.default_expiry_days
            expires_at = created_at + timedelta(days=expiry_days)
            
            # Criar metadados
            metadata = CacheMetadata(
                file_path=cache_file_path,
                checksum=checksum,
                created_at=created_at,
                expires_at=expires_at,
                file_size=file_size
            )
            
            # Armazenar metadados
            self.metadata[key] = metadata
            self._save_metadata()
            
            logger.info(f"Arquivo cacheado: {key} -> {cache_file_path}")
            
            # Limpeza automática
            self._cleanup_expired()
            self._cleanup_by_size()
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao cachear arquivo {source_file}: {e}")
            return False
    
    def invalidate_cache(self, pattern: Optional[str] = None):
        """Remove itens do cache baseado em padrão."""
        if pattern is None:
            # Limpar todo o cache
            keys_to_remove = list(self.metadata.keys())
        else:
            # Limpar itens que correspondem ao padrão
            keys_to_remove = [key for key in self.metadata.keys() if pattern in key]
        
        removed_count = 0
        for key in keys_to_remove:
            metadata = self.metadata[key]
            
            # Remover arquivo
            if metadata.file_path.exists():
                try:
                    metadata.file_path.unlink()
                    removed_count += 1
                except Exception as e:
                    logger.warning(f"Erro ao remover arquivo {metadata.file_path}: {e}")
            
            # Remover metadados
            del self.metadata[key]
        
        if removed_count > 0:
            logger.info(f"Invalidados {removed_count} itens do cache")
            self._save_metadata()
    
    def get_cache_stats(self) -> Dict:
        """Retorna estatísticas do cache."""
        total_items = len(self.metadata)
        total_size_gb = self._get_cache_size_gb()
        
        valid_items = sum(1 for m in self.metadata.values() if m.is_valid())
        expired_items = sum(1 for m in self.metadata.values() if m.is_expired())
        
        total_accesses = sum(m.access_count for m in self.metadata.values())
        
        return {
            'total_items': total_items,
            'valid_items': valid_items,
            'expired_items': expired_items,
            'total_size_gb': round(total_size_gb, 2),
            'max_size_gb': self.max_size_gb,
            'usage_percentage': round((total_size_gb / self.max_size_gb) * 100, 2),
            'total_accesses': total_accesses,
            'cache_directory': str(self.cache_dir)
        }
    
    def cleanup(self):
        """Executa limpeza completa do cache."""
        logger.info("Iniciando limpeza do cache...")
        
        initial_stats = self.get_cache_stats()
        
        self._cleanup_expired()
        self._cleanup_by_size()
        
        final_stats = self.get_cache_stats()
        
        logger.info(f"Limpeza concluída:")
        logger.info(f"  Itens: {initial_stats['total_items']} -> {final_stats['total_items']}")
        logger.info(f"  Tamanho: {initial_stats['total_size_gb']}GB -> {final_stats['total_size_gb']}GB")


# Instância global do cache manager
_cache_manager = None


def get_cache_manager() -> CacheManager:
    """Retorna instância global do cache manager."""
    global _cache_manager
    
    if _cache_manager is None:
        _cache_manager = CacheManager()
    
    return _cache_manager


def clear_cache_manager():
    """Limpa instância global do cache manager."""
    global _cache_manager
    _cache_manager = None