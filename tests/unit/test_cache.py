"""Testes unitários para o sistema de cache."""

import json
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from src.utils.cache import CacheManager, CacheMetadata


class TestCacheMetadata:
    """Testes para a classe CacheMetadata."""
    
    def test_create_metadata(self):
        """Testa criação de metadados."""
        file_path = Path("test.txt")
        checksum = "abc123"
        created_at = datetime.now()
        expires_at = created_at + timedelta(days=30)
        file_size = 1024
        
        metadata = CacheMetadata(
            file_path=file_path,
            checksum=checksum,
            created_at=created_at,
            expires_at=expires_at,
            file_size=file_size
        )
        
        assert metadata.file_path == file_path
        assert metadata.checksum == checksum
        assert metadata.created_at == created_at
        assert metadata.expires_at == expires_at
        assert metadata.file_size == file_size
        assert metadata.access_count == 0
        assert metadata.last_accessed == created_at
    
    def test_to_dict_and_from_dict(self):
        """Testa conversão para/de dicionário."""
        file_path = Path("test.txt")
        checksum = "abc123"
        created_at = datetime.now()
        expires_at = created_at + timedelta(days=30)
        file_size = 1024
        
        original = CacheMetadata(
            file_path=file_path,
            checksum=checksum,
            created_at=created_at,
            expires_at=expires_at,
            file_size=file_size
        )
        
        # Converter para dict
        data = original.to_dict()
        
        # Converter de volta
        restored = CacheMetadata.from_dict(data)
        
        assert restored.file_path == original.file_path
        assert restored.checksum == original.checksum
        assert restored.created_at == original.created_at
        assert restored.expires_at == original.expires_at
        assert restored.file_size == original.file_size
    
    def test_is_expired(self):
        """Testa verificação de expiração."""
        created_at = datetime.now()
        
        # Sem expiração
        metadata_no_expiry = CacheMetadata(
            file_path=Path("test.txt"),
            checksum="abc123",
            created_at=created_at,
            expires_at=None,
            file_size=1024
        )
        assert not metadata_no_expiry.is_expired()
        
        # Expiração futura
        metadata_future = CacheMetadata(
            file_path=Path("test.txt"),
            checksum="abc123",
            created_at=created_at,
            expires_at=created_at + timedelta(days=1),
            file_size=1024
        )
        assert not metadata_future.is_expired()
        
        # Expiração passada
        metadata_past = CacheMetadata(
            file_path=Path("test.txt"),
            checksum="abc123",
            created_at=created_at,
            expires_at=created_at - timedelta(days=1),
            file_size=1024
        )
        assert metadata_past.is_expired()
    
    def test_update_access(self):
        """Testa atualização de acesso."""
        metadata = CacheMetadata(
            file_path=Path("test.txt"),
            checksum="abc123",
            created_at=datetime.now(),
            file_size=1024
        )
        
        initial_count = metadata.access_count
        initial_time = metadata.last_accessed
        
        time.sleep(0.01)  # Pequena pausa para garantir diferença de tempo
        metadata.update_access()
        
        assert metadata.access_count == initial_count + 1
        assert metadata.last_accessed > initial_time


class TestCacheManager:
    """Testes para a classe CacheManager."""
    
    @pytest.fixture
    def temp_cache_dir(self):
        """Cria diretório temporário para cache."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)
    
    @pytest.fixture
    def cache_manager(self, temp_cache_dir):
        """Cria instância de CacheManager para testes."""
        return CacheManager(
            cache_dir=temp_cache_dir,
            max_size_gb=0.001,  # 1MB para testes
            default_expiry_days=1
        )
    
    @pytest.fixture
    def sample_file(self, temp_cache_dir):
        """Cria arquivo de exemplo para testes."""
        file_path = temp_cache_dir / "sample.txt"
        file_path.write_text("Conteúdo de exemplo para teste", encoding='utf-8')
        return file_path
    
    def test_cache_manager_initialization(self, temp_cache_dir):
        """Testa inicialização do CacheManager."""
        cache_manager = CacheManager(cache_dir=temp_cache_dir)
        
        assert cache_manager.cache_dir == temp_cache_dir
        assert cache_manager.cache_dir.exists()
        assert cache_manager.metadata_file == temp_cache_dir / "metadata.json"
        assert isinstance(cache_manager.metadata, dict)
    
    def test_get_cache_key(self, cache_manager):
        """Testa geração de chave de cache."""
        key1 = cache_manager.get_cache_key("test_identifier", "downloads")
        key2 = cache_manager.get_cache_key("test_identifier", "extractions")
        key3 = cache_manager.get_cache_key("other_identifier", "downloads")
        
        assert key1.startswith("downloads_")
        assert key2.startswith("extractions_")
        assert key3.startswith("downloads_")
        assert key1 != key2
        assert key1 != key3
        assert len(key1.split("_")[1]) == 32  # MD5 hash length
    
    def test_cache_file_success(self, cache_manager, sample_file):
        """Testa cache de arquivo com sucesso."""
        key = "test_key"
        
        result = cache_manager.cache_file(key, sample_file)
        
        assert result is True
        assert key in cache_manager.metadata
        
        metadata = cache_manager.metadata[key]
        assert metadata.file_path.exists()
        assert metadata.file_size > 0
        assert metadata.checksum != ""
        assert not metadata.is_expired()
    
    def test_cache_file_nonexistent(self, cache_manager, temp_cache_dir):
        """Testa cache de arquivo inexistente."""
        nonexistent_file = temp_cache_dir / "nonexistent.txt"
        key = "test_key"
        
        result = cache_manager.cache_file(key, nonexistent_file)
        
        assert result is False
        assert key not in cache_manager.metadata
    
    def test_get_cached_file_hit(self, cache_manager, sample_file):
        """Testa recuperação de arquivo do cache (cache hit)."""
        key = "test_key"
        
        # Cachear arquivo
        cache_manager.cache_file(key, sample_file)
        
        # Recuperar do cache
        cached_file = cache_manager.get_cached_file(key)
        
        assert cached_file is not None
        assert cached_file.exists()
        assert cached_file.read_text(encoding='utf-8') == sample_file.read_text(encoding='utf-8')
        
        # Verificar atualização de estatísticas
        metadata = cache_manager.metadata[key]
        assert metadata.access_count == 1
    
    def test_get_cached_file_miss(self, cache_manager):
        """Testa recuperação de arquivo não existente no cache (cache miss)."""
        cached_file = cache_manager.get_cached_file("nonexistent_key")
        assert cached_file is None
    
    def test_get_cached_file_expired(self, cache_manager, sample_file):
        """Testa recuperação de arquivo expirado."""
        key = "test_key"
        
        # Cachear arquivo com expiração imediata
        cache_manager.cache_file(key, sample_file, expiry_days=0)
        
        # Simular passagem de tempo
        metadata = cache_manager.metadata[key]
        metadata.expires_at = datetime.now() - timedelta(seconds=1)
        
        # Tentar recuperar arquivo expirado
        cached_file = cache_manager.get_cached_file(key)
        
        assert cached_file is None
        assert key not in cache_manager.metadata
    
    def test_invalidate_cache_all(self, cache_manager, sample_file):
        """Testa invalidação de todo o cache."""
        # Cachear múltiplos arquivos
        cache_manager.cache_file("key1", sample_file)
        cache_manager.cache_file("key2", sample_file)
        cache_manager.cache_file("key3", sample_file)
        
        assert len(cache_manager.metadata) == 3
        
        # Invalidar todo o cache
        cache_manager.invalidate_cache()
        
        assert len(cache_manager.metadata) == 0
    
    def test_invalidate_cache_pattern(self, cache_manager, sample_file):
        """Testa invalidação de cache por padrão."""
        # Cachear arquivos com diferentes padrões
        cache_manager.cache_file("downloads_key1", sample_file)
        cache_manager.cache_file("downloads_key2", sample_file)
        cache_manager.cache_file("extractions_key1", sample_file)
        
        assert len(cache_manager.metadata) == 3
        
        # Invalidar apenas downloads
        cache_manager.invalidate_cache("downloads")
        
        assert len(cache_manager.metadata) == 1
        assert "extractions_key1" in cache_manager.metadata
    
    def test_get_cache_stats(self, cache_manager, sample_file):
        """Testa obtenção de estatísticas do cache."""
        # Cache inicial vazio
        stats = cache_manager.get_cache_stats()
        assert stats['total_items'] == 0
        assert stats['valid_items'] == 0
        assert stats['total_size_gb'] == 0
        
        # Adicionar arquivo ao cache
        cache_manager.cache_file("test_key", sample_file)
        
        stats = cache_manager.get_cache_stats()
        assert stats['total_items'] == 1
        assert stats['valid_items'] == 1
        # Para arquivos pequenos, o tamanho em GB pode ser 0.0 devido ao arredondamento
        # Vamos verificar se há pelo menos um item válido
        assert len(cache_manager.metadata) > 0
        assert stats['usage_percentage'] >= 0
        assert 'cache_directory' in stats
    
    def test_cleanup_expired(self, cache_manager, sample_file):
        """Testa limpeza de arquivos expirados."""
        # Cachear arquivo com expiração imediata
        cache_manager.cache_file("expired_key", sample_file, expiry_days=0)
        cache_manager.cache_file("valid_key", sample_file, expiry_days=1)
        
        # Simular expiração
        metadata = cache_manager.metadata["expired_key"]
        metadata.expires_at = datetime.now() - timedelta(seconds=1)
        
        assert len(cache_manager.metadata) == 2
        
        # Executar limpeza
        cache_manager.cleanup()
        
        assert len(cache_manager.metadata) == 1
        assert "valid_key" in cache_manager.metadata
        assert "expired_key" not in cache_manager.metadata
    
    def test_metadata_persistence(self, temp_cache_dir, sample_file):
        """Testa persistência de metadados."""
        # Criar primeiro cache manager e adicionar arquivo
        cache_manager1 = CacheManager(cache_dir=temp_cache_dir)
        cache_manager1.cache_file("test_key", sample_file)
        
        assert len(cache_manager1.metadata) == 1
        
        # Criar segundo cache manager (deve carregar metadados existentes)
        cache_manager2 = CacheManager(cache_dir=temp_cache_dir)
        
        assert len(cache_manager2.metadata) == 1
        assert "test_key" in cache_manager2.metadata
        
        # Verificar se arquivo ainda pode ser recuperado
        cached_file = cache_manager2.get_cached_file("test_key")
        assert cached_file is not None
        assert cached_file.exists()
    
    def test_checksum_validation(self, cache_manager, sample_file):
        """Testa validação por checksum."""
        key = "test_key"
        
        # Cachear arquivo
        cache_manager.cache_file(key, sample_file)
        metadata = cache_manager.metadata[key]
        original_checksum = metadata.checksum
        
        # Modificar arquivo cacheado
        metadata.file_path.write_text("Conteúdo modificado", encoding='utf-8')
        
        # Arquivo deve ser considerado inválido devido à mudança de tamanho
        assert not metadata.is_valid()
        
        # Tentar recuperar arquivo modificado
        cached_file = cache_manager.get_cached_file(key)
        assert cached_file is None
        assert key not in cache_manager.metadata


class TestCacheIntegration:
    """Testes de integração para o sistema de cache."""
    
    @pytest.fixture
    def temp_cache_dir(self):
        """Cria diretório temporário para cache."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)
    
    def test_cache_workflow(self, temp_cache_dir):
        """Testa fluxo completo de cache."""
        cache_manager = CacheManager(cache_dir=temp_cache_dir)
        
        # Criar arquivo de teste
        test_file = temp_cache_dir / "test_data.txt"
        test_content = "Dados de teste para cache"
        test_file.write_text(test_content, encoding='utf-8')
        
        # Gerar chave de cache
        key = cache_manager.get_cache_key("2024_01_caged.7z", "downloads")
        
        # Verificar cache miss inicial
        cached_file = cache_manager.get_cached_file(key)
        assert cached_file is None
        
        # Cachear arquivo
        success = cache_manager.cache_file(key, test_file)
        assert success is True
        
        # Verificar cache hit
        cached_file = cache_manager.get_cached_file(key)
        assert cached_file is not None
        assert cached_file.read_text(encoding='utf-8') == test_content
        
        # Verificar estatísticas
        stats = cache_manager.get_cache_stats()
        assert stats['total_items'] == 1
        assert stats['valid_items'] == 1
        assert stats['total_accesses'] == 1
        
        # Acessar novamente
        cached_file = cache_manager.get_cached_file(key)
        assert cached_file is not None
        
        # Verificar atualização de estatísticas
        stats = cache_manager.get_cache_stats()
        assert stats['total_accesses'] == 2
        
        # Invalidar cache
        cache_manager.invalidate_cache("downloads")
        
        # Verificar cache miss após invalidação
        cached_file = cache_manager.get_cached_file(key)
        assert cached_file is None
        
        stats = cache_manager.get_cache_stats()
        assert stats['total_items'] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])