#!/usr/bin/env python3
"""
Serviço FTP Real para Sistema CAGED
Implementação conforme especificação do item 1.2 do documento IMPLEMENTACAO_SERVICOS_REAIS.md
"""

import ftplib
import hashlib
import os
import re
import time
import functools
import logging
from pathlib import Path
from typing import List, Optional, Dict, Tuple, Callable, Any
from datetime import datetime
from dataclasses import dataclass

# Usar o logger centralizado configurado no main.py
logger = logging.getLogger("caged")

# Importar sistema de métricas
from ..utils.metrics import record_operation, record_cache_hit, record_cache_miss


@dataclass
class FTPConfig:
    """Configuração para o serviço FTP"""
    host: str = "ftp.mtps.gov.br"
    port: int = 21
    username: str = "anonymous"
    password: str = ""
    base_path: str = "/pdet/microdados/NOVO CAGED/"
    timeout: int = 30
    retry_attempts: int = 3
    
    @classmethod
    def from_config(cls, config_dict: dict) -> 'FTPConfig':
        """Cria configuração a partir de dicionário"""
        return cls(
            host=config_dict.get('server', 'ftp.mtps.gov.br'),
            port=21,
            username='anonymous',
            password='',
            base_path=config_dict.get('directory', '/pdet/microdados/NOVO CAGED/'),
            timeout=config_dict.get('timeout', 30),
            retry_attempts=config_dict.get('max_retries', 3)
        )


# Exceções personalizadas
class FTPRetryError(Exception):
    """Exceção para erros que devem ser retentados"""
    pass


class FTPConnectionError(Exception):
    """Exceção para erros de conexão FTP"""
    pass


# Decorator de retry
def retry_on_ftp_error(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator para retry automático em operações FTP
    
    Args:
        max_retries: Número máximo de tentativas
        delay: Delay inicial entre tentativas (segundos)
        backoff: Fator de multiplicação do delay a cada tentativa
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (ftplib.error_temp, ftplib.error_perm, ConnectionError, OSError) as e:
                    last_exception = e
                    error_code = str(e)
                    
                    # Verificar se é um erro que vale a pena retentar
                    retry_errors = [
                        '550',  # File not found / Permission denied
                        '421',  # Service not available
                        '426',  # Connection closed
                        '450',  # Requested file action not taken
                        '451',  # Requested action aborted
                        'timed out',
                        'connection',
                        'reset',
                        'broken pipe'
                    ]
                    
                    should_retry = any(error in error_code.lower() for error in retry_errors)
                    
                    if attempt < max_retries and should_retry:
                        logger.warning(f"🔄 Tentativa {attempt + 1}/{max_retries} falhou para {func.__name__}: {e}")
                        logger.info(f"⏳ Aguardando {current_delay:.1f}s antes da próxima tentativa...")
                        time.sleep(current_delay)
                        current_delay *= backoff
                        
                        # Tentar reconectar se for erro de conexão
                        if hasattr(args[0], 'connect'):
                            logger.debug(f"🔄 Tentando reconectar para {func.__name__}...")
                            try:
                                args[0].connect()
                            except:
                                pass
                    else:
                        break
                except Exception as e:
                    # Para outros tipos de erro, não retentar
                    logger.error(f"❌ Erro não recuperável: {e}")
                    raise e
            
            # Se chegou aqui, todas as tentativas falharam
            logger.error(f"❌ Falha após {max_retries} tentativas: {last_exception}")
            raise FTPRetryError(f"Operação falhou após {max_retries} tentativas: {last_exception}")
        
        return wrapper
    return decorator


class FTPService:
    """
    Serviço FTP Real para Sistema CAGED
    Implementa as funcionalidades especificadas no item 1.2:
    - Conexão com ftp.mtps.gov.br
    - Navegação para diretório /pdet/microdados/CAGED/
    - Download de arquivos CAGEDMOV{AAAA}{MM}.7z
    - Verificação de integridade (tamanho, checksum)
    - Tratamento de erros de conexão e timeout
    - Suporte a retry automático
    """
    
    def __init__(self, config: FTPConfig):
        """
        Inicializa o serviço FTP
        
        Args:
            config: Configuração FTP
        """
        self.config = config
        self.ftp_conn: Optional[ftplib.FTP] = None
        self._is_connected = False
        self._current_directory = None
        
        # Configurar timeout
        if hasattr(ftplib, 'FTP'):
            ftplib.FTP.timeout = self.config.timeout
    
    @retry_on_ftp_error(max_retries=3, delay=2.0)
    def connect(self) -> bool:
        """
        Estabelece conexão com o servidor FTP
        
        Returns:
            bool: True se conectou com sucesso
        """
        if self._is_connected and self.ftp_conn:
            try:
                # Testar se a conexão ainda está ativa
                self.ftp_conn.pwd()
                return True
            except:
                logger.warning("🔄 Conexão FTP perdida, reconectando...")
                self._is_connected = False
                self.ftp_conn = None
        
        connection_start_time = time.time()
        try:
            logger.info(f"🔗 Conectando ao servidor FTP: {self.config.host}:{self.config.port}")
            
            # Criar conexão FTP
            self.ftp_conn = ftplib.FTP()
            self.ftp_conn.connect(self.config.host, self.config.port, timeout=self.config.timeout)
            self.ftp_conn.login(self.config.username, self.config.password)
            
            # Navegar para o diretório base
            logger.info(f"📁 Navegando para: {self.config.base_path}")
            self.ftp_conn.cwd(self.config.base_path)
            self._current_directory = self.config.base_path
            
            self._is_connected = True
            logger.info(f"✅ Conectado com sucesso ao FTP CAGED")
            
            # Registrar métrica de conexão bem-sucedida
            connection_duration = time.time() - connection_start_time
            record_operation(
                "ftp_connection",
                True,
                connection_duration,
                {"server": self.config.host, "directory": self.config.base_path}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao conectar: {e}")
            self._is_connected = False
            self.ftp_conn = None
            
            # Registrar métrica de erro na conexão
            connection_duration = time.time() - connection_start_time
            record_operation(
                "ftp_connection",
                False,
                connection_duration,
                {"server": self.config.host, "error": str(e)}
            )
            
            raise FTPConnectionError(f"Falha na conexão FTP: {e}")
    
    @retry_on_ftp_error(max_retries=3, delay=1.5)
    def download_file(self, ano: int, mes: int, dest_path: Path) -> bool:
        """
        Baixa arquivo CAGEDEST_{MM}{AAAA}.7z do servidor FTP
        
        Args:
            ano: Ano dos dados (ex: 2019)
            mes: Mês dos dados (1-12)
            dest_path: Caminho de destino para salvar o arquivo
            
        Returns:
            bool: True se o download foi bem-sucedido
        """
        if not self._is_connected:
            if not self.connect():
                return False
        
        filename = f"CAGEDEST_{mes:02d}{ano}.7z"
        download_start_time = time.time()
        
        try:
            # Navegar para o diretório do ano
            logger.info(f"📁 Navegando para o diretório do ano: {ano}")
            try:
                self.ftp_conn.cwd(str(ano))
            except ftplib.error_perm:
                logger.error(f"❌ Diretório do ano {ano} não encontrado no servidor")
                return False
            
            # Navegar para o diretório do mês (formato AAAAMM)
            month_dir = f"{ano}{mes:02d}"
            logger.info(f"📁 Navegando para o diretório do mês: {month_dir}")
            try:
                self.ftp_conn.cwd(month_dir)
            except ftplib.error_perm:
                logger.error(f"❌ Diretório do mês {month_dir} não encontrado no servidor")
                self.ftp_conn.cwd("..")  # Voltar ao diretório pai
                return False
            
            # Listar todos os arquivos .7z no diretório do mês
            logger.info(f"🔍 Listando arquivos .7z disponíveis para {ano}/{month_dir}...")
            all_files = self.ftp_conn.nlst()
            zip_files = [f for f in all_files if f.lower().endswith('.7z')]
            
            if not zip_files:
                logger.error(f"❌ Nenhum arquivo .7z encontrado para {ano}/{month_dir}")
                self.ftp_conn.cwd("../..")  # Voltar dois níveis acima
                return False
            
            logger.info(f"📋 Encontrados {len(zip_files)} arquivos .7z para {ano}/{month_dir}")
            
            # Criar estrutura de diretórios espelhando o FTP: files-zip/AAAA/AAAAMM/
            year_month_dir = dest_path.parent / str(ano) / month_dir
            year_month_dir.mkdir(parents=True, exist_ok=True)
            
            downloaded_files = []
            total_size = 0
            
            # Baixar todos os arquivos .7z
            for file in zip_files:
                try:
                    logger.info(f"📥 Baixando arquivo: {file}")
                    
                    # Verificar tamanho do arquivo
                    try:
                        file_size = self.ftp_conn.size(file)
                        logger.info(f"📊 Tamanho: {self._format_size(file_size)}")
                    except ftplib.error_perm:
                        logger.warning(f"⚠️ Não foi possível obter tamanho de {file}")
                        file_size = 0
                    
                    # Definir caminho de destino para este arquivo
                    file_dest_path = year_month_dir / file
                    
                    # Baixar arquivo
                    with open(file_dest_path, 'wb') as f:
                        self.ftp_conn.retrbinary(f'RETR {file}', f.write)
                    
                    # Verificar se o download foi bem-sucedido
                    if not file_dest_path.exists() or file_dest_path.stat().st_size == 0:
                        logger.error(f"❌ Arquivo baixado está vazio ou corrompido: {file}")
                        if file_dest_path.exists():
                            file_dest_path.unlink()
                        continue
                    
                    downloaded_size = file_dest_path.stat().st_size
                    total_size += downloaded_size
                    downloaded_files.append(file)
                    
                    logger.info(f"✅ Download concluído: {file} - {self._format_size(downloaded_size)}")
                    
                except Exception as e:
                    logger.error(f"❌ Erro ao baixar {file}: {e}")
                    continue
            
            # Voltar ao diretório base (dois níveis acima)
            self.ftp_conn.cwd("../..")
            
            if not downloaded_files:
                logger.error(f"❌ Nenhum arquivo foi baixado com sucesso para {ano}")
                return False
            
            download_duration = time.time() - download_start_time
            
            logger.info(f"✅ Download completo: {len(downloaded_files)} arquivos - {self._format_size(total_size)}")
            
            # Registrar métrica de download bem-sucedido
            record_operation(
                "file_download",
                True,
                download_duration,
                {
                    "files_count": len(downloaded_files),
                    "total_size_bytes": total_size,
                    "ano": ano,
                    "mes": mes,
                    "transfer_rate_mbps": (total_size / (1024 * 1024)) / download_duration if download_duration > 0 else 0,
                    "downloaded_files": downloaded_files
                }
            )
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro no download de {filename}: {e}")
            
            # Remover arquivo parcial se existir
            if dest_path.exists():
                dest_path.unlink()
            
            # Registrar métrica de download falhado
            record_operation(
                "file_download",
                False,
                time.time() - download_start_time,
                {
                    "filename": filename,
                    "ano": ano,
                    "mes": mes,
                    "error": str(e)
                }
            )
            
            raise e
    
    @retry_on_ftp_error(max_retries=2, delay=0.5)
    def list_available_files(self) -> List[str]:
        """
        Lista arquivos disponíveis no diretório FTP
        
        Returns:
            List[str]: Lista de nomes de arquivos disponíveis
        """
        if not self._is_connected:
            if not self.connect():
                return []
        
        try:
            logger.info("🔍 Listando arquivos disponíveis no FTP...")
            files = self.ftp_conn.nlst()
            
            # Filtrar apenas arquivos .7z do CAGED
            caged_files = [f for f in files if f.lower().endswith('.7z') and 'CAGEDMOV' in f.upper()]
            
            logger.info(f"📋 Encontrados {len(caged_files)} arquivos CAGED disponíveis")
            for file in caged_files[:5]:  # Mostrar apenas os primeiros 5
                logger.info(f"   📄 {file}")
            if len(caged_files) > 5:
                logger.info(f"   ... e mais {len(caged_files) - 5} arquivos")
            
            return caged_files
            
        except Exception as e:
            logger.error(f"❌ Erro ao listar arquivos: {e}")
            return []
    
    def verify_file_integrity(self, file_path: Path) -> bool:
        """
        Verifica integridade do arquivo baixado
        
        Args:
            file_path: Caminho do arquivo para verificar
            
        Returns:
            bool: True se o arquivo está íntegro
        """
        if not file_path.exists():
            logger.error(f"❌ Arquivo não encontrado: {file_path}")
            return False
        
        try:
            file_size = file_path.stat().st_size
            
            # Verificar se arquivo não está vazio
            if file_size == 0:
                logger.error(f"❌ Arquivo está vazio: {file_path.name}")
                return False
            
            # Verificar tamanho mínimo esperado (arquivos CAGED são tipicamente > 1MB)
            min_size = 1024 * 1024  # 1MB
            if file_size < min_size:
                logger.warning(f"⚠️  Arquivo muito pequeno ({self._format_size(file_size)}): {file_path.name}")
                return False
            
            # Calcular checksum MD5 para verificação de integridade
            md5_hash = hashlib.md5()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    md5_hash.update(chunk)
            
            checksum = md5_hash.hexdigest()
            logger.info(f"🔐 Checksum MD5: {checksum} - {file_path.name}")
            
            # Verificar se é um arquivo 7z válido (magic number)
            with open(file_path, 'rb') as f:
                magic = f.read(6)
                if magic != b'7z\xbc\xaf\'\x1c':
                    logger.error(f"❌ Arquivo não é um 7z válido: {file_path.name}")
                    return False
            
            logger.info(f"✅ Arquivo íntegro: {file_path.name} - {self._format_size(file_size)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro na verificação de integridade: {e}")
            return False
    
    def disconnect(self):
        """
        Fecha conexão FTP
        """
        if self.ftp_conn:
            try:
                self.ftp_conn.quit()
                logger.info("🔌 Conexão FTP encerrada")
            except:
                try:
                    self.ftp_conn.close()
                except:
                    pass
            finally:
                self.ftp_conn = None
                self._is_connected = False
                self._current_directory = None
    
    def _format_size(self, bytes_size: int) -> str:
        """
        Formatar tamanho do arquivo em formato legível
        
        Args:
            bytes_size: Tamanho em bytes
            
        Returns:
            str: Tamanho formatado (KB, MB, GB)
        """
        size = float(bytes_size)
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"


# Manter compatibilidade com código existente
class GerenciadorArquivosCaged(FTPService):
    """
    Classe de compatibilidade - redireciona para FTPService
    Mantida para não quebrar código existente
    """
    
    def __init__(self, 
                 servidor_ftp: str = "ftp.mtps.gov.br",
                 diretorio_remoto: str = "/pdet/microdados/NOVO CAGED"):
        config = FTPConfig(
            host=servidor_ftp,
            base_path=diretorio_remoto
        )
        super().__init__(config)
        self.servidor_ftp = servidor_ftp
        self.diretorio_remoto = diretorio_remoto
        self.arquivos_disponiveis = []
        self.metadados_downloads: Dict[str, Dict] = {}
        self.contador_entidades = 0
    
    def conectar(self) -> bool:
        """Método de compatibilidade"""
        return self.connect()
    
    def desconectar(self):
        """Método de compatibilidade"""
        self.disconnect()


# Função para testar rapidamente a conexão com o FTP CAGED
def testar_conexao_caged():
    """
    Função para testar rapidamente a conexão com o FTP CAGED
    """
    print("🧪 Testando conexão com FTP CAGED...")
    
    config = FTPConfig()
    ftp_service = FTPService(config)
    
    try:
        if ftp_service.connect():
            print("✅ Conexão estabelecida com sucesso")
            
            # Testar listagem de arquivos
            files = ftp_service.list_available_files()
            print(f"📋 Encontrados {len(files)} arquivos CAGED")
            
            ftp_service.disconnect()
            return True
        else:
            print("❌ Falha na conexão")
            return False
    except Exception as e:
        print(f"❌ Erro no teste: {e}")
        return False


if __name__ == "__main__":
    testar_conexao_caged()