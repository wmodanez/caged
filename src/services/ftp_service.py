#!/usr/bin/env python3
"""
Serviço FTP Real para Sistema CAGED
Implementação conforme especificação do item 1.2 do documento IMPLEMENTACAO_SERVICOS_REAIS.md
"""

import ftplib
import hashlib
import os
import re
import socket
import time
import functools
import logging
from pathlib import Path
from typing import List, Optional, Dict, Tuple, Callable, Any
from datetime import datetime
from dataclasses import dataclass
from contextlib import contextmanager

# Usar o logger centralizado configurado no main.py
logger = logging.getLogger("caged")

# Importar sistema de métricas
from ..utils.metrics import record_operation


@dataclass
class FTPConfig:
    """Configuração para o serviço FTP"""
    host: str = "ftp.mtps.gov.br"
    port: int = 21
    username: str = "anonymous"
    password: str = "user@example.com"
    base_path: str = "/pdet/microdados/NOVO CAGED/"
    timeout: int = 60  # Aumentado para 60 segundos
    retry_attempts: int = 3
    
    @classmethod
    def from_config(cls, config: Dict) -> 'FTPConfig':
        """Cria configuração a partir do objeto de configuração principal"""
        return cls(
            host=config.ftp.server,
            port=21,
            username='anonymous',
            password='user@example.com',
            base_path=config.ftp.directory,
            timeout=config.ftp.timeout,
            retry_attempts=config.ftp.max_retries
        )


# Exceções personalizadas
class FTPRetryError(Exception):
    """Exceção para erros que devem ser retentados"""
    pass


class FTPConnectionError(Exception):
    """Exceção para erros de conexão FTP"""
    pass


# Decorator de retry
def retry_on_ftp_error(max_retries: int = 3, delay: float = 3.0, backoff: float = 2.0):
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
                except (ftplib.error_temp, ftplib.error_perm, ftplib.error_proto, ftplib.error_reply,
                        ConnectionError, OSError, TimeoutError, EOFError, socket.timeout) as e:
                    last_exception = e
                    error_code = str(e)
                    
                    # Verificar se é um erro que vale a pena retentar
                    retry_errors = [
                        '550',  # File not found / Permission denied
                        '421',  # Service not available
                        '425',  # Can't open data connection
                        '426',  # Connection closed
                        '450',  # Requested file action not taken
                        '451',  # Requested action aborted
                        '500',  # Syntax error
                        '501',  # Invalid number of parameters
                        'timed out',
                        'timeout',
                        'connection',
                        'reset',
                        'broken pipe',
                        'eof',
                        'none'
                    ]
                    
                    should_retry = any(error in error_code.lower() for error in retry_errors) or \
                                  isinstance(e, (TimeoutError, socket.timeout, EOFError))
                    
                    if attempt < max_retries and should_retry:
                        logger.warning(f"🔄 Tentativa {attempt + 1}/{max_retries} falhou para {func.__name__}: {e}")
                        logger.info(f"⏳ Aguardando {current_delay:.1f}s antes da próxima tentativa...")
                        time.sleep(current_delay)
                        current_delay *= backoff
                        
                        # Apenas aguardar e tentar novamente, a reconexão será tratada no início do método `connect`
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
    
    def __init__(self, config: Dict):
        """
        Inicializa o serviço FTP
        
        Args:
            config: Configuração principal do CAGED
        """
        self.config = FTPConfig.from_config(config)
        self.ftp_conn: Optional[ftplib.FTP] = None
        self._is_connected = False
        self._current_directory = None
        
        # Configurar timeout
        if hasattr(ftplib, 'FTP'):
            ftplib.FTP.timeout = self.config.timeout
    
    def _connect_and_login(self) -> Optional[ftplib.FTP]:
        """
        Cria e retorna uma nova conexão FTP autenticada.
        """
        try:
            ftp = ftplib.FTP(self.config.host, timeout=self.config.timeout, encoding='latin-1')
            ftp.set_pasv(True)
            ftp.login(self.config.username, self.config.password)
            return ftp
        except Exception as e:
            logger.error(f"❌ Falha ao conectar e logar no FTP: {e}")
            return None

    @retry_on_ftp_error(max_retries=3, delay=2.0)
    def connect(self) -> bool:
        """
        Estabelece conexão com o servidor FTP
        
        Returns:
            bool: True se conectou com sucesso
        """
        ftp = self._connect_and_login()
        if ftp:
            try:
                ftp.cwd(self.config.base_path)
                ftp.quit()
                logger.info("✅ Conectividade FTP OK")
                return True
            except Exception as e:
                logger.error(f"❌ Falha ao verificar conectividade FTP: {e}")
                return False
        return False
    
    @contextmanager
    def _get_ftp_connection(self):
        """
        Context manager para obter e fechar uma conexão FTP.
        """
        ftp = self._connect_and_login()
        if not ftp:
            raise FTPConnectionError("Não foi possível estabelecer conexão com o servidor FTP.")
        
        try:
            yield ftp
        finally:
            if ftp:
                try:
                    ftp.quit()
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao fechar conexão FTP: {e}")

    @retry_on_ftp_error(max_retries=3, delay=2.0)
    def list_remote_directories(self, year: int) -> List[int]:
        """
        Lista os diretórios remotos para um determinado ano e retorna os meses.

        Args:
            year: O ano para o qual listar os diretórios.

        Returns:
            Uma lista de inteiros representando os meses.
        """
        path = f"{self.config.base_path}/{year}"
        
        with self._get_ftp_connection() as ftp:
            try:
                ftp.cwd(path)
                dir_names = ftp.nlst()
                # Extrai o mês do nome do diretório (ex: '202401' -> 1)
                months = []
                for name in dir_names:
                    if name.isdigit() and len(name) == 6 and name.startswith(str(year)):
                        try:
                            month = int(name[4:])
                            if 1 <= month <= 12:
                                months.append(month)
                        except ValueError:
                            continue
                return sorted(months)
            except ftplib.error_perm as e:
                if '550' in str(e):
                    logger.warning(f"Diretório para o ano {year} não encontrado: {e}")
                    return []
                raise e

    @retry_on_ftp_error(max_retries=3, delay=3.0)
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
        download_start_time = time.time()
        with self._get_ftp_connection() as ftp:
            try:
                # Navegar para o diretório do ano
                logger.info(f"📁 Navegando para o diretório do ano: {ano}")
                ftp.cwd(f"{self.config.base_path}/{ano}")

                # Navegar para o diretório do mês (formato AAAAMM)
                month_dir = f"{ano}{mes:02d}"
                logger.info(f"📁 Navegando para o diretório do mês: {month_dir}")
                ftp.cwd(month_dir)

                # Listar todos os arquivos .7z no diretório do mês
                logger.info(f"🔍 Listando arquivos .7z disponíveis para {ano}/{month_dir}...")
                all_files = ftp.nlst()
                zip_files = [f for f in all_files if f.lower().endswith('.7z')]

                if not zip_files:
                    logger.warning(f"⚠️ Nenhum arquivo .7z encontrado para {ano}/{month_dir}")
                    return False

                logger.info(f"📋 Encontrados {len(zip_files)} arquivos .7z para {ano}/{month_dir}")

                # O diretório de destino já deve ser o caminho completo, incluindo ano e mês.
                # Apenas garantimos que ele exista.
                dest_path.mkdir(parents=True, exist_ok=True)

                downloaded_files = []
                total_size = 0

                # Baixar todos os arquivos .7z
                for file in zip_files:
                    file_dest_path = dest_path / file
                    try:
                        logger.info(f"📥 Baixando arquivo: {file}")
                        file_size = ftp.size(file)
                        logger.info(f"📊 Tamanho: {self._format_size(file_size)}")

                        with open(file_dest_path, 'wb') as f:
                            ftp.retrbinary(f'RETR {file}', f.write)

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
                        if file_dest_path.exists():
                            file_dest_path.unlink()
                        continue

                if not downloaded_files:
                    logger.error(f"❌ Nenhum arquivo foi baixado com sucesso para {ano}")
                    return False

                download_duration = time.time() - download_start_time
                logger.info(f"✅ Download completo: {len(downloaded_files)} arquivos - {self._format_size(total_size)}")

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

            except ftplib.error_perm as e:
                if '550' in str(e):
                    logger.warning(f"⚠️ Diretório para {ano}-{mes:02d} não encontrado: {e}")
                else:
                    logger.error(f"❌ Erro de permissão FTP: {e}")
                return False
            except Exception as e:
                logger.error(f"❌ Erro inesperado durante o download: {e}")
                record_operation(
                    "file_download",
                    False,
                    time.time() - download_start_time,
                    {"ano": ano, "mes": mes, "error": str(e)}
                )
                return False
    
    @retry_on_ftp_error(max_retries=3, delay=1.0)
    def list_remote_dirs(self, path: str) -> List[str]:
        """
        Lista diretórios em um caminho remoto.
        """
        with self._get_ftp_connection() as ftp:
            try:
                ftp.cwd(path)
                # Usar nlst() que é mais compatível que mlsd()
                items = ftp.nlst()
                # Heurística: diretórios geralmente não têm '.' no nome
                return [item for item in items if '.' not in item]
            except ftplib.error_perm as e:
                logger.warning(f"⚠️  Não foi possível listar diretórios em '{path}': {e}")
                return []

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
        Fecha conexão FTP de forma segura
        """
        if self.ftp_conn:
            try:
                # Tentar encerrar a conexão de forma limpa
                logger.info("🔌 Encerrando conexão FTP...")
                try:
                    self.ftp_conn.quit()
                    logger.info("✅ Conexão FTP encerrada normalmente")
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao encerrar conexão FTP normalmente: {e}")
                    try:
                        # Forçar fechamento se quit() falhar
                        self.ftp_conn.close()
                        logger.info("✅ Conexão FTP fechada forçadamente")
                    except Exception as e2:
                        logger.warning(f"⚠️ Erro ao forçar fechamento da conexão: {e2}")
            except Exception as e:
                logger.warning(f"⚠️ Erro ao desconectar: {e}")
            finally:
                # Garantir que as referências sejam limpas
                self.ftp_conn = None
                self._is_connected = False
                self._current_directory = None
                logger.info("🧹 Referências de conexão FTP limpas")
        else:
            logger.info("ℹ️ Nenhuma conexão FTP ativa para desconectar")
    
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