#!/usr/bin/env python3
"""
Gerenciador de Arquivos CAGED
Adaptado para download de dados mensais do CAGED via FTP
"""

import ftplib
import os
import re
import time
import functools
import logging
from pathlib import Path
from typing import List, Optional, Dict, Tuple, Callable, Any
from datetime import datetime

import logging

# Usar o logger centralizado configurado no main.py
logger = logging.getLogger("caged")

# Importação das entidades
from src.Entity.movimentacao import Movimentacao
from src.Entity.saldo_mensal import SaldoMensal
from src.Entity.exclusao import Exclusao
from src.Entity.movimentacao_fora_prazo import MovimentacaoForaPrazo
from src.Entity.indicador import Indicador


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
                        if hasattr(args[0], '_reconectar_se_necessario'):
                            logger.debug(f"🔄 Tentando reconectar para {func.__name__}...")
                            args[0]._reconectar_se_necessario()
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


class GerenciadorArquivosCaged:
    """
    Gerenciador para download de arquivos CAGED do FTP oficial
    Adaptado para periodicidade mensal
    """
    
    def __init__(self, 
                 servidor_ftp: str = "ftp.mtps.gov.br",
                 diretorio_remoto: str = "/pdet/microdados/NOVO CAGED"):
        """
        Inicializa o gerenciador de arquivos CAGED
        
        Args:
            servidor_ftp: Servidor FTP oficial
            diretorio_remoto: Diretório remoto dos dados CAGED
        """
        self.servidor_ftp = servidor_ftp
        self.diretorio_remoto = diretorio_remoto
        self.ftp_conn = None
        self.arquivos_disponiveis = []
        self.metadados_downloads: Dict[str, Dict] = {}
        self.contador_entidades = 0
        self._diretorio_atual = None  # Para rastrear o diretório atual
        
    def _reconectar_se_necessario(self) -> bool:
        """
        Reconecta ao FTP se a conexão foi perdida
        
        Returns:
            bool: True se reconectou com sucesso
        """
        try:
            if self.ftp_conn:
                # Testar se a conexão ainda está ativa
                self.ftp_conn.pwd()
                return True
        except:
            logger.warning("🔄 Conexão FTP perdida, tentando reconectar...")
            self.ftp_conn = None
        
        # Tentar reconectar
        if self.conectar():
            # Restaurar diretório se necessário
            if self._diretorio_atual:
                try:
                    self.ftp_conn.cwd(self._diretorio_atual)
                    logger.info(f"📁 Diretório restaurado: {self._diretorio_atual}")
                except:
                    logger.warning(f"⚠️  Não foi possível restaurar diretório: {self._diretorio_atual}")
            return True
        return False
    
    @retry_on_ftp_error(max_retries=3, delay=2.0)
    def conectar(self) -> bool:
        """
        Estabelece conexão com o servidor FTP
        
        Returns:
            bool: True se conectou com sucesso
        """
        try:
            logger.info(f"🔗 Conectando ao servidor FTP: {self.servidor_ftp}")
            self.ftp_conn = ftplib.FTP(self.servidor_ftp)
            self.ftp_conn.login()  # Login anônimo
            caminhos_possiveis = [
                "/pdet/microdados/NOVO CAGED",
                "/pdet/microdados/NOVO_CAGED",
                "/pdet/microdados/CAGED",
                "/microdados/CAGED",
                "/caged",
                "/dados/caged",
                "/public/caged"
            ]
            for caminho in caminhos_possiveis:
                try:
                    logger.info(f"📁 Tentando navegar para: {caminho}")
                    self.ftp_conn.cwd(caminho)
                    self._diretorio_atual = caminho
                    logger.info(f"✅ Conectado com sucesso em: {caminho}")
                    self.diretorio_remoto = caminho
                    return True
                except ftplib.error_perm as e:
                    logger.warning(f"❌ Caminho não encontrado: {caminho} - {e}")
                    continue
            logger.warning("🔍 Nenhum caminho CAGED encontrado. Listando diretório raiz...")
            arquivos = []
            self.ftp_conn.retrlines('LIST', arquivos.append)
            logger.info("📋 Conteúdo do diretório raiz:")
            for arquivo in arquivos[:10]:
                logger.info(f"   {arquivo}")
            logger.error("❌ Não foi possível encontrar o diretório CAGED")
            return False
        except Exception as e:
            logger.error(f"❌ Erro ao conectar: {e}", exc_info=True)
            self.ftp_conn = None
            return False
    
    @retry_on_ftp_error(max_retries=2, delay=0.5)
    def verificar_periodo_existe(self, ano: int, mes: Optional[int] = None) -> bool:
        """
        Verifica se um período (ano/mês) existe no servidor FTP
        
        Args:
            ano: Ano dos dados
            mes: Mês específico (opcional)
            
        Returns:
            bool: True se o período existe no servidor
        """
        if not self.ftp_conn:
            logger.error("❌ Não conectado ao FTP. Use conectar() primeiro.")
            return False
        
        try:
            # Salvar diretório atual
            diretorio_original = self.ftp_conn.pwd()
            
            # Tentar navegar para o ano
            diretorio_ano = str(ano)
            self.ftp_conn.cwd(diretorio_ano)
            
            if mes:
                # Tentar navegar para o mês específico
                diretorio_mes = f"{ano}{mes:02d}"
                self.ftp_conn.cwd(diretorio_mes)
                
                # Verificar se há arquivos .7z no diretório
                try:
                    arquivos = self.ftp_conn.nlst()
                    arquivos_caged = [arquivo for arquivo in arquivos if arquivo.lower().endswith('.7z')]
                    existe = len(arquivos_caged) > 0
                except:
                    existe = False
                
                # Voltar para o diretório do ano
                self.ftp_conn.cwd("..")
            else:
                # Verificar se o ano tem pelo menos um mês com dados
                try:
                    diretorios = self.ftp_conn.nlst()
                    # Procurar por diretórios no formato YYYYMM
                    diretorios_mes = [d for d in diretorios if len(d) == 6 and d.startswith(str(ano))]
                    existe = len(diretorios_mes) > 0
                except:
                    existe = False
            
            # Voltar para o diretório original
            self.ftp_conn.cwd(diretorio_original)
            return existe
            
        except Exception as e:
            logger.debug(f"🔍 Período {ano}/{mes if mes else 'todos'} não encontrado: {e}")
            try:
                # Tentar voltar para o diretório original em caso de erro
                self.ftp_conn.cwd(self.diretorio_remoto)
            except:
                pass
            return False
    
    @retry_on_ftp_error(max_retries=3, delay=1.0)
    def listar_arquivos_mensais(self, ano: int, mes: Optional[int] = None) -> List[str]:
        """
        Lista arquivos mensais disponíveis para download
        
        Args:
            ano: Ano dos dados (ex: 2024)
            mes: Mês específico (1-12) ou None para todos os meses
            
        Returns:
            Lista de nomes de arquivos disponíveis
        """
        if not self.ftp_conn:
            logger.error("❌ Não conectado ao FTP. Use conectar() primeiro.")
            return []
        try:
            diretorio_ano = str(ano)
            logger.info(f"📁 Navegando para diretório do ano: {diretorio_ano}")
            self.ftp_conn.cwd(diretorio_ano)
            if mes:
                diretorio_mes = f"{ano}{mes:02d}"
                logger.info(f"📁 Navegando para diretório do mês: {diretorio_mes}")
                self.ftp_conn.cwd(diretorio_mes)
                logger.info(f"🔍 Listando arquivos CAGED para {ano}/{mes:02d}")
            else:
                logger.info(f"🔍 Listando arquivos CAGED para {ano}")
            try:
                arquivos_caged = self.ftp_conn.nlst()
                arquivos_caged = [arquivo for arquivo in arquivos_caged if arquivo.lower().endswith('.7z')]
            except Exception as e:
                logger.error(f"❌ Erro ao listar arquivos: {e}", exc_info=True)
                arquivos_caged = []
            self.arquivos_disponiveis = arquivos_caged
            logger.info(f"📋 Encontrados {len(arquivos_caged)} arquivos:")
            for arquivo in arquivos_caged[:5]:
                logger.info(f"   📄 {arquivo}")
            if len(arquivos_caged) > 5:
                logger.info(f"   ... e mais {len(arquivos_caged) - 5} arquivos")
            return arquivos_caged
        except Exception as e:
            logger.error(f"❌ Erro ao listar arquivos: {e}", exc_info=True)
            try:
                if mes:
                    self.ftp_conn.cwd("..")
                self.ftp_conn.cwd("..")
            except Exception as e2:
                logger.warning(f"Falha ao retornar diretório raiz: {e2}")
            return []
    
    @retry_on_ftp_error(max_retries=3, delay=1.5)
    def baixar_dados_mensais(self, 
                           ano: int, 
                           mes: int, 
                           diretorio_destino: str = "files-zip") -> Tuple[bool, List[Dict]]:
        """
        Baixa dados mensais do CAGED
        
        Args:
            ano: Ano dos dados
            mes: Mês dos dados (1-12)
            diretorio_destino: Diretório local para salvar
            
        Returns:
            Tuple[bool, List[Dict]]: (Sucesso, Lista de metadados dos downloads)
        """
        if not self.ftp_conn:
            logger.error("❌ Não conectado ao FTP. Use conectar() primeiro.")
            return False, []
        
        # Primeiro verificar se o período existe no servidor
        if not self.verificar_periodo_existe(ano, mes):
            logger.warning(f"⚠️  Período {ano}/{mes:02d} não encontrado no servidor - dados não disponíveis")
            return False, []
        
        # Listar arquivos disponíveis
        arquivos = self.listar_arquivos_mensais(ano, mes)
        if not arquivos:
            logger.warning(f"⚠️  Nenhum arquivo encontrado para {ano}/{mes:02d} - diretório existe mas está vazio")
            return False, []
        
        # Só criar diretório local se houver arquivos para baixar
        destino = Path(diretorio_destino) / str(ano) / f"{ano}{mes:02d}"
        destino.mkdir(parents=True, exist_ok=True)
        logger.info(f"📦 Baixando arquivos CAGED para {ano}/{mes:02d} (contém dados de todas as UFs)")
        sucessos = 0
        total = len(arquivos)
        metadados_downloads = []
        logger.info(f"📥 Iniciando download de {total} arquivos...")
        for i, arquivo in enumerate(arquivos, 1):
            try:
                logger.info(f"📥 [{i}/{total}] Baixando: {arquivo}")
                caminho_local = destino / arquivo
                
                # Verificar se arquivo já existe
                if self.verificar_arquivo_existe(str(caminho_local)):
                    logger.info(f"⏭️  Arquivo já existe: {arquivo}")
                    metadados = self.metadados_downloads.get(arquivo, self._extrair_metadados_arquivo(arquivo, ano, mes))
                    metadados_downloads.append(metadados)
                    sucessos += 1
                    continue
                
                # Tentar baixar o arquivo com retry automático
                if self._baixar_arquivo_individual(arquivo, caminho_local):
                    sucessos += 1
                    tamanho = self._formatar_tamanho(caminho_local.stat().st_size)
                    logger.info(f"✅ {arquivo} - {tamanho}")
                    metadados = self._criar_metadados_download(arquivo, caminho_local, ano, mes)
                    metadados_downloads.append(metadados)
                    self.metadados_downloads[arquivo] = metadados
                else:
                    logger.error(f"❌ Falha no download após todas as tentativas: {arquivo}")
                    
            except FTPRetryError as e:
                logger.error(f"❌ Erro de retry esgotado para {arquivo}: {e}")
            except Exception as e:
                logger.error(f"❌ Erro inesperado ao baixar {arquivo}: {e}", exc_info=True)
        logger.info(f"📊 Download concluído: {sucessos}/{total} arquivos baixados")
        self._criar_indicadores_download(ano, mes, sucessos, total)
        return sucessos > 0, metadados_downloads
    
    @retry_on_ftp_error(max_retries=3, delay=1.0)
    def _baixar_arquivo_individual(self, nome_arquivo: str, caminho_local: Path) -> bool:
        """
        Baixa um arquivo individual do FTP com retry automático
        
        Args:
            nome_arquivo: Nome do arquivo no servidor FTP
            caminho_local: Caminho local onde salvar o arquivo
            
        Returns:
            bool: True se o download foi bem-sucedido
        """
        try:
            with open(caminho_local, 'wb') as f:
                self.ftp_conn.retrbinary(f'RETR {nome_arquivo}', f.write)
            
            # Verificar se o arquivo foi baixado corretamente
            if caminho_local.exists() and caminho_local.stat().st_size > 0:
                return True
            else:
                logger.error(f"❌ Arquivo baixado está vazio ou corrompido: {nome_arquivo}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Erro no download de {nome_arquivo}: {e}")
            # Remover arquivo parcial se existir
            if caminho_local.exists():
                caminho_local.unlink()
            raise e
    
    def _extrair_metadados_arquivo(self, nome_arquivo: str, ano: int, mes: int) -> Dict:
        """
        Extrai metadados básicos do nome do arquivo
        
        Args:
            nome_arquivo: Nome do arquivo
            ano: Ano dos dados
            mes: Mês dos dados
            
        Returns:
            Dict: Metadados extraídos
        """
        metadados = {
            "nome_arquivo": nome_arquivo,
            "ano": ano,
            "mes": mes,
            "competencia": f"{ano}-{mes:02d}",
            "data_processamento": datetime.now().isoformat()
        }
        
        # Tentar identificar UF
        ufs = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", 
               "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", 
               "RO", "RR", "RS", "SC", "SE", "SP", "TO"]
        
        for uf in ufs:
            if uf in nome_arquivo.upper():
                metadados["uf"] = uf
                break
        
        # Identificar tipo de arquivo
        if "MOVIMENTACAO" in nome_arquivo.upper():
            metadados["tipo"] = "movimentacao"
        elif "EXCLUSAO" in nome_arquivo.upper():
            metadados["tipo"] = "exclusao"
        elif "FORA_PRAZO" in nome_arquivo.upper() or "FORAPRAZO" in nome_arquivo.upper():
            metadados["tipo"] = "fora_prazo"
        else:
            metadados["tipo"] = "outros"
        
        return metadados
    
    def _criar_metadados_download(self, nome_arquivo: str, caminho_local: Path, 
                                 ano: int, mes: int) -> Dict:
        """
        Cria metadados completos do download
        
        Args:
            nome_arquivo: Nome do arquivo
            caminho_local: Caminho local do arquivo
            ano: Ano dos dados
            mes: Mês dos dados
            ufs: UFs filtradas
            
        Returns:
            Dict: Metadados completos
        """
        metadados = self._extrair_metadados_arquivo(nome_arquivo, ano, mes)
        
        # Adicionar informações do download
        metadados.update({
            "caminho_local": str(caminho_local),
            "tamanho_bytes": caminho_local.stat().st_size,
            "tamanho_formatado": self._formatar_tamanho(caminho_local.stat().st_size),
            "data_download": datetime.now().isoformat(),
            "ufs_filtradas": None,  # Filtragem por UF deve ser feita após download
            "status": "baixado"
        })
        
        return metadados
    
    def _criar_indicadores_download(self, ano: int, mes: int, arquivos_baixados: int, 
                                   total_arquivos: int) -> List[Indicador]:
        """
        Cria indicadores sobre o processo de download
        
        Args:
            ano: Ano dos dados
            mes: Mês dos dados
            arquivos_baixados: Quantidade de arquivos baixados
            total_arquivos: Total de arquivos disponíveis
            
        Returns:
            List[Indicador]: Lista de indicadores criados
        """
        indicadores = []
        competencia = f"{ano}-{mes:02d}"
        
        # Taxa de sucesso do download
        taxa_sucesso = (arquivos_baixados / total_arquivos) * 100 if total_arquivos > 0 else 0
        
        # Indicadores básicos
        indicadores_dados = [
            ("arquivos_disponiveis", total_arquivos),
            ("arquivos_baixados", arquivos_baixados),
            ("taxa_sucesso_download", taxa_sucesso)
        ]
        
        for nome, valor in indicadores_dados:
            self.contador_entidades += 1
            indicador = Indicador(
                id=self.contador_entidades,
                cnpj="",  # Não aplicável para downloads
                competencia=competencia,
                nome_indicador=nome,
                valor=float(valor)
            )
            indicadores.append(indicador)
        
        print(f"📊 Criados {len(indicadores)} indicadores de download")
        return indicadores
    
    def verificar_arquivo_existe(self, caminho_arquivo: str) -> bool:
        """
        Verifica se arquivo já existe localmente
        
        Args:
            caminho_arquivo: Caminho completo do arquivo
            
        Returns:
            bool: True se arquivo existe e não está vazio
        """
        arquivo = Path(caminho_arquivo)
        return arquivo.exists() and arquivo.stat().st_size > 0
    
    def obter_info_servidor(self) -> dict:
        """
        Obtém informações sobre o servidor FTP
        
        Returns:
            dict: Informações do servidor
        """
        if not self.ftp_conn:
            return {"status": "desconectado"}
        
        try:
            return {
                "status": "conectado",
                "servidor": self.servidor_ftp,
                "diretorio": self.ftp_conn.pwd(),
                "welcome": self.ftp_conn.getwelcome(),
                "arquivos_disponiveis": len(self.arquivos_disponiveis)
            }
        except Exception as e:
            return {"status": "erro", "erro": str(e)}
    
    def listar_anos_disponiveis(self) -> List[int]:
        """
        Lista anos disponíveis no servidor FTP
        
        Returns:
            List[int]: Lista de anos disponíveis
        """
        if not self.ftp_conn:
            logger.error("❌ Não conectado ao FTP")
            return []
        try:
            arquivos = []
            self.ftp_conn.retrlines('LIST', arquivos.append)
            anos = set()
            for linha in arquivos:
                matches = re.findall(r'(20\d{2})', linha)
                for match in matches:
                    anos.add(int(match))
            return sorted(list(anos))
        except Exception as e:
            logger.error(f"❌ Erro ao listar anos: {e}", exc_info=True)
            return []
    
    def _formatar_tamanho(self, bytes_size: int) -> str:
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
    
    def gerar_relatorio_downloads(self, ano: Optional[int] = None) -> Dict:
        """
        Gera relatório sobre os downloads realizados
        
        Args:
            ano: Ano específico ou None para todos
            
        Returns:
            Dict: Relatório com estatísticas
        """
        if not self.metadados_downloads:
            return {"status": "sem_dados", "mensagem": "Nenhum download registrado"}
        
        # Filtrar por ano se especificado
        downloads = self.metadados_downloads.values()
        if ano:
            downloads = [d for d in downloads if d.get("ano") == ano]
        
        if not downloads:
            return {"status": "sem_dados", "mensagem": f"Nenhum download encontrado para {ano}"}
        
        # Calcular estatísticas
        total_arquivos = len(downloads)
        total_bytes = sum(d.get("tamanho_bytes", 0) for d in downloads)
        ufs_unicas = set()
        tipos_unicos = set()
        competencias_unicas = set()
        
        for download in downloads:
            if "uf" in download:
                ufs_unicas.add(download["uf"])
            if "tipo" in download:
                tipos_unicos.add(download["tipo"])
            if "competencia" in download:
                competencias_unicas.add(download["competencia"])
        
        return {
            "total_arquivos": total_arquivos,
            "total_bytes": total_bytes,
            "total_formatado": self._formatar_tamanho(total_bytes),
            "ufs_unicas": len(ufs_unicas),
            "tipos_unicos": len(tipos_unicos),
            "competencias_unicas": len(competencias_unicas),
            "ufs": sorted(list(ufs_unicas)),
            "tipos": sorted(list(tipos_unicos)),
            "competencias": sorted(list(competencias_unicas))
        }
    
    def desconectar(self):
        """
        Fecha conexão FTP
        """
        if self.ftp_conn:
            try:
                self.ftp_conn.quit()
                print("🔌 Conexão FTP encerrada")
            except:
                self.ftp_conn.close()
            finally:
                self.ftp_conn = None


# Função para testar rapidamente a conexão com o FTP CAGED
def testar_conexao_caged():
    """
    Função para testar rapidamente a conexão com o FTP CAGED
    """
    print("🧪 Testando conexão com FTP CAGED...")
    
    gerenciador = GerenciadorArquivosCaged()
    
    if gerenciador.conectar():
        info = gerenciador.obter_info_servidor()
        print(f"📊 Info do servidor: {info}")
        
        anos = gerenciador.listar_anos_disponiveis()
        print(f"📅 Anos disponíveis: {anos}")
        
        # Testar listagem para o ano mais recente
        if anos:
            ano_recente = max(anos)
            arquivos = gerenciador.listar_arquivos_mensais(ano_recente)
            print(f"📋 Arquivos encontrados para {ano_recente}: {len(arquivos)}")
        
        gerenciador.desconectar()
        return True
    else:
        return False


if __name__ == "__main__":
    testar_conexao_caged()