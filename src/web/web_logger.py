import logging
import os
from datetime import datetime
import random
import string

class WebLogger:
    """Sistema de logging específico para a aplicação web CAGED"""
    
    def __init__(self, log_dir=None):
        if log_dir is None:
            # Usar diretório raiz do projeto
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            self.log_dir = os.path.join(project_root, "logs")
        else:
            self.log_dir = log_dir
        self.logger = None
        self.log_file = None
        self._setup_logger()
    
    def _setup_logger(self):
        """Configura o logger com arquivo específico"""
        # Criar diretório de logs se não existir
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        
        # Gerar nome do arquivo de log com milissegundos em vez de sufixo aleatório
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        milliseconds = now.strftime("%H%M%S%f")[:9]  # Formato HHMMSSMMM (hora, minuto, segundo, milissegundos)
        self.log_file = os.path.join(self.log_dir, f"caged_web_{today}_{milliseconds}.log")
        
        # Configurar logger
        self.logger = logging.getLogger('caged_web')
        self.logger.setLevel(logging.DEBUG)
        
        # Remover handlers existentes para evitar duplicação
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Configurar handler para arquivo
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # Configurar handler para console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formato das mensagens
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Adicionar handlers ao logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Log inicial
        self.logger.info(f"Sistema de logging iniciado. Arquivo: {self.log_file}")
    
    def debug(self, message):
        """Log de debug"""
        self.logger.debug(message)
    
    def info(self, message):
        """Log de informação"""
        self.logger.info(message)
    
    def warning(self, message):
        """Log de aviso"""
        self.logger.warning(message)
    
    def error(self, message, exc_info=False):
        """Log de erro"""
        self.logger.error(message, exc_info=exc_info)
    
    def critical(self, message, exc_info=False):
        """Log crítico"""
        self.logger.critical(message, exc_info=exc_info)
    
    def log_request(self, request):
        """Log específico para requisições HTTP"""
        self.info(f"Requisição: {request.method} {request.url} - IP: {request.remote_addr}")
    
    def log_processamento(self, processamento_id, acao, detalhes=""):
        """Log específico para processamentos"""
        self.info(f"Processamento {processamento_id}: {acao} - {detalhes}")
    
    def log_erro_processamento(self, processamento_id, erro, exc_info=False):
        """Log específico para erros de processamento"""
        self.error(f"Erro no processamento {processamento_id}: {erro}", exc_info=exc_info)
    
    def log_database_operation(self, operacao, tabela, detalhes=""):
        """Log específico para operações de banco de dados"""
        self.debug(f"DB {operacao} - Tabela: {tabela} - {detalhes}")
    
    def get_log_file_path(self):
        """Retorna o caminho do arquivo de log atual"""
        return self.log_file

# Instância global do logger
web_logger = WebLogger()