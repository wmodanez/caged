from src.web.dashboard import registrar_processamento, atualizar_processamento, run_dashboard
from src.util.gerenciador_ftp import GerenciadorArquivosCaged
from src.util.descompactador import DescompactadorCaged
from src.util.conversor_parquet import ConversorParquetCaged as ConversorParquet
import threading
import os

class IntegradorCagedDashboard:
    """Classe para integrar o processador CAGED com o dashboard web"""
    
    @staticmethod
    def baixar(ano, mes=None, processamento_id=None):
        """Baixa arquivos CAGED e atualiza o dashboard"""
        if not processamento_id:
            processamento_id = registrar_processamento('baixar', ano, mes)
        
        try:
            gerenciador = GerenciadorArquivosCaged()
            
            # Configurar callback para atualizar progresso
            def callback_progresso(atual, total):
                progresso = (atual / total) * 100 if total > 0 else 0
                atualizar_processamento(processamento_id, progresso=progresso)
            
            # Baixar arquivos
            if mes:
                gerenciador.baixar_arquivos_por_mes(ano, mes, callback=callback_progresso)
            else:
                gerenciador.baixar_arquivos_por_ano(ano, callback=callback_progresso)
            
            # Atualizar status para concluído
            atualizar_processamento(
                processamento_id, 
                status='concluido', 
                progresso=100.0,
                mensagem='Download concluído com sucesso!'
            )
            return True
        except Exception as e:
            # Atualizar status para erro
            atualizar_processamento(
                processamento_id, 
                status='erro', 
                mensagem=f'Erro durante o download: {str(e)}'
            )
            return False
    
    @staticmethod
    def descompactar(ano, mes=None, processamento_id=None):
        """Descompacta arquivos CAGED e atualiza o dashboard"""
        if not processamento_id:
            processamento_id = registrar_processamento('descompactar', ano, mes)
        
        try:
            descompactador = DescompactadorCaged()
            
            # Configurar callback para atualizar progresso
            def callback_progresso(atual, total):
                progresso = (atual / total) * 100 if total > 0 else 0
                atualizar_processamento(processamento_id, progresso=progresso)
            
            # Descompactar arquivos
            if mes:
                descompactador.descompactar_por_mes(ano, mes, callback=callback_progresso)
            else:
                descompactador.descompactar_por_ano(ano, callback=callback_progresso)
            
            # Atualizar status para concluído
            atualizar_processamento(
                processamento_id, 
                status='concluido', 
                progresso=100.0,
                mensagem='Descompactação concluída com sucesso!'
            )
            return True
        except Exception as e:
            # Atualizar status para erro
            atualizar_processamento(
                processamento_id, 
                status='erro', 
                mensagem=f'Erro durante a descompactação: {str(e)}'
            )
            return False
    
    @staticmethod
    def converter(ano, mes=None, processamento_id=None):
        """Converte arquivos CAGED para Parquet e atualiza o dashboard"""
        if not processamento_id:
            processamento_id = registrar_processamento('converter', ano, mes)
        
        try:
            conversor = ConversorParquet()
            
            # Configurar callback para atualizar progresso
            def callback_progresso(atual, total):
                progresso = (atual / total) * 100 if total > 0 else 0
                atualizar_processamento(processamento_id, progresso=progresso)
            
            # Converter arquivos
            if mes:
                conversor.converter_por_mes(ano, mes, callback=callback_progresso)
            else:
                conversor.converter_por_ano(ano, callback=callback_progresso)
            
            # Atualizar status para concluído
            atualizar_processamento(
                processamento_id, 
                status='concluido', 
                progresso=100.0,
                mensagem='Conversão concluída com sucesso!'
            )
            return True
        except Exception as e:
            # Atualizar status para erro
            atualizar_processamento(
                processamento_id, 
                status='erro', 
                mensagem=f'Erro durante a conversão: {str(e)}'
            )
            return False
    
    @staticmethod
    def processamento_completo(ano, mes=None):
        """Realiza o processamento completo (baixar, descompactar, converter)"""
        processamento_id = registrar_processamento('completo', ano, mes)
        
        try:
            # Atualizar mensagem
            atualizar_processamento(
                processamento_id, 
                mensagem='Iniciando download dos arquivos...'
            )
            
            # Baixar arquivos
            if not IntegradorCagedDashboard.baixar(ano, mes):
                return False
            
            # Atualizar mensagem e progresso
            atualizar_processamento(
                processamento_id, 
                progresso=33.0,
                mensagem='Download concluído. Iniciando descompactação...'
            )
            
            # Descompactar arquivos
            if not IntegradorCagedDashboard.descompactar(ano, mes):
                return False
            
            # Atualizar mensagem e progresso
            atualizar_processamento(
                processamento_id, 
                progresso=66.0,
                mensagem='Descompactação concluída. Iniciando conversão para Parquet...'
            )
            
            # Converter arquivos
            if not IntegradorCagedDashboard.converter(ano, mes):
                return False
            
            # Atualizar status para concluído
            atualizar_processamento(
                processamento_id, 
                status='concluido', 
                progresso=100.0,
                mensagem='Processamento completo concluído com sucesso!'
            )
            return True
        except Exception as e:
            # Atualizar status para erro
            atualizar_processamento(
                processamento_id, 
                status='erro', 
                mensagem=f'Erro durante o processamento completo: {str(e)}'
            )
            return False

# Função para executar o dashboard
def executar_dashboard(host='127.0.0.1', port=5000, debug=False):
    """Executa o dashboard web"""
    # Criar diretórios necessários
    os.makedirs('src/web/templates', exist_ok=True)
    os.makedirs('src/web/static', exist_ok=True)
    
    # Executar o dashboard
    run_dashboard(host=host, port=port, debug=debug)