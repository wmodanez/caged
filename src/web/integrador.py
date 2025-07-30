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
                sucesso, metadados = descompactador.descompactar_mensal(ano, mes)
            else:
                sucesso, metadados = descompactador.descompactar_todos(ano)
            
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
            # Usar caminhos absolutos para os diretórios
            diretorio_origem = os.path.abspath('files-unzip')
            diretorio_destino = os.path.abspath('parquet')
            conversor = ConversorParquet(diretorio_origem=diretorio_origem, diretorio_destino=diretorio_destino)
            
            # Configurar callback para atualizar progresso
            def callback_progresso(atual, total):
                progresso = (atual / total) * 100 if total > 0 else 0
                atualizar_processamento(processamento_id, progresso=progresso)
            
            # Converter arquivos
            if mes:
                sucesso, movimentacoes, saldos, indicadores = conversor.converter_mensal(ano, mes)
            else:
                sucesso, movimentacoes, saldos, indicadores = conversor.consolidar_anual(ano)
            
            # Atualizar status baseado no resultado da conversão
            if sucesso:
                atualizar_processamento(
                    processamento_id, 
                    status='concluido', 
                    progresso=100.0,
                    mensagem='Conversão concluída com sucesso!'
                )
                return True
            else:
                atualizar_processamento(
                    processamento_id, 
                    status='erro', 
                    progresso=0.0,
                    mensagem='Erro na conversão: nenhum arquivo foi processado com sucesso'
                )
                return False
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