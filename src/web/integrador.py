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
            if mes is not None:  # Download de um mês específico
                sucesso, _ = gerenciador.baixar_dados_mensais(ano, mes)
            else:
                # Para download anual, verificar quais meses estão disponíveis
                sucesso = True
                meses_disponiveis = []
                
                # Verificar quais meses estão disponíveis no servidor FTP
                if gerenciador.conectar():
                    try:
                        gerenciador.ftp_conn.cwd(str(ano))
                        diretorios = gerenciador.ftp_conn.nlst()
                        
                        for diretorio in diretorios:
                            if diretorio.startswith(str(ano)) and len(diretorio) == 6:  # formato AAAAMM
                                mes_num = int(diretorio[-2:])
                                meses_disponiveis.append(mes_num)
                        
                        meses_disponiveis.sort()
                        atualizar_processamento(
                            processamento_id,
                            mensagem=f'Meses disponíveis para {ano}: {meses_disponiveis}'
                        )
                    except Exception as e:
                        atualizar_processamento(
                            processamento_id,
                            mensagem=f'Erro ao verificar meses disponíveis: {str(e)}. Tentando todos os meses.'
                        )
                        meses_disponiveis = list(range(1, 13))
                else:
                    atualizar_processamento(
                        processamento_id,
                        mensagem='Falha ao conectar ao FTP. Tentando todos os meses.'
                    )
                    meses_disponiveis = list(range(1, 13))
                
                # Baixar cada mês disponível
                meses_baixados = 0
                for m in meses_disponiveis:
                    try:
                        atualizar_processamento(
                            processamento_id,
                            mensagem=f'Baixando dados para {ano}/{m:02d}...'
                        )
                        sucesso_mes, metadados = gerenciador.baixar_dados_mensais(ano, m)
                        
                        if sucesso_mes:
                            meses_baixados += 1
                            atualizar_processamento(
                                processamento_id,
                                mensagem=f'Download concluído para {ano}/{m:02d}. Arquivos: {len(metadados)}'
                            )
                        else:
                            atualizar_processamento(
                                processamento_id,
                                mensagem=f'Falha no download para {ano}/{m:02d}'
                            )
                    except Exception as e:
                        sucesso = False
                        atualizar_processamento(
                            processamento_id,
                            mensagem=f'Erro no mês {m}: {str(e)}'
                        )
            
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