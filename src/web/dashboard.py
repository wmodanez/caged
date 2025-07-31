from flask import Flask, render_template, request, jsonify, flash, redirect, url_for
from flask_wtf.csrf import CSRFProtect
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import os
import threading
import time
import random
from src.util.gerenciador_ftp import GerenciadorArquivosCaged
from src.web.web_logger import web_logger

# Configuração do aplicativo Flask
app = Flask(__name__)
app.config['SECRET_KEY'] = 'caged-dashboard-secret-key'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///dashboard.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Inicialização de extensões
db = SQLAlchemy(app)
csrf = CSRFProtect(app)
bootstrap = Bootstrap(app)

# Log da inicialização
web_logger.info("Aplicação Flask CAGED Dashboard iniciada")
web_logger.info(f"Arquivo de log: {web_logger.get_log_file_path()}")

# Modelo para armazenar o status dos processamentos
class ProcessamentoStatus(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tipo = db.Column(db.String(50))  # download, descompactacao, conversao, consolidacao
    ano = db.Column(db.Integer)
    mes = db.Column(db.Integer, nullable=True)
    status = db.Column(db.String(20))  # em_andamento, concluido, erro
    progresso = db.Column(db.Float, default=0.0)  # 0 a 100
    mensagem = db.Column(db.Text, nullable=True)
    data_inicio = db.Column(db.DateTime, default=datetime.now)
    data_fim = db.Column(db.DateTime, nullable=True)
    
    def to_dict(self):
        return {
            'id': self.id,
            'tipo': self.tipo,
            'ano': self.ano,
            'mes': self.mes,
            'status': self.status,
            'progresso': self.progresso,
            'mensagem': self.mensagem,
            'data_inicio': self.data_inicio.strftime('%Y-%m-%d %H:%M:%S') if self.data_inicio else None,
            'data_fim': self.data_fim.strftime('%Y-%m-%d %H:%M:%S') if self.data_fim else None
        }

# Rotas da aplicação
@app.route('/')
def index():
    web_logger.log_request(request)
    try:
        return render_template('index.html')
    except Exception as e:
        web_logger.error(f"Erro ao renderizar página inicial: {str(e)}", exc_info=True)
        raise

@app.route('/processamentos')
def processamentos():
    web_logger.log_request(request)
    try:
        return render_template('processamentos.html')
    except Exception as e:
        web_logger.error(f"Erro ao renderizar página de processamentos: {str(e)}", exc_info=True)
        raise

@app.route('/processamento/<int:id>')
def detalhe_processamento(id):
    web_logger.log_request(request)
    try:
        processamento = ProcessamentoStatus.query.get_or_404(id)
        web_logger.info(f"Visualizando detalhes do processamento {id}")
        return render_template('detalhe_processamento.html', processamento=processamento)
    except Exception as e:
        web_logger.error(f"Erro ao renderizar detalhes do processamento {id}: {str(e)}", exc_info=True)
        raise

# API para obter dados dos processamentos
@app.route('/api/processamentos')
def api_processamentos():
    web_logger.log_request(request)
    try:
        processamentos = ProcessamentoStatus.query.order_by(ProcessamentoStatus.data_inicio.desc()).all()
        web_logger.debug(f"API: Retornando {len(processamentos)} processamentos")
        return jsonify([p.to_dict() for p in processamentos])
    except Exception as e:
        web_logger.error(f"Erro na API de processamentos: {str(e)}", exc_info=True)
        return jsonify({'error': 'Erro interno do servidor'}), 500

@app.route('/api/processamento/<int:id>')
def api_processamento(id):
    web_logger.log_request(request)
    try:
        processamento = ProcessamentoStatus.query.get_or_404(id)
        web_logger.debug(f"API: Retornando dados do processamento {id}")
        
        # Obter dados do processamento
        dados = processamento.to_dict()
        
        # Adicionar logs do processamento
        dados['logs'] = obter_logs_processamento(id)
        
        return jsonify(dados)
    except Exception as e:
        web_logger.error(f"Erro na API do processamento {id}: {str(e)}", exc_info=True)
        return jsonify({'error': 'Erro interno do servidor'}), 500

# Função para obter logs de um processamento específico
def obter_logs_processamento(processamento_id):
    """Obtém os logs relacionados a um processamento específico"""
    try:
        # Obter caminho do arquivo de log atual
        log_file = web_logger.get_log_file_path()
        
        if not os.path.exists(log_file):
            web_logger.warning(f"Arquivo de log não encontrado: {log_file}")
            return []
        
        # Ler o arquivo de log e filtrar entradas relacionadas ao processamento
        logs = []
        with open(log_file, 'r', encoding='utf-8') as f:
            for linha in f:
                # Verificar se a linha contém o ID do processamento
                if f"Processamento {processamento_id}" in linha:
                    # Extrair a mensagem de log (parte após o último hífen)
                    partes = linha.split(' - ')
                    if len(partes) >= 4:
                        # Formato: data - nome - nível - função:linha - mensagem
                        mensagem = ' - '.join(partes[3:])
                        logs.append(mensagem.strip())
        
        return logs
    except Exception as e:
        web_logger.error(f"Erro ao obter logs do processamento {processamento_id}: {str(e)}", exc_info=True)
        return []

# Rota para excluir um processamento
@app.route('/api/processamento/<int:id>/excluir', methods=['GET', 'POST'])
@csrf.exempt
def excluir_processamento(id):
    web_logger.log_request(request)
    web_logger.info(f"Tentativa de exclusão do processamento {id}")
    
    try:
        processamento = ProcessamentoStatus.query.get_or_404(id)
        web_logger.log_database_operation("DELETE", "processamento_status", f"ID: {id}")
        
        db.session.delete(processamento)
        db.session.commit()
        
        web_logger.info(f"Processamento {id} excluído com sucesso!")
        
        # Se for uma requisição GET, redirecionar para a página de processamentos
        if request.method == 'GET':
            flash(f'Processamento {id} excluído com sucesso!', 'success')
            return redirect(url_for('processamentos'))
        
        # Se for uma requisição POST, retornar JSON
        return jsonify({'success': True, 'message': f'Processamento {id} excluído com sucesso!'})
    except Exception as e:
        db.session.rollback()
        web_logger.error(f"Erro ao excluir processamento {id}: {str(e)}", exc_info=True)
        
        # Se for uma requisição GET, redirecionar para a página de processamentos
        if request.method == 'GET':
            flash(f'Erro ao excluir processamento: {str(e)}', 'danger')
            return redirect(url_for('processamentos'))
        
        # Se for uma requisição POST, retornar JSON
        return jsonify({'success': False, 'message': f'Erro ao excluir processamento: {str(e)}'}), 500

# Rota para iniciar um novo processamento
@app.route('/iniciar_processamento', methods=['POST'])
def iniciar_processamento():
    web_logger.log_request(request)
    
    comando = request.form.get('comando')
    ano = request.form.get('ano')
    mes = request.form.get('mes')
    
    web_logger.info(f"Iniciando processamento: {comando} - Ano: {ano} - Mês: {mes}")
    
    if not comando or not ano:
        web_logger.warning("Tentativa de iniciar processamento sem comando ou ano")
        flash('Comando e ano são obrigatórios', 'danger')
        return redirect(url_for('index'))
    
    try:
        # Criar registro de processamento
        processamento = ProcessamentoStatus(
            tipo=comando,
            ano=int(ano),
            mes=int(mes) if mes else None,
            status='em_andamento',
            progresso=0.0
        )
        db.session.add(processamento)
        db.session.commit()
        
        web_logger.log_database_operation("INSERT", "processamento_status", f"ID: {processamento.id}")
        web_logger.log_processamento(processamento.id, "CRIADO", f"Tipo: {comando}")
        
        # Iniciar processamento real em thread separada
        threading.Thread(target=executar_processamento_real, args=(processamento.id, comando, int(ano), int(mes) if mes else None)).start()
        
        web_logger.info(f"Thread de processamento {processamento.id} iniciada")
        flash(f'Processamento {comando} iniciado com sucesso!', 'success')
        return redirect(url_for('detalhe_processamento', id=processamento.id))
        
    except Exception as e:
        web_logger.error(f"Erro ao iniciar processamento: {str(e)}", exc_info=True)
        flash(f'Erro ao iniciar processamento: {str(e)}', 'danger')
        return redirect(url_for('index'))

# Função para registrar um novo processamento
def registrar_processamento(tipo, ano, mes=None):
    try:
        web_logger.info(f"Registrando novo processamento: {tipo} - {ano}/{mes if mes else 'anual'}")
        processamento = ProcessamentoStatus(
            tipo=tipo,
            ano=ano,
            mes=mes,
            status='em_andamento',
            progresso=0.0
        )
        db.session.add(processamento)
        db.session.commit()
        
        web_logger.log_database_operation("INSERT", "processamento_status", f"ID: {processamento.id}")
        web_logger.log_processamento(processamento.id, "REGISTRADO", f"Tipo: {tipo}")
        return processamento.id
    except Exception as e:
        web_logger.error(f"Erro ao registrar processamento: {str(e)}", exc_info=True)
        raise

# Função para atualizar o status de um processamento
def atualizar_processamento(id, status=None, progresso=None, mensagem=None):
    try:
        processamento = ProcessamentoStatus.query.get(id)
        if not processamento:
            web_logger.warning(f"Tentativa de atualizar processamento inexistente: {id}")
            return False
        
        # Log das mudanças
        mudancas = []
        if status and status != processamento.status:
            mudancas.append(f"status: {processamento.status} -> {status}")
            processamento.status = status
        if progresso is not None and progresso != processamento.progresso:
            mudancas.append(f"progresso: {processamento.progresso}% -> {progresso}%")
            processamento.progresso = progresso
        if mensagem and mensagem != processamento.mensagem:
            mudancas.append(f"mensagem: {mensagem}")
            processamento.mensagem = mensagem
        
        if mudancas:
            web_logger.log_processamento(id, "ATUALIZADO", "; ".join(mudancas))
        
        if status == 'concluido' or status == 'erro':
            processamento.data_fim = datetime.now()
            web_logger.log_processamento(id, "FINALIZADO", f"Status final: {status}")
        
        db.session.commit()
        web_logger.log_database_operation("UPDATE", "processamento_status", f"ID: {id}")
        return True
    except Exception as e:
        web_logger.error(f"Erro ao atualizar processamento {id}: {str(e)}", exc_info=True)
        return False

# Função para executar processamento real dos dados CAGED
def executar_processamento_real(processamento_id, comando, ano, mes=None):
    """Executa o processamento real dos dados CAGED"""
    with app.app_context():
        web_logger.info(f"Iniciando execução do processamento {processamento_id}: {comando}")
        processamento = ProcessamentoStatus.query.get(processamento_id)
        if not processamento:
            web_logger.error(f"Processamento {processamento_id} não encontrado para execução")
            return
        
        try:
            if comando.lower() in ['download', 'baixar']:
                # Executar download real
                gerenciador = GerenciadorArquivosCaged()
                
                # Atualizar status
                web_logger.log_processamento(processamento_id, "DOWNLOAD_INICIADO", f"Ano: {ano}, Mês: {mes}")
                atualizar_processamento(processamento_id, mensagem='Conectando ao servidor FTP...')
                
                # Conectar ao FTP
                if not gerenciador.conectar():
                    web_logger.log_erro_processamento(processamento_id, "Falha na conexão FTP")
                    atualizar_processamento(
                        processamento_id,
                        status='erro',
                        mensagem='Erro ao conectar com o servidor FTP'
                    )
                    return
                
                # Atualizar progresso
                atualizar_processamento(processamento_id, progresso=10.0, mensagem='Conectado! Listando arquivos disponíveis...')
                
                # Baixar arquivos
                if mes:
                    atualizar_processamento(processamento_id, progresso=20.0, mensagem=f'Iniciando download para {ano}/{mes:02d}...')
                    sucesso, metadados = gerenciador.baixar_dados_mensais(ano, mes)
                else:
                    atualizar_processamento(processamento_id, progresso=20.0, mensagem=f'Iniciando download para {ano}...')
                    # Para download anual, baixar todos os meses
                    sucesso = False  # Será True se pelo menos um mês for baixado com sucesso
                    total_meses = 12
                    meses_baixados = 0
                    meses_disponiveis = []
                    
                    # Primeiro, verificar quais meses estão disponíveis
                    try:
                        gerenciador.ftp_conn.cwd(str(ano))
                        diretorios = gerenciador.ftp_conn.nlst()
                        for diretorio in diretorios:
                            if diretorio.startswith(str(ano)) and len(diretorio) == 6:  # formato AAAAMM
                                mes_num = int(diretorio[-2:])
                                meses_disponiveis.append(mes_num)
                        meses_disponiveis.sort()
                        
                        if not meses_disponiveis:
                            web_logger.log_erro_processamento(processamento_id, f"Nenhum mês disponível para o ano {ano}")
                            atualizar_processamento(
                                processamento_id,
                                status='erro',
                                mensagem=f'Nenhum mês disponível para o ano {ano}'
                            )
                            return
                            
                        web_logger.info(f"Meses disponíveis para {ano}: {meses_disponiveis}")
                        atualizar_processamento(processamento_id, mensagem=f'Meses disponíveis para {ano}: {meses_disponiveis}')
                    except Exception as e:
                        web_logger.log_erro_processamento(processamento_id, f"Erro ao listar meses disponíveis: {e}")
                        # Continuar com a abordagem padrão se não conseguir listar os meses
                        meses_disponiveis = list(range(1, 13))
                    
                    # Baixar apenas os meses disponíveis
                    for i, m in enumerate(meses_disponiveis):
                        try:
                            progresso_mes = 20.0 + (i / len(meses_disponiveis)) * 70.0
                            atualizar_processamento(processamento_id, progresso=progresso_mes, mensagem=f'Baixando dados de {ano}/{m:02d}...')
                            sucesso_mes, metadados_mes = gerenciador.baixar_dados_mensais(ano, m)
                            if sucesso_mes:
                                sucesso = True  # Pelo menos um mês foi baixado com sucesso
                                meses_baixados += 1
                            else:
                                web_logger.warning(f"Aviso: Falha no download de {ano}/{m:02d}")
                        except Exception as e:
                            web_logger.log_erro_processamento(processamento_id, f"Erro no download de {ano}/{m:02d}: {e}")
                    
                    # Atualizar mensagem com o resumo
                    if meses_baixados > 0:
                        atualizar_processamento(processamento_id, mensagem=f'Download concluído para {meses_baixados} de {len(meses_disponiveis)} meses disponíveis')
                    else:
                        atualizar_processamento(processamento_id, mensagem=f'Nenhum mês foi baixado com sucesso')
                
                # Desconectar
                gerenciador.desconectar()
                
                if sucesso:
                    web_logger.log_processamento(processamento_id, "DOWNLOAD_CONCLUIDO", "Sucesso")
                    atualizar_processamento(
                        processamento_id,
                        status='concluido',
                        progresso=100.0,
                        mensagem='Download concluído com sucesso!'
                    )
                else:
                    web_logger.log_erro_processamento(processamento_id, "Falha no download dos arquivos")
                    atualizar_processamento(
                        processamento_id,
                        status='erro',
                        mensagem='Falha no download dos arquivos'
                    )
            
            elif comando.lower() == 'descompactar':
                # Executar descompactação real
                web_logger.log_processamento(processamento_id, "DESCOMPACTACAO_INICIADA", f"Ano: {ano}, Mês: {mes}")
                from src.web.integrador import IntegradorCagedDashboard
                IntegradorCagedDashboard.descompactar(ano, mes, processamento_id)
                
            elif comando.lower() == 'converter':
                # Executar conversão real
                web_logger.log_processamento(processamento_id, "CONVERSAO_INICIADA", f"Ano: {ano}, Mês: {mes}")
                from src.web.integrador import IntegradorCagedDashboard
                IntegradorCagedDashboard.converter(ano, mes, processamento_id)
                
            elif comando.lower() == 'completo':
                # Executar processamento completo real
                web_logger.log_processamento(processamento_id, "PROCESSAMENTO_COMPLETO_INICIADO", f"Ano: {ano}, Mês: {mes}")
                from src.web.integrador import IntegradorCagedDashboard
                IntegradorCagedDashboard.processamento_completo(ano, mes)
                
            else:
                # Para comandos não reconhecidos, marcar como erro
                web_logger.log_erro_processamento(processamento_id, f"Comando não reconhecido: {comando}")
                atualizar_processamento(
                    processamento_id,
                    status='erro',
                    mensagem=f'Comando não reconhecido: {comando}'
                )
                
        except Exception as e:
            web_logger.log_erro_processamento(processamento_id, f"Erro durante execução: {str(e)}", exc_info=True)
            atualizar_processamento(
                processamento_id,
                status='erro',
                mensagem=f'Erro durante o processamento: {str(e)}'
            )



# Inicializar banco de dados
def init_db():
    with app.app_context():
        try:
            # Obter caminho absoluto do banco de dados
            db_uri = app.config['SQLALCHEMY_DATABASE_URI']
            if db_uri.startswith('sqlite:///'):
                # Caminho relativo - converter para absoluto
                db_rel_path = db_uri.replace('sqlite:///', '')
                if not os.path.isabs(db_rel_path):
                    # Se o caminho for relativo ao diretório atual da aplicação
                    app_dir = os.path.dirname(os.path.abspath(__file__))
                    db_abs_path = os.path.join(app_dir, db_rel_path)
                else:
                    db_abs_path = db_rel_path
            else:
                # Outro tipo de banco de dados ou caminho já absoluto
                db_abs_path = db_uri.split('/')[-1]
            
            # Verificar se o banco de dados existe e tem tamanho maior que zero
            db_exists = os.path.exists(db_abs_path) and os.path.getsize(db_abs_path) > 0
            
            # Verificar se a tabela existe
            table_exists = False
            if db_exists:
                try:
                    # Tentar fazer uma consulta simples para verificar se a tabela existe
                    ProcessamentoStatus.query.first()
                    table_exists = True
                    web_logger.info(f"Banco de dados já existe em {db_abs_path} e tabela ProcessamentoStatus está presente.")
                except Exception as table_error:
                    web_logger.warning(f"Banco de dados existe, mas tabela ProcessamentoStatus não encontrada: {str(table_error)}")
                    # Se o arquivo existe mas a tabela não, vamos recriar o banco
                    table_exists = False
            
            if not db_exists or not table_exists:
                web_logger.info(f"Inicializando novo banco de dados em {db_abs_path}")
                # Se o arquivo existe mas está vazio ou corrompido, vamos excluí-lo e recriar
                if os.path.exists(db_abs_path):
                    try:
                        os.remove(db_abs_path)
                        web_logger.info(f"Arquivo de banco de dados existente removido: {db_abs_path}")
                    except Exception as remove_error:
                        web_logger.warning(f"Não foi possível remover o arquivo de banco de dados: {str(remove_error)}")
                
                # Criar todas as tabelas
                db.create_all()
                web_logger.info("Banco de dados inicializado com sucesso")
        except Exception as e:
            web_logger.error(f"Erro ao inicializar banco de dados: {str(e)}", exc_info=True)
            raise

# Função para verificar e corrigir processamentos pendentes
def verificar_processamentos_pendentes():
    """Verifica e corrige processamentos que ficaram com status 'em_andamento'."""
    try:
        web_logger.info("Verificando processamentos pendentes")
        processamentos = ProcessamentoStatus.query.filter_by(status='em_andamento').all()
        
        if processamentos:
            web_logger.warning(f"Encontrados {len(processamentos)} processamentos pendentes. Corrigindo...")
            for p in processamentos:
                web_logger.log_processamento(p.id, "CORRIGIDO", "Processamento interrompido")
                p.status = 'erro'
                p.mensagem = 'Processamento interrompido devido ao encerramento do servidor.'
                p.data_fim = datetime.now()
            
            db.session.commit()
            web_logger.info("Processamentos pendentes corrigidos para status 'erro'")
        else:
            web_logger.info("Nenhum processamento pendente encontrado")
    except Exception as e:
        web_logger.error(f"Erro ao verificar processamentos pendentes: {str(e)}", exc_info=True)

# Função para executar o aplicativo
def run_dashboard(host='127.0.0.1', port=5000, debug=False):
    web_logger.info(f"Iniciando dashboard CAGED em {host}:{port} (debug={debug})")
    
    try:
        init_db()
        
        # Verificar processamentos pendentes ao iniciar
        with app.app_context():
            verificar_processamentos_pendentes()
        
        web_logger.info("Dashboard CAGED pronto para uso")
        app.run(host=host, port=port, debug=debug)
    except Exception as e:
        web_logger.critical(f"Erro crítico ao iniciar dashboard: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    try:
        init_db()
        web_logger.info("Iniciando aplicação em modo debug")
        app.run(debug=True)
    except Exception as e:
        web_logger.critical(f"Erro crítico na inicialização: {str(e)}", exc_info=True)
        raise