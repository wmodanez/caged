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

# Configuração do aplicativo Flask
app = Flask(__name__)
app.config['SECRET_KEY'] = 'caged-dashboard-secret-key'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///dashboard.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Inicialização de extensões
db = SQLAlchemy(app)
csrf = CSRFProtect(app)
bootstrap = Bootstrap(app)

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
    return render_template('index.html')

@app.route('/processamentos')
def processamentos():
    return render_template('processamentos.html')

@app.route('/processamento/<int:id>')
def detalhe_processamento(id):
    processamento = ProcessamentoStatus.query.get_or_404(id)
    return render_template('detalhe_processamento.html', processamento=processamento)

# API para obter dados dos processamentos
@app.route('/api/processamentos')
def api_processamentos():
    processamentos = ProcessamentoStatus.query.order_by(ProcessamentoStatus.data_inicio.desc()).all()
    return jsonify([p.to_dict() for p in processamentos])

@app.route('/api/processamento/<int:id>')
def api_processamento(id):
    processamento = ProcessamentoStatus.query.get_or_404(id)
    return jsonify(processamento.to_dict())

# Rota para excluir um processamento
@app.route('/api/processamento/<int:id>/excluir', methods=['GET', 'POST'])
@csrf.exempt
def excluir_processamento(id):
    processamento = ProcessamentoStatus.query.get_or_404(id)
    
    try:
        db.session.delete(processamento)
        db.session.commit()
        print(f"Processamento {id} excluído com sucesso!")
        
        # Se for uma requisição GET, redirecionar para a página de processamentos
        if request.method == 'GET':
            flash(f'Processamento {id} excluído com sucesso!', 'success')
            return redirect(url_for('processamentos'))
        
        # Se for uma requisição POST, retornar JSON
        return jsonify({'success': True, 'message': f'Processamento {id} excluído com sucesso!'})
    except Exception as e:
        db.session.rollback()
        print(f"Erro ao excluir processamento {id}: {str(e)}")
        
        # Se for uma requisição GET, redirecionar para a página de processamentos
        if request.method == 'GET':
            flash(f'Erro ao excluir processamento: {str(e)}', 'danger')
            return redirect(url_for('processamentos'))
        
        # Se for uma requisição POST, retornar JSON
        return jsonify({'success': False, 'message': f'Erro ao excluir processamento: {str(e)}'}), 500

# Rota para iniciar um novo processamento
@app.route('/iniciar_processamento', methods=['POST'])
def iniciar_processamento():
    comando = request.form.get('comando')
    ano = request.form.get('ano')
    mes = request.form.get('mes')
    
    if not comando or not ano:
        flash('Comando e ano são obrigatórios', 'danger')
        return redirect(url_for('index'))
    
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
    
    # Iniciar processamento real em thread separada
    threading.Thread(target=executar_processamento_real, args=(processamento.id, comando, int(ano), int(mes) if mes else None)).start()
    
    flash(f'Processamento {comando} iniciado com sucesso!', 'success')
    return redirect(url_for('detalhe_processamento', id=processamento.id))

# Função para registrar um novo processamento
def registrar_processamento(tipo, ano, mes=None):
    processamento = ProcessamentoStatus(
        tipo=tipo,
        ano=ano,
        mes=mes,
        status='em_andamento',
        progresso=0.0
    )
    db.session.add(processamento)
    db.session.commit()
    return processamento.id

# Função para atualizar o status de um processamento
def atualizar_processamento(id, status=None, progresso=None, mensagem=None):
    processamento = ProcessamentoStatus.query.get(id)
    if not processamento:
        return False
    
    if status:
        processamento.status = status
    if progresso is not None:
        processamento.progresso = progresso
    if mensagem:
        processamento.mensagem = mensagem
    
    if status == 'concluido' or status == 'erro':
        processamento.data_fim = datetime.now()
    
    db.session.commit()
    return True

# Função para executar processamento real dos dados CAGED
def executar_processamento_real(processamento_id, comando, ano, mes=None):
    """Executa o processamento real dos dados CAGED"""
    with app.app_context():
        processamento = ProcessamentoStatus.query.get(processamento_id)
        if not processamento:
            return
        
        try:
            if comando.lower() == 'download':
                # Executar download real
                gerenciador = GerenciadorArquivosCaged()
                
                # Atualizar status
                atualizar_processamento(processamento_id, mensagem='Conectando ao servidor FTP...')
                
                # Conectar ao FTP
                if not gerenciador.conectar():
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
                    sucesso = True
                    total_meses = 12
                    for m in range(1, 13):
                        try:
                            progresso_mes = 20.0 + (m / total_meses) * 70.0
                            atualizar_processamento(processamento_id, progresso=progresso_mes, mensagem=f'Baixando dados de {ano}/{m:02d}...')
                            sucesso_mes, _ = gerenciador.baixar_dados_mensais(ano, m)
                            if not sucesso_mes:
                                print(f"Aviso: Falha no download de {ano}/{m:02d}")
                        except Exception as e:
                            print(f"Erro no download de {ano}/{m:02d}: {e}")
                
                # Desconectar
                gerenciador.desconectar()
                
                if sucesso:
                    atualizar_processamento(
                        processamento_id,
                        status='concluido',
                        progresso=100.0,
                        mensagem='Download concluído com sucesso!'
                    )
                else:
                    atualizar_processamento(
                        processamento_id,
                        status='erro',
                        mensagem='Falha no download dos arquivos'
                    )
            
            elif comando.lower() == 'descompactar':
                # Executar descompactação real
                from src.web.integrador import IntegradorCagedDashboard
                IntegradorCagedDashboard.descompactar(ano, mes, processamento_id)
                
            elif comando.lower() == 'converter':
                # Executar conversão real
                from src.web.integrador import IntegradorCagedDashboard
                IntegradorCagedDashboard.converter(ano, mes, processamento_id)
                
            elif comando.lower() == 'completo':
                # Executar processamento completo real
                from src.web.integrador import IntegradorCagedDashboard
                IntegradorCagedDashboard.processamento_completo(ano, mes)
                
            else:
                # Para comandos não reconhecidos, marcar como erro
                atualizar_processamento(
                    processamento_id,
                    status='erro',
                    mensagem=f'Comando não reconhecido: {comando}'
                )
                
        except Exception as e:
            atualizar_processamento(
                processamento_id,
                status='erro',
                mensagem=f'Erro durante o processamento: {str(e)}'
            )



# Inicializar banco de dados
def init_db():
    with app.app_context():
        db.create_all()

# Função para verificar e corrigir processamentos pendentes
def verificar_processamentos_pendentes():
    """Verifica e corrige processamentos que ficaram com status 'em_andamento'."""
    try:
        processamentos = ProcessamentoStatus.query.filter_by(status='em_andamento').all()
        
        if processamentos:
            print(f"⚠️ Encontrados {len(processamentos)} processamentos pendentes. Corrigindo...")
            for p in processamentos:
                p.status = 'erro'
                p.mensagem = 'Processamento interrompido devido ao encerramento do servidor.'
                p.data_fim = datetime.now()
            
            db.session.commit()
            print("✅ Processamentos pendentes corrigidos para status 'erro'.")
    except Exception as e:
        print(f"⚠️ Erro ao verificar processamentos pendentes: {e}")

# Função para executar o aplicativo
def run_dashboard(host='127.0.0.1', port=5000, debug=False):
    init_db()
    
    # Verificar processamentos pendentes ao iniciar
    with app.app_context():
        verificar_processamentos_pendentes()
    
    app.run(host=host, port=port, debug=debug)

if __name__ == '__main__':
    init_db()
    app.run(debug=True)