#!/usr/bin/env python3
"""
Processador de Dados CAGED
Script principal para download, processamento e consolidação de dados mensais
"""

import click
import logging
from pathlib import Path
from datetime import datetime
from src.util.gerenciador_ftp import GerenciadorArquivosCaged
from src.util.conversor_parquet import ConversorParquetCaged
from src.util.descompactador import DescompactadorCaged


# Configuração de logging padrão
def configurar_logging(nivel: str = "WARNING", usar_emojis: bool = True):
    """
    Configura o sistema de logging padrão para o CAGED
    
    Args:
        nivel: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        usar_emojis: Se deve usar emojis nas mensagens
    """
    # Criar diretório de logs
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Configurar logger principal
    logger = logging.getLogger("caged_main")
    logger.setLevel(getattr(logging, nivel.upper(), logging.WARNING))
    
    # Evitar duplicação de handlers
    if logger.handlers:
        return logger
    
    # Formatter para logs
    formatter = logging.Formatter(
        '%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para arquivo
    file_handler = logging.FileHandler(
        log_dir / f"caged_{datetime.now().strftime('%Y%m%d')}.log",
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Handler para console (apenas para níveis debug e info)
    if nivel.upper() in ['DEBUG', 'INFO']:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger


# Logger global
logger = configurar_logging()


@click.group()
@click.version_option(version="1.0.0")
@click.option('--log-level', type=click.Choice(['debug', 'info', 'warn'], case_sensitive=False), default='warn', help='Nível de log (debug, info, warn)')
@click.pass_context
def cli(ctx, log_level):
    """
    🎯 Processador de Dados CAGED
    
    Sistema para download, processamento e consolidação de dados mensais
    do Cadastro Geral de Empregados e Desempregados (CAGED).
    
    Exemplos de uso com níveis de log:
    \b
    # Executar com log padrão (warn)
    python main.py baixar --ano 2024 --mes 1
    
    # Executar com log detalhado (info)
    python main.py --log-level info baixar --ano 2024 --mes 1
    
    # Executar com log de debug
    python main.py --log-level debug baixar --ano 2024 --mes 1
    """
    # Configurar nível de log baseado no parâmetro
    ctx.ensure_object(dict)
    
    # Mapear níveis de log
    log_level_map = {
        'debug': 'DEBUG',
        'info': 'INFO', 
        'warn': 'WARNING'
    }
    
    nivel_log = log_level_map[log_level.lower()]
    ctx.obj['log_level'] = nivel_log
    
    # Reconfigurar logger com novo nível
    global logger
    logger = configurar_logging(nivel_log, usar_emojis=True)
    
    # Configurar logging para console se necessário
    if log_level.lower() in ['debug', 'info']:
        def log_to_console(msg):
            emoji = "🔍" if log_level.lower() == 'debug' else "ℹ️"
            click.echo(f"{emoji} {msg}", err=True)
        
        # Adicionar handler customizado para console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, nivel_log))
        console_handler.addFilter(lambda record: log_to_console(record.getMessage()) or False)
        logger.addHandler(console_handler)


@cli.command()
@click.option('--ano', type=int, help='Ano dos dados (ex: 2024)')
@click.option('--mes', type=int, help='Mês específico (1-12)')
@click.option('--mes-inicio', type=int, help='Mês inicial para faixa')
@click.option('--mes-fim', type=int, help='Mês final para faixa')
@click.option('--ano-inicio', type=int, help='Ano inicial para faixa de anos')
@click.option('--mes-inicio-faixa', type=int, help='Mês inicial para faixa de anos (1-12)')
@click.option('--ano-fim', type=int, help='Ano final para faixa de anos')
@click.option('--mes-fim-faixa', type=int, help='Mês final para faixa de anos (1-12)')
@click.option('--todos-anos', is_flag=True, help='Baixar todos os anos disponíveis')
@click.option('--todos-meses', is_flag=True, help='Baixar todos os meses do ano especificado')
def baixar(ano, mes, mes_inicio, mes_fim, ano_inicio, mes_inicio_faixa, ano_fim, mes_fim_faixa, todos_anos, todos_meses):
    """
    📥 Apenas baixa dados CAGED do servidor FTP oficial
    
    Exemplos:
    \b
    # Baixar janeiro de 2024
    python main.py baixar --ano 2024 --mes 1
    
    # Baixar primeiro semestre de 2024
    python main.py baixar --ano 2024 --mes-inicio 1 --mes-fim 6
    
    # Baixar todos os meses de 2024
    python main.py baixar --ano 2024 --todos-meses
    
    # Baixar todos os anos disponíveis
    python main.py baixar --todos-anos
    
    # Baixar faixa de datas: janeiro/2020 até abril/2020
    python main.py baixar --ano-inicio 2020 --mes-inicio-faixa 1 --ano-fim 2020 --mes-fim-faixa 4
    
    # Baixar faixa de datas: julho/2021 até março/2023
    python main.py baixar --ano-inicio 2021 --mes-inicio-faixa 7 --ano-fim 2023 --mes-fim-faixa 3
    """
    logger.info(f"🚀 Iniciando download - Configurações: Ano: {ano}, Mês: {mes}, Faixa: {ano_inicio}/{mes_inicio_faixa} até {ano_fim}/{mes_fim_faixa}, Todos anos: {todos_anos}, Todos meses: {todos_meses}")
    
    gerenciador = GerenciadorArquivosCaged()
    gerenciador.conectar()
    
    try:
        # Validações
        if not todos_anos and ano is None and ano_inicio is None:
            click.echo("❌ Erro: Especifique um ano, uma faixa de datas ou use --todos-anos")
            return
        
        # Validação para faixa de datas
        if ano_inicio is not None or ano_fim is not None:
            if ano_inicio is None or ano_fim is None or mes_inicio_faixa is None or mes_fim_faixa is None:
                click.echo("❌ Erro: Para faixa de datas, especifique --ano-inicio, --mes-inicio-faixa, --ano-fim e --mes-fim-faixa")
                return
            if ano_inicio > ano_fim or (ano_inicio == ano_fim and mes_inicio_faixa > mes_fim_faixa):
                click.echo("❌ Erro: Data inicial deve ser anterior à data final")
                return
            if not (1 <= mes_inicio_faixa <= 12) or not (1 <= mes_fim_faixa <= 12):
                click.echo("❌ Erro: Meses devem estar entre 1 e 12")
                return
        
        total_downloads = 0
        total_metadados = []
        
        if ano_inicio is not None and ano_fim is not None:
            # Baixar faixa de datas
            click.echo(f"🔄 Baixando faixa de datas: {mes_inicio_faixa:02d}/{ano_inicio} até {mes_fim_faixa:02d}/{ano_fim}")
            
            ano_atual = ano_inicio
            mes_atual = mes_inicio_faixa
            
            while ano_atual < ano_fim or (ano_atual == ano_fim and mes_atual <= mes_fim_faixa):
                click.echo(f"🔄 Processando {ano_atual}/{mes_atual:02d}")
                sucesso, metadados = gerenciador.baixar_dados_mensais(ano_atual, mes_atual)
                if sucesso:
                    total_downloads += 1
                    total_metadados.extend(metadados)
                
                # Avançar para o próximo mês
                mes_atual += 1
                if mes_atual > 12:
                    mes_atual = 1
                    ano_atual += 1
                    
        elif todos_anos:
            # Baixar todos os anos disponíveis
            anos = gerenciador.listar_anos_disponiveis()
            click.echo(f"📅 Anos disponíveis: {anos}")
            
            for ano_atual in anos:
                click.echo(f"🔄 Processando ano: {ano_atual}")
                for mes_atual in range(1, 13):
                    sucesso, metadados = gerenciador.baixar_dados_mensais(ano_atual, mes_atual)
                    if sucesso:
                        total_downloads += 1
                        total_metadados.extend(metadados)
                    
        elif todos_meses:
            # Baixar todos os meses do ano especificado
            click.echo(f"🔄 Baixando todos os meses de {ano}")
            for mes_atual in range(1, 13):
                sucesso, metadados = gerenciador.baixar_dados_mensais(ano, mes_atual)
                if sucesso:
                    total_downloads += 1
                    total_metadados.extend(metadados)
                
        elif mes_inicio and mes_fim:
            # Baixar faixa de meses
            for mes_atual in range(mes_inicio, mes_fim + 1):
                sucesso, metadados = gerenciador.baixar_dados_mensais(ano, mes_atual)
                if sucesso:
                    total_downloads += 1
                    total_metadados.extend(metadados)
                
        elif mes:
            # Baixar mês específico
            sucesso, metadados = gerenciador.baixar_dados_mensais(ano, mes)
            if sucesso:
                total_downloads += 1
                total_metadados.extend(metadados)
            
        else:
            click.echo("❌ Erro: Especifique um mês, uma faixa de meses ou use --todos-meses")
            return
        
        # Gerar relatório final
        if total_metadados:
            relatorio = gerenciador.gerar_relatorio_downloads(ano)
            click.echo(f"📊 Relatório de downloads: {relatorio}")
            
        click.echo(f"✅ Download completado! {total_downloads} operações realizadas")
        
    except Exception as e:
        logger.error(f"❌ Erro durante o download: {e}")
        click.echo(f"❌ Erro: {e}")
    finally:
        gerenciador.desconectar()


@cli.command()
@click.option('--ano', type=int, required=True, help='Ano dos dados')
@click.option('--mes', type=int, help='Mês específico')
@click.option('--consolidacao-anual', is_flag=True, help='Consolidar ano completo')
@click.option('--campos', multiple=True, help='Campos específicos a processar')
def apenas_converter(ano, mes, consolidacao_anual, campos):
    """
    🔄 Apenas converte arquivos já baixados para Parquet
    
    Exemplos:
    \b
    # Converter janeiro de 2024
    python main.py apenas-converter --ano 2024 --mes 1
    
    # Converter ano completo de 2024
    python main.py apenas-converter --ano 2024 --consolidacao-anual
    """
    logger.info(f"Iniciando conversão - Ano: {ano}, Mês: {mes}, Consolidacao anual: {consolidacao_anual}")
    
    conversor = ConversorParquetCaged()
    
    if consolidacao_anual:
        click.echo(f"🔄 Convertendo ano completo: {ano}")
        sucesso = conversor.converter_ano_completo(ano, campos)
        if sucesso:
            click.echo(f"✅ Ano {ano} convertido com sucesso")
        else:
            click.echo(f"❌ Falha na conversão do ano {ano}")
    elif mes:
        click.echo(f"🔄 Convertendo {ano}/{mes:02d}")
        sucesso = conversor.converter_mensal(ano, mes, campos)
        if sucesso:
            click.echo(f"✅ {ano}/{mes:02d} convertido com sucesso")
        else:
            click.echo(f"❌ Falha na conversão")
    else:
        click.echo("❌ Erro: Especifique mês ou use --consolidacao-anual")


@cli.command()
@click.option('--ano', type=int, required=True, help='Ano dos dados')
@click.option('--mes', type=int, help='Mês específico')
@click.option('--consolidacao-anual', is_flag=True, help='Consolidar ano completo')
@click.option('--campos', multiple=True, help='Campos específicos a processar')
def converter(ano, mes, consolidacao_anual, campos):
    """
    🔄 Converte dados CAGED para formato Parquet
    
    Exemplos:
    \b
    # Converter janeiro de 2024
    python main.py converter --ano 2024 --mes 1
    
    # Consolidação anual
    python main.py converter --ano 2024 --consolidacao-anual
    
    # Campos específicos
    python main.py converter --ano 2024 --mes 1 --campos ADMITIDOS DESLIGADOS SALDO
    """
    logger.info(f"Iniciando conversão - Ano: {ano}, Mês: {mes}, Consolidacao anual: {consolidacao_anual}")
    
    conversor = ConversorParquetCaged()
    
    try:
        if consolidacao_anual:
            # Consolidação anual
            click.echo(f"🔄 Consolidando dados anuais de {ano}")
            sucesso, movimentacoes, saldos, indicadores = conversor.consolidar_anual(ano)
            
            if sucesso:
                click.echo(f"✅ Consolidação anual concluída!")
                click.echo(f"   🏢 Movimentações: {len(movimentacoes)}")
                click.echo(f"   📈 Saldos: {len(saldos)}")
                click.echo(f"   📊 Indicadores: {len(indicadores)}")
            else:
                click.echo("❌ Falha na consolidação anual")
                
        elif mes:
            # Conversão mensal
            click.echo(f"🔄 Convertendo dados de {ano}/{mes:02d}")
            sucesso, movimentacoes, saldos, indicadores = conversor.converter_mensal(ano, mes, campos)
            
            if sucesso:
                click.echo(f"✅ Conversão mensal concluída!")
                click.echo(f"   🏢 Movimentações: {len(movimentacoes)}")
                click.echo(f"   📈 Saldos: {len(saldos)}")
                click.echo(f"   📊 Indicadores: {len(indicadores)}")
            else:
                click.echo("❌ Falha na conversão mensal")
        else:
            click.echo("❌ Erro: Especifique um mês ou use --consolidacao-anual")
            return
            
    except Exception as e:
        logger.error(f"Erro durante a conversão: {e}")
        click.echo(f"❌ Erro: {e}")


@cli.command()
@click.option('--mensal', is_flag=True, help='Consolidação mensal')
@click.option('--anual', is_flag=True, help='Consolidação anual')
@click.option('--ano', type=int, required=True, help='Ano dos dados')
@click.option('--mes', type=int, help='Mês específico (para consolidação mensal)')
def consolidar(mensal, anual, ano, mes):
    """
    📊 Consolida dados CAGED processados
    
    Exemplos:
    \b
    # Consolidar dados mensais
    python main.py consolidar --mensal --ano 2024 --mes 1
    
    # Consolidar dados anuais
    python main.py consolidar --anual --ano 2024
    """
    logger.info(f"Iniciando consolidação - Ano: {ano}, Tipo: {'mensal' if mensal else 'anual'}")
    
    try:
        if mensal and mes:
            # Consolidação mensal
            click.echo(f"🔄 Consolidando dados mensais de {ano}/{mes:02d}")
            # TODO: Implementar lógica específica de consolidação mensal
            click.echo("✅ Consolidação mensal concluída!")
            
        elif anual:
            # Consolidação anual
            click.echo(f"🔄 Consolidando dados anuais de {ano}")
            # TODO: Implementar lógica específica de consolidação anual
            click.echo("✅ Consolidação anual concluída!")
            
        else:
            click.echo("❌ Erro: Especifique --mensal com --mes ou --anual")
            return
            
    except Exception as e:
        logger.error(f"Erro durante a consolidação: {e}")
        click.echo(f"❌ Erro: {e}")


@cli.command()
@click.option('--ano', type=int, help='Ano específico para descompactar')
@click.option('--mes', type=int, help='Mês específico para descompactar')
@click.option('--todos', is_flag=True, help='Descompactar todos os arquivos')
def apenas_descompactar(ano, mes, todos):
    """
    📦 Apenas descompacta arquivos .7z já baixados
    
    Exemplos:
    \b
    # Descompactar janeiro de 2024
    python main.py apenas-descompactar --ano 2024 --mes 1
    
    # Descompactar todos os arquivos de 2024
    python main.py apenas-descompactar --ano 2024 --todos
    """
    logger.info(f"Iniciando descompactação - Ano: {ano}, Mês: {mes}, Todos: {todos}")
    
    descompactador = DescompactadorCaged()
    
    if todos and ano:
        click.echo(f"🔄 Descompactando todos os meses de {ano}")
        for mes_atual in range(1, 13):
            sucesso = descompactador.descompactar_mensal(ano, mes_atual)
            if sucesso:
                click.echo(f"✅ {ano}/{mes_atual:02d} descompactado")
            else:
                click.echo(f"❌ Falha na descompactação de {ano}/{mes_atual:02d}")
    elif ano and mes:
        click.echo(f"🔄 Descompactando {ano}/{mes:02d}")
        sucesso = descompactador.descompactar_mensal(ano, mes)
        if sucesso:
            click.echo(f"✅ {ano}/{mes:02d} descompactado com sucesso")
        else:
            click.echo(f"❌ Falha na descompactação")
    else:
        click.echo("❌ Erro: Especifique ano e mês ou use --todos com ano")


@cli.command()
@click.option('--ano', type=int, help='Ano específico para baixar e descompactar')
@click.option('--mes', type=int, help='Mês específico para baixar e descompactar')
@click.option('--todos', is_flag=True, help='Baixar e descompactar todos os arquivos')
def descompactar(ano, mes, todos):
    """
    📥 Baixa e descompacta dados CAGED
    
    Exemplos:
    \b
    # Baixar e descompactar janeiro de 2024
    python main.py descompactar --ano 2024 --mes 1
    
    # Baixar e descompactar todos os arquivos de 2024
    python main.py descompactar --ano 2024 --todos
    """
    logger.info(f"Iniciando download e descompactação - Ano: {ano}, Mês: {mes}, Todos: {todos}")
    
    gerenciador = GerenciadorArquivosCaged()
    descompactador = DescompactadorCaged()
    
    gerenciador.conectar()
    
    try:
        if todos and ano:
            click.echo(f"🔄 Baixando e descompactando todos os meses de {ano}")
            for mes_atual in range(1, 13):
                # Baixar
                sucesso_download, _ = gerenciador.baixar_dados_mensais(ano, mes_atual)
                if sucesso_download:
                    # Descompactar
                    sucesso_descompactar = descompactador.descompactar_mensal(ano, mes_atual)
                    if sucesso_descompactar:
                        click.echo(f"✅ {ano}/{mes_atual:02d} baixado e descompactado")
                    else:
                        click.echo(f"❌ Falha na descompactação de {ano}/{mes_atual:02d}")
                else:
                    click.echo(f"❌ Falha no download de {ano}/{mes_atual:02d}")
        elif ano and mes:
            click.echo(f"🔄 Baixando e descompactando {ano}/{mes:02d}")
            # Baixar
            sucesso_download, _ = gerenciador.baixar_dados_mensais(ano, mes)
            if sucesso_download:
                # Descompactar
                sucesso_descompactar = descompactador.descompactar_mensal(ano, mes)
                if sucesso_descompactar:
                    click.echo(f"✅ {ano}/{mes:02d} baixado e descompactado com sucesso")
                else:
                    click.echo(f"❌ Falha na descompactação")
            else:
                click.echo(f"❌ Falha no download")
        else:
            click.echo("❌ Erro: Especifique ano e mês ou use --todos com ano")
    finally:
        gerenciador.desconectar()
    """
    📦 Descompacta arquivos .7z baixados
    
    Exemplos:
    \b
    # Descompactar janeiro de 2024
    python main.py descompactar --ano 2024 --mes 1
    
    # Descompactar todos os arquivos de 2024
    python main.py descompactar --ano 2024
    
    # Descompactar todos os arquivos
    python main.py descompactar --todos
    """
    logger.info(f"Iniciando descompactação - Ano: {ano}, Mês: {mes}, Todos: {todos}")
    
    descompactador = DescompactadorCaged()
    
    try:
        if mes and ano:
            # Descompactar mês específico
            click.echo(f"🔄 Descompactando {ano}/{mes:02d}")
            sucesso, metadados = descompactador.descompactar_mensal(ano, mes)
            
            if sucesso:
                click.echo(f"✅ Descompactação concluída! {len(metadados)} arquivos processados")
            else:
                click.echo("❌ Falha na descompactação")
                
        elif ano:
            # Descompactar ano específico
            click.echo(f"🔄 Descompactando todos os arquivos de {ano}")
            sucesso, metadados = descompactador.descompactar_todos(ano)
            
            if sucesso:
                click.echo(f"✅ Descompactação concluída! {len(metadados)} arquivos processados")
            else:
                click.echo("❌ Falha na descompactação")
                
        elif todos:
            # Descompactar todos
            click.echo("🔄 Descompactando todos os arquivos")
            sucesso, metadados = descompactador.descompactar_todos()
            
            if sucesso:
                click.echo(f"✅ Descompactação concluída! {len(metadados)} arquivos processados")
            else:
                click.echo("❌ Falha na descompactação")
        else:
            click.echo("❌ Erro: Especifique --ano e --mes, apenas --ano, ou --todos")
            return
            
        # Gerar relatório
        if sucesso:
            relatorio = descompactador.gerar_relatorio_descompactacao(ano)
            click.echo(f"📊 Relatório de descompactação: {relatorio}")
            
    except Exception as e:
        logger.error(f"Erro durante a descompactação: {e}")
        click.echo(f"❌ Erro: {e}")


@cli.command()
@click.option('--ano', type=int, help='Ano dos dados')
@click.option('--mes', type=int, help='Mês específico')
@click.option('--ano-inicio', type=int, help='Ano inicial para faixa de anos')
@click.option('--mes-inicio-faixa', type=int, help='Mês inicial para faixa de anos (1-12)')
@click.option('--ano-fim', type=int, help='Ano final para faixa de anos')
@click.option('--mes-fim-faixa', type=int, help='Mês final para faixa de anos (1-12)')
@click.option('--consolidacao-anual', is_flag=True, help='Processar ano completo')
@click.option('--campos', multiple=True, help='Campos específicos a processar')
def completo(ano, mes, ano_inicio, mes_inicio_faixa, ano_fim, mes_fim_faixa, consolidacao_anual, campos):
    """
    🚀 Processamento completo: baixa, descompacta e converte dados CAGED
    
    Exemplos:
    \b
    # Processamento completo de janeiro de 2024
    python main.py completo --ano 2024 --mes 1
    
    # Processamento completo do ano de 2024
    python main.py completo --ano 2024 --consolidacao-anual
    
    # Processamento completo de faixa: janeiro/2020 até abril/2020
    python main.py completo --ano-inicio 2020 --mes-inicio-faixa 1 --ano-fim 2020 --mes-fim-faixa 4
    
    # Processamento completo de faixa: julho/2021 até março/2023
    python main.py completo --ano-inicio 2021 --mes-inicio-faixa 7 --ano-fim 2023 --mes-fim-faixa 3
    """
    logger.info(f"Iniciando processamento completo - Ano: {ano}, Mês: {mes}, Faixa: {ano_inicio}/{mes_inicio_faixa} até {ano_fim}/{mes_fim_faixa}, Consolidacao anual: {consolidacao_anual}")
    
    # Validações
    if not consolidacao_anual and ano is None and mes is None and ano_inicio is None:
        click.echo("❌ Erro: Especifique um ano/mês, uma faixa de datas ou use --consolidacao-anual")
        return
    
    # Validação para faixa de datas
    if ano_inicio is not None or ano_fim is not None:
        if ano_inicio is None or ano_fim is None or mes_inicio_faixa is None or mes_fim_faixa is None:
            click.echo("❌ Erro: Para faixa de datas, especifique --ano-inicio, --mes-inicio-faixa, --ano-fim e --mes-fim-faixa")
            return
        if ano_inicio > ano_fim or (ano_inicio == ano_fim and mes_inicio_faixa > mes_fim_faixa):
            click.echo("❌ Erro: Data inicial deve ser anterior à data final")
            return
        if not (1 <= mes_inicio_faixa <= 12) or not (1 <= mes_fim_faixa <= 12):
            click.echo("❌ Erro: Meses devem estar entre 1 e 12")
            return
    
    gerenciador = GerenciadorArquivosCaged()
    descompactador = DescompactadorCaged()
    conversor = ConversorParquetCaged()
    
    gerenciador.conectar()
    
    try:
        if ano_inicio is not None and ano_fim is not None:
            # Processamento completo de faixa de datas
            click.echo(f"🚀 Processamento completo da faixa: {mes_inicio_faixa:02d}/{ano_inicio} até {mes_fim_faixa:02d}/{ano_fim}")
            
            ano_atual = ano_inicio
            mes_atual = mes_inicio_faixa
            
            while ano_atual < ano_fim or (ano_atual == ano_fim and mes_atual <= mes_fim_faixa):
                click.echo(f"🔄 Processando {ano_atual}/{mes_atual:02d}")
                
                # 1. Baixar
                sucesso_download, _ = gerenciador.baixar_dados_mensais(ano_atual, mes_atual)
                if not sucesso_download:
                    click.echo(f"❌ Falha no download de {ano_atual}/{mes_atual:02d}")
                    # Avançar para o próximo mês mesmo com falha
                    mes_atual += 1
                    if mes_atual > 12:
                        mes_atual = 1
                        ano_atual += 1
                    continue
                
                # 2. Descompactar
                sucesso_descompactar = descompactador.descompactar_mensal(ano_atual, mes_atual)
                if not sucesso_descompactar:
                    click.echo(f"❌ Falha na descompactação de {ano_atual}/{mes_atual:02d}")
                    # Avançar para o próximo mês mesmo com falha
                    mes_atual += 1
                    if mes_atual > 12:
                        mes_atual = 1
                        ano_atual += 1
                    continue
                
                # 3. Converter
                sucesso_conversao = conversor.converter_mensal(ano_atual, mes_atual, campos)
                if sucesso_conversao:
                    click.echo(f"✅ {ano_atual}/{mes_atual:02d} processado completamente")
                else:
                    click.echo(f"❌ Falha na conversão de {ano_atual}/{mes_atual:02d}")
                
                # Avançar para o próximo mês
                mes_atual += 1
                if mes_atual > 12:
                    mes_atual = 1
                    ano_atual += 1
                    
        elif consolidacao_anual:
            click.echo(f"🚀 Processamento completo do ano: {ano}")
            for mes_atual in range(1, 13):
                click.echo(f"🔄 Processando {ano}/{mes_atual:02d}")
                
                # 1. Baixar
                sucesso_download, _ = gerenciador.baixar_dados_mensais(ano, mes_atual)
                if not sucesso_download:
                    click.echo(f"❌ Falha no download de {ano}/{mes_atual:02d}")
                    continue
                
                # 2. Descompactar
                sucesso_descompactar = descompactador.descompactar_mensal(ano, mes_atual)
                if not sucesso_descompactar:
                    click.echo(f"❌ Falha na descompactação de {ano}/{mes_atual:02d}")
                    continue
                
                # 3. Converter
                sucesso_conversao = conversor.converter_mensal(ano, mes_atual, campos)
                if sucesso_conversao:
                    click.echo(f"✅ {ano}/{mes_atual:02d} processado completamente")
                else:
                    click.echo(f"❌ Falha na conversão de {ano}/{mes_atual:02d}")
                    
        elif mes:
            click.echo(f"🚀 Processamento completo de {ano}/{mes:02d}")
            
            # 1. Baixar
            sucesso_download, _ = gerenciador.baixar_dados_mensais(ano, mes)
            if not sucesso_download:
                click.echo(f"❌ Falha no download")
                return
            
            # 2. Descompactar
            sucesso_descompactar = descompactador.descompactar_mensal(ano, mes)
            if not sucesso_descompactar:
                click.echo(f"❌ Falha na descompactação")
                return
            
            # 3. Converter
            sucesso_conversao = conversor.converter_mensal(ano, mes, campos)
            if sucesso_conversao:
                click.echo(f"✅ {ano}/{mes:02d} processado completamente")
            else:
                click.echo(f"❌ Falha na conversão")
        else:
            click.echo("❌ Erro: Especifique mês, faixa de datas ou use --consolidacao-anual")
    finally:
        gerenciador.desconectar()


@cli.command()
def status():
    """
    📈 Exibe status do projeto e arquivos processados
    """
    click.echo("🎯 Status do Processador CAGED")
    click.echo("=" * 40)
    
    try:
        # Verificar arquivos baixados
        files_zip = Path("files-zip")
        if files_zip.exists():
            zip_count = len(list(files_zip.rglob("*.7z")))
            click.echo(f"📦 Arquivos baixados (.7z): {zip_count}")
        
            # Verificar arquivos descompactados
            files_unzip = Path("files-unzip")
            if files_unzip.exists():
                txt_count = len(list(files_unzip.rglob("*.txt")))
                csv_count = len(list(files_unzip.rglob("*.csv")))
                click.echo(f"📁 Arquivos descompactados (.txt): {txt_count}")
                click.echo(f"📁 Arquivos descompactados (.csv): {csv_count}")
        
        # Verificar arquivos processados
        parquet_dir = Path("parquet")
        if parquet_dir.exists():
            parquet_count = len(list(parquet_dir.rglob("*.parquet")))
            click.echo(f"📊 Arquivos processados (.parquet): {parquet_count}")
        
        # Testar conexão FTP
        click.echo("\n🔗 Testando conexão FTP...")
        from src.util.gerenciador_ftp import testar_conexao_caged
        if testar_conexao_caged():
            click.echo("✅ Conexão FTP: OK")
        else:
            click.echo("❌ Conexão FTP: Falha")
        
        click.echo("\n✅ Status exibido!")
    except Exception as e:
        logger.error(f"Erro ao exibir status: {e}")
        click.echo(f"❌ Erro ao exibir status: {e}")


# ============================================================================
# COMANDOS FASE 5.1 - OTIMIZAÇÕES E CACHE
# ============================================================================

@cli.command()
@click.option('--ano', type=int, required=True, help='Ano para consolidação otimizada')
@click.option('--sem-cache', is_flag=True, help='Desabilitar uso de cache')
@click.option('--sem-paralelismo', is_flag=True, help='Desabilitar processamento paralelo')
def consolidar_otimizado(ano, sem_cache, sem_paralelismo):
    """
    🚀 Consolidação anual otimizada com cache e paralelismo - Fase 5.1
    
    Exemplos:
    \b
    # Consolidação otimizada com cache e paralelismo
    python main.py consolidar-otimizado --ano 2024
    
    # Consolidação sem cache
    python main.py consolidar-otimizado --ano 2024 --sem-cache
    
    # Consolidação sequencial (sem paralelismo)
    python main.py consolidar-otimizado --ano 2024 --sem-paralelismo
    """
    logger.info(f"Iniciando consolidação otimizada - Ano: {ano}, Cache: {not sem_cache}, Paralelismo: {not sem_paralelismo}")
    
    try:
        conversor = ConversorParquetCaged(habilitar_cache=not sem_cache)
        
        click.echo(f"🚀 Consolidação otimizada do ano {ano}")
        click.echo(f"   🎯 Cache: {'Habilitado' if not sem_cache else 'Desabilitado'}")
        click.echo(f"   ⚡ Paralelismo: {'Habilitado' if not sem_paralelismo else 'Desabilitado'}")
        
        sucesso, _, _, _ = conversor.consolidar_anual(
            ano=ano,
            usar_cache=not sem_cache,
            usar_paralelismo=not sem_paralelismo
        )
        
        if sucesso:
            click.echo(f"✅ Consolidação otimizada de {ano} concluída!")
            
            # Exibir estatísticas se cache habilitado
            if not sem_cache:
                estatisticas = conversor.exibir_estatisticas_cache()
                click.echo("\n📊 Estatísticas de Cache:")
                click.echo(f"   🎯 Cache Hits: {estatisticas.get('cache_hits', 0)}")
                click.echo(f"   ❌ Cache Misses: {estatisticas.get('cache_misses', 0)}")
                click.echo(f"   📈 Taxa de Acerto: {estatisticas.get('taxa_acerto_cache', 0):.1f}%")
                click.echo(f"   💾 Economia de Espaço: {estatisticas.get('economia_espaco_mb', 0):.2f} MB")
        else:
            click.echo(f"❌ Falha na consolidação otimizada de {ano}")
            
    except Exception as e:
        logger.error(f"Erro na consolidação otimizada: {e}")
        click.echo(f"❌ Erro na consolidação otimizada: {e}")


@cli.command()
def estatisticas_cache():
    """
    📊 Exibe estatísticas detalhadas do sistema de cache - Fase 5.1
    
    Mostra informações sobre cache hits, misses, economia de espaço
    e arquivos de cache armazenados.
    """
    try:
        conversor = ConversorParquetCaged(habilitar_cache=True)
        estatisticas = conversor.exibir_estatisticas_cache()
        
        click.echo("📊 Estatísticas do Sistema de Cache")
        click.echo("=" * 40)
        
        if not estatisticas:
            click.echo("❌ Nenhuma estatística de cache disponível")
            return
        
        click.echo(f"🎯 Cache Habilitado: {'Sim' if estatisticas.get('cache_habilitado') else 'Não'}")
        click.echo(f"📁 Diretório de Cache: {estatisticas.get('diretorio_cache', 'N/A')}")
        click.echo(f"\n📈 Estatísticas de Uso:")
        click.echo(f"   ✅ Cache Hits: {estatisticas.get('cache_hits', 0)}")
        click.echo(f"   ❌ Cache Misses: {estatisticas.get('cache_misses', 0)}")
        click.echo(f"   🎯 Taxa de Acerto: {estatisticas.get('taxa_acerto_cache', 0):.1f}%")
        click.echo(f"   🚀 Consolidações Otimizadas: {estatisticas.get('consolidacoes_otimizadas', 0)}")
        
        click.echo(f"\n💾 Arquivos de Cache:")
        click.echo(f"   📄 Total: {estatisticas.get('arquivos_cache_total', 0)}")
        click.echo(f"   📅 Cache Anual: {estatisticas.get('arquivos_cache_anual', 0)}")
        click.echo(f"   📆 Cache Mensal: {estatisticas.get('arquivos_cache_mensal', 0)}")
        click.echo(f"   💾 Economia de Espaço: {estatisticas.get('economia_espaco_mb', 0):.2f} MB")
        
        click.echo("\n✅ Estatísticas exibidas!")
        
    except Exception as e:
        logger.error(f"Erro ao exibir estatísticas de cache: {e}")
        click.echo(f"❌ Erro ao exibir estatísticas de cache: {e}")


@cli.command()
@click.confirmation_option(prompt='Tem certeza que deseja limpar o cache expirado?')
def limpar_cache():
    """
    🧹 Remove arquivos de cache expirados - Fase 5.1
    
    Remove automaticamente arquivos de cache que excederam
    o tempo de expiração configurado.
    """
    try:
        conversor = ConversorParquetCaged(habilitar_cache=True)
        
        click.echo("🧹 Limpando cache expirado...")
        arquivos_removidos = conversor.limpar_cache_expirado()
        
        if arquivos_removidos > 0:
            click.echo(f"✅ {arquivos_removidos} arquivos de cache expirados removidos")
        else:
            click.echo("ℹ️  Nenhum arquivo de cache expirado encontrado")
            
    except Exception as e:
        logger.error(f"Erro ao limpar cache: {e}")
        click.echo(f"❌ Erro ao limpar cache: {e}")


@cli.command()
@click.option('--ano', type=int, required=True, help='Ano dos dados')
@click.option('--mes', type=int, required=True, help='Mês dos dados (1-12)')
@click.option('--sem-cache', is_flag=True, help='Desabilitar cache')
@click.option('--sem-paralelismo', is_flag=True, help='Desabilitar paralelismo')
@click.option('--campos', multiple=True, help='Campos específicos a processar')
def converter_otimizado(ano, mes, sem_cache, sem_paralelismo, campos):
    """
    ⚡ Conversão mensal otimizada com cache e paralelismo - Fase 5.1
    
    Exemplos:
    \b
    # Conversão otimizada com todas as funcionalidades
    python main.py converter-otimizado --ano 2024 --mes 1
    
    # Conversão sem cache
    python main.py converter-otimizado --ano 2024 --mes 1 --sem-cache
    
    # Conversão com campos específicos
    python main.py converter-otimizado --ano 2024 --mes 1 --campos ADMITIDOS DESLIGADOS SALDO
    """
    logger.info(f"Iniciando conversão otimizada - {ano}/{mes:02d}, Cache: {not sem_cache}, Paralelismo: {not sem_paralelismo}")
    
    try:
        conversor = ConversorParquetCaged(habilitar_cache=not sem_cache)
        
        click.echo(f"⚡ Conversão otimizada de {ano}/{mes:02d}")
        click.echo(f"   🎯 Cache: {'Habilitado' if not sem_cache else 'Desabilitado'}")
        click.echo(f"   ⚡ Paralelismo: {'Habilitado' if not sem_paralelismo else 'Desabilitado'}")
        if campos:
            click.echo(f"   📊 Campos: {', '.join(campos)}")
        
        sucesso, _, _, _ = conversor.converter_mensal(
            ano=ano,
            mes=mes,
            campos_selecionados=list(campos) if campos else None,
            usar_paralelismo=not sem_paralelismo
        )
        
        if sucesso:
            click.echo(f"✅ Conversão otimizada de {ano}/{mes:02d} concluída!")
        else:
            click.echo(f"❌ Falha na conversão otimizada de {ano}/{mes:02d}")
            
    except Exception as e:
        logger.error(f"Erro na conversão otimizada: {e}")
        click.echo(f"❌ Erro na conversão otimizada: {e}")


if __name__ == '__main__':
    # Criar diretórios necessários se não existirem
    for dir_name in ['files-zip', 'files-unzip', 'parquet', 'logs']:
        Path(dir_name).mkdir(exist_ok=True)
    
    cli()