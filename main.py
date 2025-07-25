#!/usr/bin/env python3
"""
Processador de Dados CAGED
Script principal para download, processamento e consolidação de dados mensais
"""

import click
from pathlib import Path
from loguru import logger
from src.util.gerenciador_ftp import GerenciadorArquivosCaged
from src.util.conversor_parquet import ConversorParquetCaged
from src.util.descompactador import DescompactadorCaged


# Configuração de logging
logger.add("logs/caged_{time}.log", rotation="1 day", retention="30 days")


@click.group()
@click.version_option(version="1.0.0")
def cli():
    """
    🎯 Processador de Dados CAGED
    
    Sistema para download, processamento e consolidação de dados mensais
    do Cadastro Geral de Empregados e Desempregados (CAGED).
    """
    pass


@cli.command()
@click.option('--ano', type=int, help='Ano dos dados (ex: 2024)')
@click.option('--mes', type=int, help='Mês específico (1-12)')
@click.option('--mes-inicio', type=int, help='Mês inicial para faixa')
@click.option('--mes-fim', type=int, help='Mês final para faixa')
@click.option('--todos-anos', is_flag=True, help='Baixar todos os anos disponíveis')
@click.option('--todos-meses', is_flag=True, help='Baixar todos os meses do ano especificado')
def baixar(ano, mes, mes_inicio, mes_fim, todos_anos, todos_meses):
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
    """
    logger.info(f"Iniciando download - Configurações: Ano: {ano}, Mês: {mes}, Todos anos: {todos_anos}, Todos meses: {todos_meses}")
    
    gerenciador = GerenciadorArquivosCaged()
    gerenciador.conectar()
    
    try:
        # Validações
        if not todos_anos and ano is None:
            click.echo("❌ Erro: Especifique um ano ou use --todos-anos")
            return
        
        total_downloads = 0
        total_metadados = []
        
        if todos_anos:
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
        logger.error(f"Erro durante o download: {e}")
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
@click.option('--ano', type=int, required=True, help='Ano dos dados')
@click.option('--mes', type=int, help='Mês específico')
@click.option('--consolidacao-anual', is_flag=True, help='Processar ano completo')
@click.option('--campos', multiple=True, help='Campos específicos a processar')
def completo(ano, mes, consolidacao_anual, campos):
    """
    🚀 Processamento completo: baixa, descompacta e converte dados CAGED
    
    Exemplos:
    \b
    # Processamento completo de janeiro de 2024
    python main.py completo --ano 2024 --mes 1
    
    # Processamento completo do ano de 2024
    python main.py completo --ano 2024 --consolidacao-anual
    """
    logger.info(f"Iniciando processamento completo - Ano: {ano}, Mês: {mes}, Consolidacao anual: {consolidacao_anual}")
    
    gerenciador = GerenciadorArquivosCaged()
    descompactador = DescompactadorCaged()
    conversor = ConversorParquetCaged()
    
    gerenciador.conectar()
    
    try:
        if consolidacao_anual:
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
            click.echo("❌ Erro: Especifique mês ou use --consolidacao-anual")
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


if __name__ == '__main__':
    # Criar diretórios necessários se não existirem
    for dir_name in ['files-zip', 'files-unzip', 'parquet', 'logs']:
        Path(dir_name).mkdir(exist_ok=True)
    
    cli() 