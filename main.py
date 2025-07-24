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
@click.option('--ano', type=int, required=True, help='Ano dos dados (ex: 2024)')
@click.option('--mes', type=int, help='Mês específico (1-12)')
@click.option('--mes-inicio', type=int, help='Mês inicial para faixa')
@click.option('--mes-fim', type=int, help='Mês final para faixa')
@click.option('--ufs', multiple=True, help='UFs específicas (ex: SP RJ MG)')
def baixar(ano, mes, mes_inicio, mes_fim, ufs):
    """
    📥 Baixa dados CAGED do servidor FTP oficial
    
    Exemplos:
    \b
    # Baixar janeiro de 2024
    python main.py baixar --ano 2024 --mes 1
    
    # Baixar primeiro semestre de 2024
    python main.py baixar --ano 2024 --mes-inicio 1 --mes-fim 6
    
    # Baixar dados de UFs específicas
    python main.py baixar --ano 2024 --mes 1 --ufs SP RJ MG
    """
    # TODO: Implementar comando de download
    logger.info(f"Iniciando download - Ano: {ano}, Mês: {mes}")
    
    gerenciador = GerenciadorArquivosCaged()
    # Implementar lógica de download
    
    click.echo("✅ Download completado!")


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
    # TODO: Implementar comando de conversão
    logger.info(f"Iniciando conversão - Ano: {ano}, Mês: {mes}")
    
    conversor = ConversorParquetCaged()
    # Implementar lógica de conversão
    
    click.echo("✅ Conversão completada!")


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
    # TODO: Implementar comando de consolidação
    logger.info(f"Iniciando consolidação - Ano: {ano}, Tipo: {'mensal' if mensal else 'anual'}")
    
    click.echo("✅ Consolidação completada!")


@cli.command()
def status():
    """
    📈 Exibe status do projeto e arquivos processados
    """
    # TODO: Implementar comando de status
    click.echo("🎯 Status do Processador CAGED")
    click.echo("=" * 40)
    
    # Verificar arquivos baixados
    files_zip = Path("files-zip")
    if files_zip.exists():
        zip_count = len(list(files_zip.rglob("*.7z")))
        click.echo(f"📦 Arquivos baixados: {zip_count}")
    
    # Verificar arquivos processados
    parquet_dir = Path("parquet")
    if parquet_dir.exists():
        parquet_count = len(list(parquet_dir.rglob("*.parquet")))
        click.echo(f"📊 Arquivos processados: {parquet_count}")
    
    click.echo("✅ Status exibido!")


if __name__ == '__main__':
    # Criar diretórios necessários se não existirem
    for dir_name in ['files-zip', 'files-unzip', 'parquet', 'logs']:
        Path(dir_name).mkdir(exist_ok=True)
    
    cli() 