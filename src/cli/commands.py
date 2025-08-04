#!/usr/bin/env python3
"""
Comandos CLI Unificados do Sistema CAGED
Implementação do Item 1.4 do Plano de Melhorias

Este módulo define os comandos da interface de linha de comando.
"""

import click
import sys
from pathlib import Path
from typing import List, Optional

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.config import ConfigManager
from src.core.pipeline import CAGEDPipeline, ProcessingStage, create_pipeline
from src.core.exceptions import CAGEDException, ValidationError
from src.utils.validators import CAGEDValidator
from src.utils.logger import setup_logger


def configurar_logging(nivel: str = "WARNING", usar_emojis: bool = True, enable_json: bool = False):
    """
    Configura o sistema de logging centralizado para o CAGED
    
    Args:
        nivel: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        usar_emojis: Se deve usar emojis nas mensagens
        enable_json: Se deve habilitar logs estruturados em JSON
    """
    # Configurar níveis específicos por módulo
    module_levels = {
        "ftp": nivel,
        "descompactador": nivel,
        "conversor": nivel,
        "validador": nivel
    }
    
    return setup_logger(
        name="caged",
        level=nivel,
        enable_file=True,
        enable_console=True,
        use_emojis=usar_emojis,
        enable_json=enable_json,
        module_levels=module_levels
    )


@click.group()
@click.option('--config', '-c', help='Arquivo de configuração')
@click.option('--profile', '-p', default='default', help='Profile de configuração')
@click.option('--debug', is_flag=True, help='Modo debug')
@click.pass_context
def cli(ctx, config, profile, debug):
    """
    🏭 Sistema CAGED - Processamento de Dados do Novo CAGED
    
    Sistema unificado para download, extração e conversão de dados do CAGED.
    """
    # Configurar contexto
    ctx.ensure_object(dict)
    ctx.obj['config_file'] = config
    ctx.obj['profile'] = profile
    ctx.obj['debug'] = debug
    
    # Configurar logging
    log_level = "DEBUG" if debug else "INFO"
    logger = configurar_logging(log_level, usar_emojis=True)
    ctx.obj['logger'] = logger
    
    if debug:
        logger.debug("Modo debug ativado")


@cli.command()
@click.option('--ano', type=int, help='Ano dos dados (ex: 2024)')
@click.option('--mes', type=int, help='Mês específico (1-12)')
@click.option('--ano-inicio', type=int, help='Ano inicial para faixa')
@click.option('--mes-inicio', type=int, help='Mês inicial para faixa')
@click.option('--ano-fim', type=int, help='Ano final para faixa')
@click.option('--mes-fim', type=int, help='Mês final para faixa')
@click.option('--todos-meses', is_flag=True, help='Processar todos os meses do ano')
@click.option('--download', is_flag=True, help='Executar etapa de download')
@click.option('--extract', is_flag=True, help='Executar etapa de extração')
@click.option('--convert', is_flag=True, help='Executar etapa de conversão')
@click.option('--skip-download', is_flag=True, help='Pular etapa de download')
@click.option('--skip-extract', is_flag=True, help='Pular etapa de extração')
@click.option('--skip-convert', is_flag=True, help='Pular etapa de conversão')
@click.option('--campos', help='Campos específicos para conversão')
@click.option('--dry-run', is_flag=True, help='Apenas validar, não executar')
@click.option('--workers', type=int, help='Número de workers paralelos')
@click.option('--use-cache', is_flag=True, help='Usar sistema de cache')
@click.pass_context
def processar(ctx, ano, mes, ano_inicio, mes_inicio, ano_fim, mes_fim, todos_meses,
             download, extract, convert, skip_download, skip_extract, skip_convert,
             campos, dry_run, workers, use_cache):
    """
    🔄 Comando Unificado de Processamento
    
    Este comando substitui os 12 comandos anteriores, oferecendo controle
    granular sobre as etapas de processamento.
    
    ETAPAS DE PROCESSAMENTO:
    📥 Download: Baixar arquivos do servidor FTP
    📦 Extract: Descompactar arquivos .7z
    🔄 Convert: Converter para formato Parquet
    
    EXEMPLOS DE USO:
    
    # Processamento completo (todas as etapas)
    python main.py processar --ano 2024 --mes 1
    
    # Apenas download
    python main.py processar --ano 2024 --mes 1 --download
    
    # Download e extração
    python main.py processar --ano 2024 --mes 1 --download --extract
    
    # Pular download, apenas extrair e converter
    python main.py processar --ano 2024 --mes 1 --skip-download --extract --convert
    
    # Processar todos os meses do ano
    python main.py processar --ano 2024 --todos-meses
    
    # Processar faixa de datas
    python main.py processar --ano-inicio 2023 --mes-inicio 6 --ano-fim 2024 --mes-fim 3
    
    # Apenas validação (dry-run)
    python main.py processar --ano 2024 --mes 1 --dry-run
    
    # Com cache e paralelismo
    python main.py processar --ano 2024 --mes 1 --use-cache --workers 8
    """
    logger = ctx.obj['logger']
    
    try:
        # Carregar configuração
        config_manager = ConfigManager(ctx.obj.get('config_file'))
        config = config_manager.load_config(ctx.obj['profile'])
        
        # Sobrescrever configurações com parâmetros da linha de comando
        if workers:
            config.processing.max_workers = workers
        if use_cache is not None:
            config.cache.enabled = use_cache
        
        # Determinar etapas de processamento
        stages = _determine_processing_stages(
            download, extract, convert,
            skip_download, skip_extract, skip_convert
        )
        
        click.echo("📋 Etapas planejadas:")
        for stage in ProcessingStage:
            if stage in stages:
                click.echo(f"   {_get_stage_emoji(stage)} {_get_stage_name(stage)}: ✅")
            else:
                click.echo(f"   {_get_stage_emoji(stage)} {_get_stage_name(stage)}: ⏭️")
        
        # Determinar modo de processamento
        if todos_meses:
            if not ano:
                raise ValidationError("Ano é obrigatório quando --todos-meses é usado")
            click.echo(f"📅 Modo: Todos os meses ({ano})")
            items = _create_year_items(ano, stages)
        elif ano_inicio and mes_inicio and ano_fim and mes_fim:
            click.echo(f"📅 Modo: Faixa de datas ({mes_inicio:02d}/{ano_inicio} - {mes_fim:02d}/{ano_fim})")
            items = _create_range_items(ano_inicio, mes_inicio, ano_fim, mes_fim, stages)
        elif ano and mes:
            click.echo(f"📅 Modo: Mensal ({mes:02d}/{ano})")
            items = _create_single_item(ano, mes, stages)
        else:
            raise ValidationError(
                "Especifique: --ano e --mes, ou --ano e --todos-meses, ou faixa com --ano-inicio/mes-inicio/ano-fim/mes-fim"
            )
        
        # Executar validações centralizadas
        _execute_validations(items, stages, config, logger)
        
        if dry_run:
            click.echo("\n🔍 MODO DRY-RUN - Apenas validação")
            click.echo("✅ Todas as validações passaram!")
            click.echo("💡 Execute sem --dry-run para processar os dados")
            return
        
        # Criar e executar pipeline
        pipeline = create_pipeline(config)
        
        # Registrar handlers (implementação futura)
        # _register_pipeline_handlers(pipeline, config)
        
        # Executar processamento
        click.echo(f"\n🚀 Iniciando processamento de {len(items)} item(s)...")
        
        # Por enquanto, apenas simular o processamento
        click.echo("⚠️ Pipeline em desenvolvimento - simulando processamento...")
        
        for i, item in enumerate(items, 1):
            click.echo(f"📊 Processando {i}/{len(items)}: {item.id}")
            # Aqui seria executado: results = await pipeline.process_items([item])
        
        click.echo("\n✅ Processamento concluído com sucesso!")
        
    except CAGEDException as e:
        logger.error(f"Erro CAGED: {e}")
        click.echo(f"❌ Erro: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        click.echo(f"💥 Erro inesperado: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--profile', default='default', help='Profile de configuração')
@click.pass_context
def config_create(ctx, profile):
    """
    📝 Criar arquivo de configuração padrão
    """
    try:
        config_manager = ConfigManager(ctx.obj.get('config_file'))
        config_manager.create_default_config_file(profile)
        
        config_file = config_manager.config_file
        click.echo(f"✅ Arquivo de configuração criado: {config_file}")
        click.echo(f"📋 Profile: {profile}")
        
    except Exception as e:
        click.echo(f"❌ Erro ao criar configuração: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def config_show(ctx):
    """
    👁️ Mostrar configuração atual
    """
    try:
        config_manager = ConfigManager(ctx.obj.get('config_file'))
        config = config_manager.load_config(ctx.obj['profile'])
        
        click.echo(f"📋 Configuração atual (profile: {ctx.obj['profile']}):")
        click.echo(f"\n🌐 FTP:")
        click.echo(f"  Servidor: {config.ftp.server}")
        click.echo(f"  Diretório: {config.ftp.directory}")
        click.echo(f"  Timeout: {config.ftp.timeout}s")
        
        click.echo(f"\n💾 Cache:")
        click.echo(f"  Habilitado: {config.cache.enabled}")
        click.echo(f"  Diretório: {config.cache.directory}")
        click.echo(f"  Tamanho máximo: {config.cache.max_size_gb}GB")
        
        click.echo(f"\n⚙️ Processamento:")
        click.echo(f"  Workers: {config.processing.max_workers}")
        click.echo(f"  Paralelo: {config.processing.enable_parallel}")
        click.echo(f"  Limite de memória: {config.processing.memory_limit_gb}GB")
        
        click.echo(f"\n📤 Saída:")
        click.echo(f"  Formato: {config.output.format}")
        click.echo(f"  Compressão: {config.output.compression}")
        click.echo(f"  Diretório: {config.output.directory}")
        
    except Exception as e:
        click.echo(f"❌ Erro ao carregar configuração: {e}", err=True)
        sys.exit(1)


def _determine_processing_stages(download, extract, convert,
                               skip_download, skip_extract, skip_convert) -> List[ProcessingStage]:
    """Determina quais estágios executar baseado nos flags"""
    stages = []
    
    # Se nenhum flag específico foi usado, executar todas as etapas
    if not any([download, extract, convert, skip_download, skip_extract, skip_convert]):
        return [ProcessingStage.DOWNLOAD, ProcessingStage.EXTRACT, ProcessingStage.CONVERT]
    
    # Determinar etapas baseado nos flags
    if download or (not skip_download and not any([extract, convert])):
        stages.append(ProcessingStage.DOWNLOAD)
    
    if extract or (not skip_extract and not any([download, convert])):
        stages.append(ProcessingStage.EXTRACT)
    
    if convert or (not skip_convert and not any([download, extract])):
        stages.append(ProcessingStage.CONVERT)
    
    # Se apenas flags skip foram usados, incluir etapas não puladas
    if any([skip_download, skip_extract, skip_convert]) and not any([download, extract, convert]):
        all_stages = [ProcessingStage.DOWNLOAD, ProcessingStage.EXTRACT, ProcessingStage.CONVERT]
        stages = []
        
        if not skip_download:
            stages.append(ProcessingStage.DOWNLOAD)
        if not skip_extract:
            stages.append(ProcessingStage.EXTRACT)
        if not skip_convert:
            stages.append(ProcessingStage.CONVERT)
    
    return stages


def _create_single_item(ano: int, mes: int, stages: List[ProcessingStage]):
    """Cria item único de processamento"""
    pipeline = create_pipeline()
    return [pipeline.create_processing_item(ano, mes, stages)]


def _create_year_items(ano: int, stages: List[ProcessingStage]):
    """Cria itens para todos os meses do ano"""
    pipeline = create_pipeline()
    return pipeline.create_date_range_items(ano, 1, ano, 12, stages)


def _create_range_items(ano_inicio: int, mes_inicio: int, ano_fim: int, mes_fim: int, stages: List[ProcessingStage]):
    """Cria itens para faixa de datas"""
    pipeline = create_pipeline()
    return pipeline.create_date_range_items(ano_inicio, mes_inicio, ano_fim, mes_fim, stages)


def _execute_validations(items, stages, config, logger):
    """Executa validações centralizadas"""
    validator = CAGEDValidator()
    
    # Validar cada item
    for item in items:
        # Validar ano/mês
        validator.validar_ano_mes(item.ano, item.mes)
    
    # Validar faixa se múltiplos itens
    if len(items) > 1:
        first_item = items[0]
        last_item = items[-1]
        validator.validar_faixa_datas(
            first_item.ano, first_item.mes,
            last_item.ano, last_item.mes
        )
    
    # Validar espaço em disco
    validator.validar_espaco_disco()
    
    # Validar conectividade FTP se download estiver habilitado
    if ProcessingStage.DOWNLOAD in stages:
        click.echo("🔍 Validando conectividade FTP...")
        try:
            validator.validar_conectividade_ftp()
            click.echo(f"✅ Conectividade FTP OK: {config.ftp.server}")
        except Exception as e:
            logger.warning(f"Falha na conectividade FTP: {e}")
            if click.confirm("❓ Continuar sem download?"):
                # Remover download dos stages
                for item in items:
                    if ProcessingStage.DOWNLOAD in item.stages:
                        item.stages.remove(ProcessingStage.DOWNLOAD)
                click.echo("⚠️ Download desabilitado")
            else:
                raise ValidationError(f"Conectividade FTP falhou: {e}")


def _get_stage_emoji(stage: ProcessingStage) -> str:
    """Retorna emoji para o estágio"""
    emojis = {
        ProcessingStage.DOWNLOAD: "📥",
        ProcessingStage.EXTRACT: "📦",
        ProcessingStage.CONVERT: "🔄",
        ProcessingStage.VALIDATE: "✅",
        ProcessingStage.CLEANUP: "🧹"
    }
    return emojis.get(stage, "⚙️")


def _get_stage_name(stage: ProcessingStage) -> str:
    """Retorna nome amigável para o estágio"""
    names = {
        ProcessingStage.DOWNLOAD: "Download",
        ProcessingStage.EXTRACT: "Extract",
        ProcessingStage.CONVERT: "Convert",
        ProcessingStage.VALIDATE: "Validate",
        ProcessingStage.CLEANUP: "Cleanup"
    }
    return names.get(stage, stage.value.title())


if __name__ == '__main__':
    cli()