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
        
        # Definir variáveis para uso posterior
        use_cache = config.cache.enabled if use_cache is None else use_cache
        
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
        
        # Mostrar estimativa de tempo
        estimated_time = _estimate_processing_time(items, stages, config)
        click.echo(f"\n⏱️ Tempo estimado: {estimated_time}")
        
        if dry_run:
            click.echo("\n🔍 MODO DRY-RUN - Apenas validação")
            click.echo("✅ Todas as validações passaram!")
            
            # Mostrar detalhes do que seria processado
            click.echo(f"\n📋 RESUMO DO QUE SERIA PROCESSADO:")
            click.echo(f"   📅 Período: {items[0].id} a {items[-1].id}")
            click.echo(f"   📊 Total de itens: {len(items)}")
            click.echo(f"   ⚙️ Etapas: {', '.join([_get_stage_name(s) for s in stages])}")
            click.echo(f"   ⏱️ Tempo estimado: {estimated_time}")
            
            if use_cache:
                click.echo(f"   💾 Cache: Habilitado")
            if workers and workers > 1:
                click.echo(f"   ⚡ Workers: {workers} (paralelo)")
            
            click.echo("\n💡 Execute sem --dry-run para processar os dados")
            return
        
        # Criar e executar pipeline
        pipeline = create_pipeline(config)
        
        # Registrar handlers (implementação futura)
        # _register_pipeline_handlers(pipeline, config)
        
        # Executar processamento
        click.echo(f"\n🚀 Iniciando processamento de {len(items)} item(s)...")
        
        # Executar pipeline real
        try:
            # Configurar callbacks de progresso
            progress_bar = None
            
            def progress_callback(item, stage, progress):
                nonlocal progress_bar
                if progress_bar is None:
                    progress_bar = click.progressbar(length=100, label='Processando')
                    progress_bar.__enter__()
                
                # Atualizar barra de progresso
                current_progress = int(progress * 100)
                progress_bar.update(current_progress - progress_bar.pos)
                
                # Log detalhado se debug
                if ctx.obj['debug']:
                    stage_name = stage.value if stage else "geral"
                    click.echo(f"\n🔄 {item.id} - {stage_name}: {progress:.1%}")
            
            def error_callback(item, error):
                click.echo(f"\n❌ Erro em {item.id}: {error}", err=True)
            
            # Configurar pipeline com callbacks
            pipeline.set_progress_callback(progress_callback)
            pipeline.set_error_callback(error_callback)
            
            # Executar processamento
            import asyncio
            
            # Determinar se usar processamento paralelo
            use_parallel = (
                config.processing.enable_parallel and 
                len(items) > 1 and 
                workers and workers > 1
            )
            
            if use_parallel:
                from src.core.pipeline import create_parallel_pipeline
                parallel_pipeline = create_parallel_pipeline(config)
                parallel_pipeline.set_progress_callback(progress_callback)
                parallel_pipeline.set_error_callback(error_callback)
                
                click.echo(f"⚡ Usando processamento paralelo ({workers} workers)")
                results = asyncio.run(
                    parallel_pipeline.process_items_parallel(
                        items, 
                        enable_throttling=True
                    )
                )
            else:
                click.echo("🔄 Usando processamento sequencial")
                results = asyncio.run(pipeline.process_items(items, parallel=False))
            
            # Fechar barra de progresso
            if progress_bar:
                progress_bar.__exit__(None, None, None)
                click.echo()  # Nova linha
            
            # Exibir resultados
            _display_processing_results(results, logger)
            
        except Exception as e:
            if progress_bar:
                progress_bar.__exit__(None, None, None)
            raise
        
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
        click.echo(f"  Max retries: {config.ftp.max_retries}")
        
        click.echo(f"\n💾 Cache:")
        click.echo(f"  Habilitado: {config.cache.enabled}")
        click.echo(f"  Diretório: {config.cache.directory}")
        click.echo(f"  Tamanho máximo: {config.cache.max_size_gb}GB")
        click.echo(f"  Expiração: {config.cache.expiry_days} dias")
        
        click.echo(f"\n⚙️ Processamento:")
        click.echo(f"  Workers: {config.processing.max_workers}")
        click.echo(f"  Chunk size: {config.processing.chunk_size:,}")
        click.echo(f"  Paralelo: {config.processing.enable_parallel}")
        click.echo(f"  Limite de memória: {config.processing.memory_limit_gb}GB")
        
        click.echo(f"\n📤 Saída:")
        click.echo(f"  Formato: {config.output.format}")
        click.echo(f"  Compressão: {config.output.compression}")
        click.echo(f"  Diretório: {config.output.directory}")
        
        click.echo(f"\n📝 Logging:")
        click.echo(f"  Nível: {config.logging.level}")
        click.echo(f"  Arquivo: {config.logging.enable_file}")
        click.echo(f"  Console: {config.logging.enable_console}")
        click.echo(f"  Emojis: {config.logging.use_emojis}")
        
        click.echo(f"\n🐛 Debug: {config.debug_mode}")
        
    except Exception as e:
        click.echo(f"❌ Erro ao carregar configuração: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--profile', default='default', help='Profile de configuração')
@click.pass_context
def config_validate(ctx, profile):
    """
    ✅ Validar configuração
    """
    try:
        config_manager = ConfigManager(ctx.obj.get('config_file'))
        config = config_manager.load_config(profile)
        
        click.echo(f"🔍 Validando configuração (profile: {profile})...")
        
        # A validação já é feita automaticamente no load_config
        # Se chegou até aqui, a configuração é válida
        click.echo("✅ Configuração válida!")
        
        # Mostrar algumas informações úteis
        click.echo(f"\n📊 Resumo da configuração:")
        click.echo(f"  🌐 FTP: {config.ftp.server}")
        click.echo(f"  💾 Cache: {'habilitado' if config.cache.enabled else 'desabilitado'}")
        click.echo(f"  ⚙️ Workers: {config.processing.max_workers}")
        click.echo(f"  📤 Formato: {config.output.format}")
        click.echo(f"  📝 Log level: {config.logging.level}")
        
    except Exception as e:
        click.echo(f"❌ Configuração inválida: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--profile', default='default', help='Profile de configuração')
@click.option('--list-profiles', is_flag=True, help='Listar perfis disponíveis')
@click.pass_context
def config_list(ctx, profile, list_profiles):
    """
    📋 Listar perfis de configuração disponíveis
    """
    try:
        config_manager = ConfigManager(ctx.obj.get('config_file'))
        
        if not config_manager.config_file.exists():
            click.echo("⚠️ Arquivo de configuração não encontrado")
            click.echo(f"💡 Execute 'python main.py config-create' para criar um arquivo padrão")
            return
        
        # Carregar arquivo YAML para listar perfis
        import yaml
        with open(config_manager.config_file, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if 'profiles' not in data:
            click.echo("⚠️ Nenhum perfil encontrado no arquivo de configuração")
            return
        
        profiles = data['profiles']
        click.echo(f"📋 Perfis disponíveis em {config_manager.config_file}:")
        
        for profile_name in sorted(profiles.keys()):
            profile_data = profiles[profile_name]
            
            # Extrair informações básicas
            ftp_server = profile_data.get('ftp', {}).get('server', 'N/A')
            cache_enabled = profile_data.get('cache', {}).get('enabled', False)
            workers = profile_data.get('processing', {}).get('max_workers', 'N/A')
            output_format = profile_data.get('output', {}).get('format', 'N/A')
            
            status = "✅" if profile_name == ctx.obj['profile'] else "📋"
            click.echo(f"\n{status} {profile_name}:")
            click.echo(f"    🌐 FTP: {ftp_server}")
            click.echo(f"    💾 Cache: {'habilitado' if cache_enabled else 'desabilitado'}")
            click.echo(f"    ⚙️ Workers: {workers}")
            click.echo(f"    📤 Formato: {output_format}")
        
        if ctx.obj['profile'] in profiles:
            click.echo(f"\n🎯 Perfil atual: {ctx.obj['profile']}")
        else:
            click.echo(f"\n⚠️ Perfil atual '{ctx.obj['profile']}' não encontrado no arquivo")
        
    except Exception as e:
        click.echo(f"❌ Erro ao listar perfis: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--profile', default='default', help='Profile de configuração')
@click.option('--section', help='Seção da configuração (ftp, cache, processing, output, logging)')
@click.option('--key', help='Chave da configuração')
@click.option('--value', help='Novo valor')
@click.pass_context
def config_set(ctx, profile, section, key, value):
    """
    ⚙️ Definir valor de configuração
    
    Exemplos:
    python main.py config-set --section ftp --key timeout --value 60
    python main.py config-set --section processing --key max_workers --value 8
    """
    if not all([section, key, value]):
        click.echo("❌ Todos os parâmetros são obrigatórios: --section, --key, --value")
        sys.exit(1)
    
    try:
        config_manager = ConfigManager(ctx.obj.get('config_file'))
        config = config_manager.load_config(profile)
        
        # Verificar se a seção existe
        if not hasattr(config, section):
            click.echo(f"❌ Seção '{section}' não encontrada")
            click.echo("💡 Seções disponíveis: ftp, cache, processing, output, logging")
            sys.exit(1)
        
        section_obj = getattr(config, section)
        
        # Verificar se a chave existe
        if not hasattr(section_obj, key):
            available_keys = [attr for attr in dir(section_obj) if not attr.startswith('_')]
            click.echo(f"❌ Chave '{key}' não encontrada na seção '{section}'")
            click.echo(f"💡 Chaves disponíveis: {', '.join(available_keys)}")
            sys.exit(1)
        
        # Converter valor para o tipo correto
        current_value = getattr(section_obj, key)
        if isinstance(current_value, bool):
            value = value.lower() in ('true', '1', 'yes', 'on')
        elif isinstance(current_value, int):
            value = int(value)
        elif isinstance(current_value, float):
            value = float(value)
        
        # Definir novo valor
        setattr(section_obj, key, value)
        
        # Validar configuração
        config_manager._validate_config(config)
        
        # Salvar configuração
        config_manager.save_config(config, profile)
        
        click.echo(f"✅ Configuração atualizada:")
        click.echo(f"   📋 Profile: {profile}")
        click.echo(f"   🔧 {section}.{key} = {value}")
        
    except ValueError as e:
        click.echo(f"❌ Erro de conversão de tipo: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Erro ao definir configuração: {e}", err=True)
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


def _display_processing_results(results, logger):
    """Exibe resultados detalhados do processamento"""
    if not results:
        click.echo("⚠️ Nenhum resultado para exibir")
        return
    
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    
    # Estatísticas gerais
    total_duration = sum(r.duration for r in results)
    total_files = sum(r.files_processed for r in results)
    total_bytes = sum(r.bytes_processed for r in results)
    
    click.echo("\n" + "="*60)
    click.echo("📊 RELATÓRIO DE PROCESSAMENTO")
    click.echo("="*60)
    
    # Resumo geral
    click.echo(f"\n📈 RESUMO GERAL:")
    click.echo(f"   ✅ Sucessos: {len(successful)}/{len(results)}")
    click.echo(f"   ❌ Falhas: {len(failed)}/{len(results)}")
    click.echo(f"   📁 Arquivos processados: {total_files:,}")
    click.echo(f"   💾 Dados processados: {_format_bytes(total_bytes)}")
    click.echo(f"   ⏱️ Tempo total: {_format_duration(total_duration)}")
    
    if total_duration > 0 and total_files > 0:
        throughput = total_files / total_duration
        click.echo(f"   🚀 Throughput: {throughput:.2f} arquivos/s")
    
    # Detalhes dos sucessos
    if successful:
        click.echo(f"\n✅ PROCESSAMENTOS BEM-SUCEDIDOS ({len(successful)}):")
        for result in successful:
            duration_str = _format_duration(result.duration)
            files_str = f"{result.files_processed} arquivos" if result.files_processed > 0 else "sem arquivos"
            click.echo(f"   📋 {result.item.id}: {duration_str}, {files_str}")
            
            # Mostrar warnings se houver
            if result.warnings:
                for warning in result.warnings[:3]:  # Máximo 3 warnings
                    click.echo(f"      ⚠️ {warning}")
                if len(result.warnings) > 3:
                    click.echo(f"      ... e mais {len(result.warnings) - 3} warnings")
    
    # Detalhes das falhas
    if failed:
        click.echo(f"\n❌ PROCESSAMENTOS COM FALHA ({len(failed)}):")
        for result in failed:
            duration_str = _format_duration(result.duration)
            click.echo(f"   💥 {result.item.id}: {duration_str}")
            
            # Mostrar erros
            for error in result.errors[:2]:  # Máximo 2 erros
                click.echo(f"      🔴 {error}")
            if len(result.errors) > 2:
                click.echo(f"      ... e mais {len(result.errors) - 2} erros")
    
    # Taxa de sucesso
    success_rate = (len(successful) / len(results)) * 100
    if success_rate == 100:
        click.echo(f"\n🎉 PROCESSAMENTO CONCLUÍDO COM 100% DE SUCESSO!")
    elif success_rate >= 80:
        click.echo(f"\n✅ Processamento concluído com {success_rate:.1f}% de sucesso")
    elif success_rate >= 50:
        click.echo(f"\n⚠️ Processamento concluído com {success_rate:.1f}% de sucesso")
    else:
        click.echo(f"\n❌ Processamento com baixa taxa de sucesso: {success_rate:.1f}%")
    
    click.echo("="*60)


def _format_bytes(bytes_count: int) -> str:
    """Formata bytes em formato legível"""
    if bytes_count == 0:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size = bytes_count
    unit_index = 0
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    return f"{size:.1f} {units[unit_index]}"


def _format_duration(seconds: float) -> str:
    """Formata duração em formato legível"""
    if seconds < 1:
        return f"{seconds*1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def _estimate_processing_time(items, stages, config) -> str:
    """Estima tempo de processamento baseado em histórico"""
    # Estimativas baseadas em experiência (podem ser refinadas com dados reais)
    base_times = {
        ProcessingStage.DOWNLOAD: 30,  # 30s por arquivo
        ProcessingStage.EXTRACT: 15,   # 15s por arquivo
        ProcessingStage.CONVERT: 45,   # 45s por arquivo
        ProcessingStage.VALIDATE: 5,   # 5s por arquivo
        ProcessingStage.CLEANUP: 2     # 2s por arquivo
    }
    
    total_estimated_seconds = 0
    
    for item in items:
        for stage in item.stages:
            base_time = base_times.get(stage, 20)  # 20s padrão
            total_estimated_seconds += base_time
    
    # Ajustar para processamento paralelo
    if config.processing.enable_parallel and len(items) > 1:
        workers = config.processing.max_workers
        parallel_factor = min(workers, len(items)) / len(items)
        total_estimated_seconds *= (1 - parallel_factor * 0.7)  # 70% de eficiência paralela
    
    return _format_duration(total_estimated_seconds)


if __name__ == '__main__':
    cli()