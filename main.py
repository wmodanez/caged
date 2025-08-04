#!/usr/bin/env python3
"""
Processador de Dados CAGED - Versão Unificada
Script principal para download, processamento e consolidação de dados mensais

Versão: 2.0.0 - Implementação do Item 1.1 do Plano de Melhorias
Data: 2024

Melhorias implementadas:
- Comando 'processar' unificado substituindo 12 comandos anteriores
- Flags --download, --extract, --convert para controle de etapas
- Flags --skip-* para pular etapas específicas
- Comandos antigos mantidos como deprecated com warnings
- Validações centralizadas
- Logging melhorado
"""

import click
import logging
from pathlib import Path
from datetime import datetime
from src.util.gerenciador_ftp import GerenciadorArquivosCaged
from src.util.conversor_parquet import ConversorParquetCaged
from src.util.descompactador import DescompactadorCaged
from src.util.logger_config import LoggerConfig, setup_logger, log_structured
from src.util.utilitarios import MedidorTempo


# ============================================================================
# CONFIGURAÇÃO DE LOGGING CENTRALIZADA
# ============================================================================

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
    
    # Usar o novo sistema de logging centralizado
    return setup_logger(
        name="caged",
        level=nivel,
        enable_file=True,
        enable_console=True,
        use_emojis=usar_emojis,
        enable_json=enable_json,
        module_levels=module_levels
    )


# ============================================================================
# VALIDAÇÕES CENTRALIZADAS
# ============================================================================

class ValidadorCaged:
    """
    Classe para validações centralizadas do sistema CAGED
    """
    
    @staticmethod
    def validar_ano_mes(ano: int, mes: int) -> tuple[bool, str]:
        """Valida ano e mês"""
        if ano is None:
            return False, "Ano é obrigatório"
        if ano < 2020 or ano > datetime.now().year:
            return False, f"Ano deve estar entre 2020 e {datetime.now().year}"
        if mes is not None and (mes < 1 or mes > 12):
            return False, "Mês deve estar entre 1 e 12"
        return True, ""
    
    @staticmethod
    def validar_faixa_datas(ano_inicio: int, mes_inicio: int, ano_fim: int, mes_fim: int) -> tuple[bool, str]:
        """Valida faixa de datas"""
        if any(x is None for x in [ano_inicio, mes_inicio, ano_fim, mes_fim]):
            return False, "Para faixa de datas, especifique --ano-inicio, --mes-inicio, --ano-fim e --mes-fim"
        
        # Validar anos e meses individuais
        for ano in [ano_inicio, ano_fim]:
            valido, msg = ValidadorCaged.validar_ano_mes(ano, None)
            if not valido:
                return False, msg
        
        for mes in [mes_inicio, mes_fim]:
            if mes < 1 or mes > 12:
                return False, "Meses devem estar entre 1 e 12"
        
        # Validar ordem cronológica
        if ano_inicio > ano_fim or (ano_inicio == ano_fim and mes_inicio > mes_fim):
            return False, "Data inicial deve ser anterior à data final"
        
        return True, ""
    
    @staticmethod
    def validar_espaco_disco(diretorio: str = ".", min_gb: float = 1.0) -> tuple[bool, str]:
        """Valida espaço disponível em disco"""
        try:
            import shutil
            total, used, free = shutil.disk_usage(diretorio)
            free_gb = free / (1024**3)
            if free_gb < min_gb:
                return False, f"Espaço insuficiente. Disponível: {free_gb:.1f}GB, Necessário: {min_gb}GB"
            return True, ""
        except Exception as e:
            return False, f"Erro ao verificar espaço em disco: {e}"


# Logger global (será configurado na função cli)
logger = None


# ============================================================================
# COMANDO PRINCIPAL UNIFICADO
# ============================================================================

@click.group()
@click.version_option(version="2.0.0")
@click.option('--log-level', type=click.Choice(['debug', 'info', 'warn'], case_sensitive=False), default='warn', help='Nível de log')
@click.pass_context
def cli(ctx, log_level):
    """
    🎯 Processador de Dados CAGED - Versão Unificada 2.0
    
    Sistema para download, processamento e consolidação de dados mensais
    do Cadastro Geral de Empregados e Desempregados (CAGED).
    
    🆕 NOVIDADES DA VERSÃO 2.0:
    ✨ Comando 'processar' unificado
    ⚡ Controle granular de etapas
    🔧 Validações aprimoradas
    📊 Logging melhorado
    
    Exemplos de uso:
    \b
    # Processamento completo
    python main.py processar --ano 2024 --mes 1
    
    # Apenas download
    python main.py processar --ano 2024 --mes 1 --download
    
    # Pular download, apenas extrair e converter
    python main.py processar --ano 2024 --mes 1 --skip-download --extract --convert
    """
    ctx.ensure_object(dict)
    
    # Configurar logging
    log_level_map = {'debug': 'DEBUG', 'info': 'INFO', 'warn': 'WARNING'}
    nivel_log = log_level_map[log_level.lower()]
    ctx.obj['log_level'] = nivel_log
    
    global logger
    logger = configurar_logging(nivel_log, usar_emojis=True)


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
@click.option('--campos', multiple=True, help='Campos específicos para conversão')
@click.option('--dry-run', is_flag=True, help='Apenas validar, não executar')
@click.option('--workers', type=int, default=4, help='Número de workers paralelos')
@click.option('--use-cache', is_flag=True, help='Usar sistema de cache')
def processar(ano, mes, ano_inicio, mes_inicio, ano_fim, mes_fim, todos_meses,
             download, extract, convert, skip_download, skip_extract, skip_convert,
             campos, dry_run, workers, use_cache):
    """
    🚀 Comando unificado para processamento de dados CAGED
    
    Este comando substitui os 12 comandos anteriores, oferecendo controle
    granular sobre as etapas de processamento.
    
    ETAPAS DE PROCESSAMENTO:
    📥 Download: Baixar arquivos do servidor FTP
    📦 Extract: Descompactar arquivos .7z
    🔄 Convert: Converter para formato Parquet
    
    EXEMPLOS DE USO:
    \b
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
    
    logger.info(f"🚀 Iniciando processamento unificado - Ano: {ano}, Mês: {mes}, Workers: {workers}, Cache: {use_cache}")
    
    # Inicializar medidor de tempo
    medidor = MedidorTempo("Processamento CAGED")
    medidor.iniciar_processo()
    
    # ========================================================================
    # VALIDAÇÕES CENTRALIZADAS
    # ========================================================================
    
    # Determinar etapas a executar
    if not any([download, extract, convert]):
        if any([skip_download, skip_extract, skip_convert]):
            # Se há flags skip, executar as não-skipadas
            executar_download = not skip_download
            executar_extract = not skip_extract
            executar_convert = not skip_convert
        else:
            # Se não há flags específicas, executar todas
            executar_download = True
            executar_extract = True
            executar_convert = True
    else:
        # Se há flags específicas, executar apenas as marcadas
        executar_download = download and not skip_download
        executar_extract = extract and not skip_extract
        executar_convert = convert and not skip_convert
    
    click.echo(f"📋 Etapas planejadas:")
    click.echo(f"   📥 Download: {'✅' if executar_download else '⏭️ '}")
    click.echo(f"   📦 Extract: {'✅' if executar_extract else '⏭️ '}")
    click.echo(f"   🔄 Convert: {'✅' if executar_convert else '⏭️ '}")
    
    # Validar parâmetros de data
    if ano_inicio is not None or ano_fim is not None:
        # Processamento de faixa
        valido, msg = ValidadorCaged.validar_faixa_datas(ano_inicio, mes_inicio, ano_fim, mes_fim)
        if not valido:
            click.echo(f"❌ Erro de validação: {msg}")
            return
        modo_processamento = "faixa"
        click.echo(f"📅 Modo: Faixa de datas ({mes_inicio:02d}/{ano_inicio} até {mes_fim:02d}/{ano_fim})")
    elif todos_meses:
        # Processamento de ano completo
        valido, msg = ValidadorCaged.validar_ano_mes(ano, None)
        if not valido:
            click.echo(f"❌ Erro de validação: {msg}")
            return
        modo_processamento = "ano_completo"
        click.echo(f"📅 Modo: Ano completo ({ano})")
    elif ano and mes:
        # Processamento mensal
        valido, msg = ValidadorCaged.validar_ano_mes(ano, mes)
        if not valido:
            click.echo(f"❌ Erro de validação: {msg}")
            return
        modo_processamento = "mensal"
        click.echo(f"📅 Modo: Mensal ({mes:02d}/{ano})")
    else:
        click.echo("❌ Erro: Especifique --ano/--mes, --todos-meses ou faixa de datas")
        return
    
    # Validar espaço em disco
    valido, msg = ValidadorCaged.validar_espaco_disco(min_gb=2.0)
    if not valido:
        click.echo(f"⚠️  Aviso: {msg}")
        if not click.confirm("Continuar mesmo assim?"):
            return
    
    # Validar campos se especificados
    if campos:
        campos_validos = ['ADMITIDOS', 'DESLIGADOS', 'SALDO', 'MOVIMENTACAO', 'INDICADORES']
        campos_invalidos = [c for c in campos if c.upper() not in campos_validos]
        if campos_invalidos:
            click.echo(f"❌ Campos inválidos: {', '.join(campos_invalidos)}")
            click.echo(f"   Campos válidos: {', '.join(campos_validos)}")
            return
        click.echo(f"📊 Campos selecionados: {', '.join(campos)}")
    
    # Modo dry-run
    if dry_run:
        click.echo("\n🔍 MODO DRY-RUN - Apenas validação")
        click.echo("✅ Todas as validações passaram!")
        click.echo("💡 Execute sem --dry-run para processar os dados")
        return
    
    # ========================================================================
    # INICIALIZAÇÃO DE COMPONENTES
    # ========================================================================
    
    gerenciador = None
    descompactador = None
    conversor = None
    
    try:
        with medidor.etapa("Inicialização de Componentes"):
            if executar_download:
                gerenciador = GerenciadorArquivosCaged()
                gerenciador.conectar()
                click.echo("🔗 Conexão FTP estabelecida")
            
            if executar_extract:
                descompactador = DescompactadorCaged()
                click.echo("📦 Descompactador inicializado")
            
            if executar_convert:
                conversor = ConversorParquetCaged()
                click.echo(f"🔄 Conversor inicializado (Cache: {'✅' if use_cache else '❌'})")
        
        # ====================================================================
        # PROCESSAMENTO PRINCIPAL
        # ====================================================================
        
        total_processados = 0
        total_sucessos = 0
        
        if modo_processamento == "mensal":
            # Processamento mensal
            with medidor.etapa(f"Processamento Mensal {mes:02d}/{ano}"):
                click.echo(f"\n🚀 Iniciando processamento de {mes:02d}/{ano}")
                sucesso = _processar_mes(
                    ano, mes, gerenciador, descompactador, conversor,
                    executar_download, executar_extract, executar_convert,
                    campos, workers, medidor
                )
                total_processados = 1
                if sucesso:
                    total_sucessos = 1
                
        elif modo_processamento == "ano_completo":
            # Processamento de ano completo
            with medidor.etapa(f"Processamento Anual {ano}"):
                click.echo(f"\n🚀 Iniciando processamento do ano {ano}")
                for mes_atual in range(1, 13):
                    click.echo(f"\n📅 Processando {mes_atual:02d}/{ano}")
                    sucesso = _processar_mes(
                        ano, mes_atual, gerenciador, descompactador, conversor,
                        executar_download, executar_extract, executar_convert,
                        campos, workers, medidor
                    )
                    total_processados += 1
                    if sucesso:
                        total_sucessos += 1
                    
        elif modo_processamento == "faixa":
            # Processamento de faixa
            with medidor.etapa(f"Processamento Faixa {mes_inicio:02d}/{ano_inicio} - {mes_fim:02d}/{ano_fim}"):
                click.echo(f"\n🚀 Iniciando processamento da faixa {mes_inicio:02d}/{ano_inicio} até {mes_fim:02d}/{ano_fim}")
                
                ano_atual = ano_inicio
                mes_atual = mes_inicio
                
                while ano_atual < ano_fim or (ano_atual == ano_fim and mes_atual <= mes_fim):
                    click.echo(f"\n📅 Processando {mes_atual:02d}/{ano_atual}")
                    sucesso = _processar_mes(
                        ano_atual, mes_atual, gerenciador, descompactador, conversor,
                        executar_download, executar_extract, executar_convert,
                        campos, workers, medidor
                    )
                    total_processados += 1
                    if sucesso:
                        total_sucessos += 1
                    
                    # Avançar para próximo mês
                    mes_atual += 1
                    if mes_atual > 12:
                        mes_atual = 1
                        ano_atual += 1
        
        # ====================================================================
        # RELATÓRIO FINAL
        # ====================================================================
        
        # Finalizar medição de tempo
        medidor.finalizar_processo()
        
        click.echo("\n" + "="*50)
        click.echo("📊 RELATÓRIO FINAL")
        click.echo("="*50)
        click.echo(f"📈 Total processado: {total_processados} meses")
        click.echo(f"✅ Sucessos: {total_sucessos}")
        click.echo(f"❌ Falhas: {total_processados - total_sucessos}")
        click.echo(f"📊 Taxa de sucesso: {(total_sucessos/total_processados)*100:.1f}%")
        
        # Imprimir resumo de tempo
        medidor.imprimir_resumo()
        
        if use_cache and conversor:
            estatisticas = conversor.exibir_estatisticas_cache()
            if estatisticas:
                click.echo(f"\n💾 ESTATÍSTICAS DE CACHE:")
                click.echo(f"   🎯 Cache Hits: {estatisticas.get('cache_hits', 0)}")
                click.echo(f"   ❌ Cache Misses: {estatisticas.get('cache_misses', 0)}")
                click.echo(f"   📈 Taxa de Acerto: {estatisticas.get('taxa_acerto_cache', 0):.1f}%")
        
        if total_sucessos == total_processados:
            click.echo("\n🎉 Processamento concluído com sucesso!")
        else:
            click.echo("\n⚠️  Processamento concluído com algumas falhas")
            
    except Exception as e:
        logger.error(f"❌ Erro durante o processamento: {e}")
        click.echo(f"❌ Erro: {e}")
    finally:
        # Cleanup
        if gerenciador:
            gerenciador.desconectar()
            click.echo("🔌 Conexão FTP encerrada")


def _processar_mes(ano: int, mes: int, gerenciador, descompactador, conversor,
                  executar_download: bool, executar_extract: bool, executar_convert: bool,
                  campos: tuple, workers: int, medidor=None) -> bool:
    """
    Processa um mês específico executando as etapas selecionadas
    
    Returns:
        bool: True se todas as etapas executadas foram bem-sucedidas
    """
    sucesso_geral = True
    
    try:
        # Etapa 1: Download
        if executar_download:
            if medidor:
                with medidor.etapa(f"Download {mes:02d}/{ano}"):
                    click.echo(f"   📥 Baixando dados...")
                    sucesso, metadados = gerenciador.baixar_dados_mensais(ano, mes)
                    if sucesso:
                        click.echo(f"   ✅ Download concluído")
                    else:
                        click.echo(f"   ❌ Falha no download")
                        sucesso_geral = False
            else:
                click.echo(f"   📥 Baixando dados...")
                sucesso, metadados = gerenciador.baixar_dados_mensais(ano, mes)
                if sucesso:
                    click.echo(f"   ✅ Download concluído")
                else:
                    click.echo(f"   ❌ Falha no download")
                    sucesso_geral = False
        
        # Etapa 2: Extração
        if executar_extract and sucesso_geral:
            if medidor:
                with medidor.etapa(f"Extração {mes:02d}/{ano}"):
                    click.echo(f"   📦 Extraindo arquivos...")
                    sucesso = descompactador.descompactar_mensal(ano, mes)
                    if sucesso:
                        click.echo(f"   ✅ Extração concluída")
                    else:
                        click.echo(f"   ❌ Falha na extração")
                        sucesso_geral = False
            else:
                click.echo(f"   📦 Extraindo arquivos...")
                sucesso = descompactador.descompactar_mensal(ano, mes)
                if sucesso:
                    click.echo(f"   ✅ Extração concluída")
                else:
                    click.echo(f"   ❌ Falha na extração")
                    sucesso_geral = False
        
        # Etapa 3: Conversão
        if executar_convert and sucesso_geral:
            if medidor:
                with medidor.etapa(f"Conversão {mes:02d}/{ano}"):
                    click.echo(f"   🔄 Convertendo para Parquet...")
                    sucesso, _, _, _ = conversor.converter_mensal(
                        ano, mes, 
                        campos_selecionados=list(campos) if campos else None,
                        usar_paralelismo=workers > 1
                    )
                    if sucesso:
                        click.echo(f"   ✅ Conversão concluída")
                    else:
                        click.echo(f"   ❌ Falha na conversão")
                        sucesso_geral = False
            else:
                click.echo(f"   🔄 Convertendo para Parquet...")
                sucesso, _, _, _ = conversor.converter_mensal(
                    ano, mes, 
                    campos_selecionados=list(campos) if campos else None,
                    usar_paralelismo=workers > 1
                )
                if sucesso:
                    click.echo(f"   ✅ Conversão concluída")
                else:
                    click.echo(f"   ❌ Falha na conversão")
                    sucesso_geral = False
        
        return sucesso_geral
        
    except Exception as e:
        logger.error(f"Erro ao processar {ano}/{mes:02d}: {e}")
        click.echo(f"   ❌ Erro: {e}")
        return False


# ============================================================================
# COMANDOS DEPRECATED (MANTIDOS PARA COMPATIBILIDADE)
# ============================================================================

@cli.command(hidden=True)
@click.pass_context
def baixar(ctx, **kwargs):
    """⚠️  DEPRECATED: Use 'processar --download' em vez deste comando"""
    click.echo("⚠️  AVISO: O comando 'baixar' está deprecated.")
    click.echo("💡 Use: python main.py processar --ano X --mes Y --download")
    click.echo("📖 Para mais informações: python main.py processar --help")


@cli.command(hidden=True)
@click.pass_context
def apenas_converter(ctx, **kwargs):
    """⚠️  DEPRECATED: Use 'processar --skip-download --skip-extract --convert'"""
    click.echo("⚠️  AVISO: O comando 'apenas-converter' está deprecated.")
    click.echo("💡 Use: python main.py processar --ano X --mes Y --skip-download --skip-extract --convert")
    click.echo("📖 Para mais informações: python main.py processar --help")


@cli.command(hidden=True)
@click.pass_context
def converter(ctx, **kwargs):
    """⚠️  DEPRECATED: Use 'processar --convert'"""
    click.echo("⚠️  AVISO: O comando 'converter' está deprecated.")
    click.echo("💡 Use: python main.py processar --ano X --mes Y --convert")
    click.echo("📖 Para mais informações: python main.py processar --help")


@cli.command(hidden=True)
@click.pass_context
def consolidar(ctx, **kwargs):
    """⚠️  DEPRECATED: Use 'processar --convert --todos-meses'"""
    click.echo("⚠️  AVISO: O comando 'consolidar' está deprecated.")
    click.echo("💡 Use: python main.py processar --ano X --todos-meses --convert")
    click.echo("📖 Para mais informações: python main.py processar --help")


@cli.command(hidden=True)
@click.pass_context
def apenas_descompactar(ctx, **kwargs):
    """⚠️  DEPRECATED: Use 'processar --skip-download --extract'"""
    click.echo("⚠️  AVISO: O comando 'apenas-descompactar' está deprecated.")
    click.echo("💡 Use: python main.py processar --ano X --mes Y --skip-download --extract")
    click.echo("📖 Para mais informações: python main.py processar --help")


@cli.command(hidden=True)
@click.pass_context
def descompactar(ctx, **kwargs):
    """⚠️  DEPRECATED: Use 'processar --download --extract'"""
    click.echo("⚠️  AVISO: O comando 'descompactar' está deprecated.")
    click.echo("💡 Use: python main.py processar --ano X --mes Y --download --extract")
    click.echo("📖 Para mais informações: python main.py processar --help")


@cli.command(hidden=True)
@click.pass_context
def completo(ctx, **kwargs):
    """⚠️  DEPRECATED: Use 'processar' (comportamento padrão)"""
    click.echo("⚠️  AVISO: O comando 'completo' está deprecated.")
    click.echo("💡 Use: python main.py processar --ano X --mes Y")
    click.echo("📖 Para mais informações: python main.py processar --help")


# ============================================================================
# COMANDOS UTILITÁRIOS MANTIDOS
# ============================================================================

@cli.command()
def status():
    """
    📈 Exibe status do projeto e arquivos processados
    """
    click.echo("🎯 Status do Processador CAGED v2.0")
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
        try:
            gerenciador = GerenciadorArquivosCaged()
            gerenciador.conectar()
            click.echo("✅ Conexão FTP: OK")
            gerenciador.desconectar()
        except Exception:
            click.echo("❌ Conexão FTP: Falha")
        
        click.echo("\n✅ Status exibido!")
    except Exception as e:
        logger.error(f"Erro ao exibir status: {e}")
        click.echo(f"❌ Erro ao exibir status: {e}")


@cli.command()
def migrar():
    """
    🔄 Guia de migração da versão 1.x para 2.0
    
    Exibe informações sobre como migrar comandos antigos para a nova sintaxe.
    """
    click.echo("🔄 Guia de Migração CAGED v1.x → v2.0")
    click.echo("=" * 50)
    click.echo("\n📋 MAPEAMENTO DE COMANDOS:")
    click.echo("\n🔸 Comandos de Download:")
    click.echo("   ANTES: python main.py baixar --ano 2024 --mes 1")
    click.echo("   AGORA: python main.py processar --ano 2024 --mes 1 --download")
    
    click.echo("\n🔸 Comandos de Extração:")
    click.echo("   ANTES: python main.py apenas-descompactar --ano 2024 --mes 1")
    click.echo("   AGORA: python main.py processar --ano 2024 --mes 1 --skip-download --extract")
    
    click.echo("\n🔸 Comandos de Conversão:")
    click.echo("   ANTES: python main.py apenas-converter --ano 2024 --mes 1")
    click.echo("   AGORA: python main.py processar --ano 2024 --mes 1 --skip-download --skip-extract --convert")
    
    click.echo("\n🔸 Processamento Completo:")
    click.echo("   ANTES: python main.py completo --ano 2024 --mes 1")
    click.echo("   AGORA: python main.py processar --ano 2024 --mes 1")
    
    click.echo("\n🔸 Processamento Anual:")
    click.echo("   ANTES: python main.py completo --ano 2024 --consolidacao-anual")
    click.echo("   AGORA: python main.py processar --ano 2024 --todos-meses")
    
    click.echo("\n✨ NOVAS FUNCIONALIDADES:")
    click.echo("   🎯 Controle granular: --download, --extract, --convert")
    click.echo("   ⏭️  Pular etapas: --skip-download, --skip-extract, --skip-convert")
    click.echo("   🔍 Validação: --dry-run")
    click.echo("   ⚡ Performance: --workers N, --use-cache")
    
    click.echo("\n📖 Para mais detalhes: python main.py processar --help")


if __name__ == '__main__':
    # Criar diretórios necessários se não existirem
    for dir_name in ['files-zip', 'files-unzip', 'parquet', 'logs']:
        Path(dir_name).mkdir(exist_ok=True)
    
    cli()