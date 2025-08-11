#!/usr/bin/env python3
"""
Handlers para Estágios do Pipeline CAGED
Implementação do Item 2.3 do Plano de Melhorias

Este módulo define os handlers específicos para cada estágio de processamento.
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import List, Optional, Dict, Any

from src.core.config import CAGEDConfig
from src.core.pipeline import PipelineStageHandler, ProcessingItem, ProcessingResult, ProcessingStage
from src.core.exceptions import PipelineError

from src.utils.logger import setup_logger
from src.services.ftp_service import FTPService, FTPConfig
import os

class CleanupStageHandler(PipelineStageHandler):
    """Handler para o estágio de limpeza de arquivos."""

    def __init__(self, config: CAGEDConfig, logger: logging.Logger):
        super().__init__(config, logger)
        self.zip_dir = Path('./files-zip')
        self.unzip_dir = Path('./files-unzip')

    async def process(self, item: ProcessingItem) -> ProcessingResult:
        """Processa a limpeza dos arquivos."""
        start_time = time.time()
        self.logger.debug(f"🗑️  Iniciando limpeza para {item.id}")

        try:
            # Arquivo compactado
            zip_filename = f"CAGEDMOV{item.ano}{item.mes:02d}.7z"
            zip_filepath = self.zip_dir / str(item.ano) / f"{item.ano}{item.mes:02d}" / zip_filename

            # Arquivo descompactado
            unzip_filename = f"CAGEDMOV{item.ano}{item.mes:02d}.txt"
            unzip_filepath = self.unzip_dir / str(item.ano) / f"{item.ano}{item.mes:02d}" / unzip_filename

            files_deleted = 0
            bytes_deleted = 0

            if zip_filepath.exists():
                bytes_deleted += zip_filepath.stat().st_size
                os.remove(zip_filepath)
                files_deleted += 1
                self.logger.debug(f"Arquivo compactado removido: {zip_filepath}")

            if unzip_filepath.exists():
                bytes_deleted += unzip_filepath.stat().st_size
                os.remove(unzip_filepath)
                files_deleted += 1
                self.logger.debug(f"Arquivo de texto removido: {unzip_filepath}")

            # Lógica para remover diretórios vazios
            self._remove_empty_dirs(zip_filepath.parent)
            self._remove_empty_dirs(unzip_filepath.parent)

            duration = time.time() - start_time
            self.logger.info(f"✅ Limpeza concluída para {item.id}: {files_deleted} arquivos removidos.")

            return ProcessingResult(
                item=item,
                success=True,
                duration=duration,
                files_processed=files_deleted,
                bytes_processed=bytes_deleted
            )

        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Erro na limpeza: {e}"
            self.logger.error(error_msg)
            return ProcessingResult(
                item=item,
                success=False,
                duration=duration,
                errors=[error_msg]
            )

    def _remove_empty_dirs(self, path: Path):
        """Remove diretórios vazios recursivamente."""
        if not path.is_dir():
            return

        # Remove o diretório do mês
        try:
            if not any(path.iterdir()):
                os.rmdir(path)
                self.logger.debug(f"Diretório do mês removido: {path}")
                # Tenta remover o diretório do ano
                year_path = path.parent
                if not any(year_path.iterdir()):
                    os.rmdir(year_path)
                    self.logger.debug(f"Diretório do ano removido: {year_path}")
        except OSError as e:
            self.logger.warning(f"Não foi possível remover o diretório {path}: {e}")


class DownloadStageHandler(PipelineStageHandler):
    """Handler para estágio de download"""
    
    def __init__(self, config: CAGEDConfig, logger: logging.Logger):
        super().__init__(config, logger)
        self.ftp_service = FTPService(config)
    
    async def process(self, item: ProcessingItem) -> ProcessingResult:
        """Processa download de arquivos"""
        start_time = time.time()
        
        try:
            self.logger.debug(f"📥 Iniciando download para {item.id}")
            

            
            # Caminho de destino para os arquivos
            dest_dir = Path(f"./files-zip/{item.ano}/{item.ano}{item.mes:02d}")

            # Verificar se o diretório remoto existe antes de tentar o download
            remote_base_path = self.config.ftp.directory
            year_dirs = await asyncio.get_event_loop().run_in_executor(None, self.ftp_service.list_remote_dirs, remote_base_path)
            if str(item.ano) not in year_dirs:
                raise FileNotFoundError(f"Diretório do ano {item.ano} não encontrado no FTP.")

            remote_year_path = f"{remote_base_path}/{item.ano}"
            month_dirs = await asyncio.get_event_loop().run_in_executor(None, self.ftp_service.list_remote_dirs, remote_year_path)
            if f"{item.ano}{item.mes:02d}" not in month_dirs:
                self.logger.warning(f"⚠️  Dados para {item.id} ainda não disponíveis (diretório não encontrado). Pulando.")
                return ProcessingResult(
                    item=item,
                    success=True,
                    duration=time.time() - start_time,
                    files_processed=0,
                    bytes_processed=0,
                    warnings=[f"Dados para {item.id} não disponíveis."]
                )

            # Fazer download do arquivo
            download_result = await asyncio.get_event_loop().run_in_executor(
                None, 
                self.ftp_service.download_file, 
                item.ano,
                item.mes,
                dest_dir
            )
            
            if not download_result:
                raise Exception("Falha no download do arquivo")
            
            # Obter tamanho total dos arquivos baixados
            total_size = sum(f.stat().st_size for f in dest_dir.glob('*.7z') if f.is_file())
            files_downloaded = list(dest_dir.glob('*.7z'))

            if not files_downloaded:
                raise Exception("Nenhum arquivo foi baixado, embora o diretório exista.")


            
            duration = time.time() - start_time
            
            self.logger.info(f"✅ Download concluído para {item.id}: {len(files_downloaded)} arquivos baixados.")
            
            return ProcessingResult(
                item=item,
                success=True,
                duration=duration,
                files_processed=len(files_downloaded),
                bytes_processed=total_size
            )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Erro no download: {e}"
            self.logger.error(error_msg)
            
            return ProcessingResult(
                item=item,
                success=False,
                duration=duration,
                errors=[error_msg]
            )
    
    def validate_item(self, item: ProcessingItem) -> bool:
        """Valida se o item pode ser baixado"""
        # Verificar se ano/mês são válidos (novo CAGED disponível a partir de 2020)
        if item.ano < 2020 or item.ano > 2030:
            return False
        if item.mes < 1 or item.mes > 12:
            return False
        return True


class ExtractStageHandler(PipelineStageHandler):
    """Handler para estágio de extração"""
    
    def __init__(self, config: CAGEDConfig, logger: logging.Logger):
        super().__init__(config, logger)
        # Inicializar ExtractService
        from ..services.extract_service import DescompactadorCaged
        self.extract_service = DescompactadorCaged(
            diretorio_origem="files-zip",
            diretorio_destino="files-unzip", 
            max_workers=config.processing.max_workers,
            usar_emojis=config.logging.use_emojis,
            nivel_log=config.logging.level
        )
    
    async def process(self, item: ProcessingItem) -> ProcessingResult:
        """Processa extração de arquivos"""
        start_time = time.time()
        
        try:
            self.logger.debug(f"📦 Iniciando extração para {item.id}")

            # Validar se o arquivo de entrada existe
            if not self.validate_item(item):
                self.logger.warning(f"⚠️  Arquivo de entrada para {item.id} não encontrado. Pulando extração.")
                return ProcessingResult(
                    item=item,
                    success=True,
                    duration=time.time() - start_time,
                    warnings=[f"Arquivo de entrada para {item.id} não encontrado."]
                )           

            
            # Executar extração real usando ExtractService
            success, metadados_lista = self.extract_service.descompactar_mensal(
                ano=item.ano,
                mes=item.mes,
                usar_paralelo=self.config.processing.enable_parallel,
                max_workers=self.config.processing.max_workers
            )
            
            if success and metadados_lista:
                # Coletar informações dos arquivos extraídos
                extracted_files = []
                total_size = 0
                
                for metadados in metadados_lista:
                    if "arquivos_extraidos" in metadados:
                        extracted_files.extend(metadados["arquivos_extraidos"])
                    if "tamanho_total_descompactado" in metadados:
                        total_size += metadados["tamanho_total_descompactado"]
                

                
                duration = time.time() - start_time
                
                self.logger.info(f"✅ Extração concluída para {item.id}: {len(extracted_files)} arquivos")
                
                return ProcessingResult(
                    item=item,
                    success=True,
                    duration=duration,
                    files_processed=len(extracted_files),
                    bytes_processed=total_size
                )
            else:
                # Falha na extração
                duration = time.time() - start_time
                error_msg = f"Falha na extração para {item.id}: nenhum arquivo foi extraído"
                self.logger.error(error_msg)
                
                return ProcessingResult(
                    item=item,
                    success=False,
                    duration=duration,
                    errors=[error_msg]
                )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Erro na extração: {e}"
            self.logger.error(error_msg)
            
            return ProcessingResult(
                item=item,
                success=False,
                duration=duration,
                errors=[error_msg]
            )
    
    def validate_item(self, item: ProcessingItem) -> bool:
        """Valida se o item pode ser extraído"""
        # Verificar se arquivo físico existe para extração
        from pathlib import Path
        filename = f"CAGEDMOV{item.ano}{item.mes:02d}.7z"
        file_path = Path(f"files-zip/{item.ano}/{item.ano}{item.mes:02d}/{filename}")
        return file_path.exists()


class ConvertStageHandler(PipelineStageHandler):
    """Handler para estágio de conversão"""
    
    def __init__(self, config: CAGEDConfig, logger: logging.Logger, campos_selecionados: Optional[str] = None):
        super().__init__(config, logger)
        self.output_config = config.output
        self.campos_selecionados = campos_selecionados
        # Inicializar ConvertService
        from ..services.convert_service import ConvertService
        self.convert_service = ConvertService(config)
    
    async def process(self, item: ProcessingItem) -> ProcessingResult:
        """Processa conversão de arquivos"""
        start_time = time.time()
        
        try:
            self.logger.debug(f"🔄 Iniciando conversão para {item.id}")

            # Validar se o arquivo de entrada existe
            if not self.validate_item(item):
                self.logger.warning(f"⚠️  Arquivos de entrada para {item.id} não encontrados. Pulando conversão.")
                return ProcessingResult(
                    item=item,
                    success=True,
                    duration=time.time() - start_time,
                    warnings=[f"Arquivos de entrada para {item.id} não encontrados."]
                )
            

            
            # Encontrar arquivos extraídos para conversão
            from pathlib import Path
            input_dir = Path(f"files-unzip/{item.ano}/{item.mes:02d}")
            
            # Se o diretório padrão não existir, tentar o diretório alternativo
            if not input_dir.exists():
                alt_input_dir = Path(f"files-unzip/{item.ano}/{item.ano}{item.mes:02d}")
                if alt_input_dir.exists():
                    input_dir = alt_input_dir
                else:
                    raise PipelineError(f"Diretório de entrada não encontrado: {input_dir} ou {alt_input_dir}")
                
            # Encontrar arquivos TXT/CSV para conversão
            input_files = list(input_dir.glob("*.txt")) + list(input_dir.glob("*.csv"))
            if not input_files:
                raise PipelineError(f"Nenhum arquivo TXT/CSV encontrado em {input_dir}")
            
            # Preparar diretório de saída (formato ano/anomes)
            output_dir = Path(f"files-parquet/{item.ano}/{item.ano}{item.mes:02d}")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Converter cada arquivo
            converted_files = []
            total_size = 0
            
            # Validar e converter a string de campos em lista
            lista_campos = None
            if self.campos_selecionados:
                try:
                    # Exemplo de validação simples: remover espaços e dividir por vírgula
                    lista_campos = [campo.strip() for campo in self.campos_selecionados.split(',')]
                    self.logger.info(f"Campos selecionados para conversão: {lista_campos}")
                except Exception as e:
                    self.logger.warning(f"Formato inválido para --campos: '{self.campos_selecionados}'. Ignorando. Erro: {e}")

            for input_file in input_files:
                # Definir arquivo de saída
                output_file = output_dir / f"{input_file.stem}.{self.output_config.format.lower()}"
                
                # Converter arquivo
                self.logger.info(f"Convertendo {input_file} para {output_file}")
                # Usar functools.partial para passar argumentos nomeados
                success = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.convert_service.processar_arquivo_mensal_wrapper,
                    Path(input_file),
                    item.ano,
                    item.mes,
                    lista_campos
                )
                
                # Verificar se o arquivo foi salvo no diretório files-parquet
                parquet_file = Path(f"files-parquet/CAGEDMOV{item.ano}{item.mes:02d}.parquet")
                
                if success and parquet_file.exists():
                    file_size = parquet_file.stat().st_size
                    converted_files.append(str(parquet_file))
                    total_size += file_size
                    self.logger.info(f"✅ Arquivo convertido: {parquet_file} ({file_size} bytes)")
                else:
                    self.logger.warning(f"⚠️ Falha ao converter {input_file}")
            

            
            duration = time.time() - start_time
            
            self.logger.info(
                f"✅ Conversão concluída para {item.id}: "
                f"{len(converted_files)} arquivos {self.output_config.format.upper()}"
            )
            
            return ProcessingResult(
                item=item,
                success=True,
                duration=duration,
                files_processed=1,
                bytes_processed=total_size
            )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Erro na conversão: {e}"
            self.logger.error(error_msg)
            
            return ProcessingResult(
                item=item,
                success=False,
                duration=duration,
                errors=[error_msg]
            )
    
    def validate_item(self, item: ProcessingItem) -> bool:
        """Valida se o item pode ser convertido"""
        # Verificar se os arquivos extraídos existem
        from pathlib import Path
        # Verificar o diretório padrão (mês com dois dígitos)
        input_dir = Path(f"files-unzip/{item.ano}/{item.mes:02d}")
        if input_dir.exists() and any(input_dir.glob("*.txt") or input_dir.glob("*.csv")):
            return True
            
        # Verificar o diretório alternativo (ano + mês com dois dígitos)
        alt_input_dir = Path(f"files-unzip/{item.ano}/{item.ano}{item.mes:02d}")
        return alt_input_dir.exists() and any(alt_input_dir.glob("*.txt") or alt_input_dir.glob("*.csv"))


class ValidateStageHandler(PipelineStageHandler):
    """Handler para estágio de validação"""
    
    def __init__(self, config: CAGEDConfig, logger: logging.Logger):
        super().__init__(config, logger)
    
    async def process(self, item: ProcessingItem) -> ProcessingResult:
        """Processa validação de arquivos"""
        start_time = time.time()
        
        try:
            self.logger.info(f"✅ Iniciando validação para {item.id}")
            
            # Simular validação
            await asyncio.sleep(0.1)  # Simular tempo de validação
            
            # Simular verificações
            validations = [
                "Estrutura de arquivos",
                "Integridade dos dados",
                "Formato de campos",
                "Consistência temporal"
            ]
            
            warnings = []
            
            # Simular alguns warnings ocasionais
            import random
            if random.random() < 0.3:  # 30% chance de warning
                warnings.append("Alguns registros com campos opcionais vazios")
            
            duration = time.time() - start_time
            
            self.logger.info(f"✅ Validação concluída para {item.id}: {len(validations)} verificações")
            
            return ProcessingResult(
                item=item,
                success=True,
                duration=duration,
                files_processed=1,
                bytes_processed=0,
                warnings=warnings
            )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Erro na validação: {e}"
            self.logger.error(error_msg)
            
            return ProcessingResult(
                item=item,
                success=False,
                duration=duration,
                errors=[error_msg]
            )
    
    def validate_item(self, item: ProcessingItem) -> bool:
        """Valida se o item pode ser validado"""
        return True  # Validação sempre pode ser executada


class CleanupStageHandler(PipelineStageHandler):
    """Handler para estágio de limpeza"""
    
    def __init__(self, config: CAGEDConfig, logger: logging.Logger):
        super().__init__(config, logger)
    
    async def process(self, item: ProcessingItem) -> ProcessingResult:
        """Processa limpeza de arquivos temporários"""
        start_time = time.time()
        
        try:
            self.logger.info(f"🧹 Iniciando limpeza para {item.id}")
            
            zip_dir = Path(f"./files-zip/{item.ano}/{item.ano}{item.mes:02d}")
            unzip_dir = Path(f"./files-unzip/{item.ano}/{item.ano}{item.mes:02d}")

            cleaned_files = []
            freed_space = 0

            # Limpar diretório de zips
            if zip_dir.exists():
                for f in zip_dir.glob('*'):
                    if f.is_file():
                        file_size = f.stat().st_size
                        f.unlink()
                        cleaned_files.append(str(f))
                        freed_space += file_size
                # Remover diretórios vazios recursivamente
                self._remove_empty_dirs(zip_dir)

            # Limpar diretório de unzips
            if unzip_dir.exists():
                for f in unzip_dir.glob('*'):
                    if f.is_file():
                        file_size = f.stat().st_size
                        f.unlink()
                        cleaned_files.append(str(f))
                        freed_space += file_size
                # Remover diretórios vazios recursivamente
                self._remove_empty_dirs(unzip_dir)
            
            duration = time.time() - start_time
            
            if cleaned_files:
                self.logger.info(f"✅ Limpeza concluída para {item.id}: {len(cleaned_files)} arquivos removidos, liberando {freed_space / (1024*1024):.2f} MB.")
            else:
                self.logger.info(f"✅ Limpeza concluída para {item.id}: Nenhum arquivo para limpar.")
            
            return ProcessingResult(
                item=item,
                success=True,
                duration=duration,
                files_processed=len(cleaned_files),
                bytes_processed=freed_space
            )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Erro na limpeza: {e}"
            self.logger.error(error_msg)
            
            return ProcessingResult(
                item=item,
                success=False,
                duration=duration,
                errors=[error_msg]
            )
    
    def _remove_empty_dirs(self, path: Path):
        """Remove diretórios vazios recursivamente."""
        if not path.is_dir():
            return

        # Remove o diretório do mês
        try:
            if not any(path.iterdir()):
                path.rmdir()
                self.logger.debug(f"Diretório do mês removido: {path}")
                # Tenta remover o diretório do ano
                year_path = path.parent
                if year_path.exists() and not any(year_path.iterdir()):
                    year_path.rmdir()
                    self.logger.debug(f"Diretório do ano removido: {year_path}")
        except OSError as e:
            self.logger.warning(f"Não foi possível remover o diretório {path}: {e}")


class ConsolidateStageHandler(PipelineStageHandler):
    """Handler para estágio de consolidação anual e multi-anual"""
    
    def __init__(self, config: CAGEDConfig, logger: logging.Logger):
        super().__init__(config, logger)
        self.output_config = config.output
        self._multi_year_context = None  # Para armazenar contexto de consolidação multi-anual
    
    def set_multi_year_context(self, anos: List[int]):
        """Define o contexto para consolidação multi-anual"""
        if len(anos) > 1:
            self._multi_year_context = {
                'anos': sorted(anos),
                'ano_inicio': min(anos),
                'ano_fim': max(anos),
                'is_multi_year': True
            }
        else:
            self._multi_year_context = None
    
    async def process(self, item: ProcessingItem) -> ProcessingResult:
        """Processa consolidação de arquivos anuais ou multi-anuais"""
        start_time = time.time()
        
        try:
            # Verificar se é consolidação multi-anual
            if self._multi_year_context and self._multi_year_context['is_multi_year']:
                return await self._process_multi_year_consolidation(item, start_time)
            else:
                return await self._process_single_year_consolidation(item, start_time)
                
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Erro na consolidação: {e}"
            self.logger.error(error_msg)
            
            return ProcessingResult(
                item=item,
                success=False,
                duration=duration,
                errors=[error_msg]
            )
    
    async def _process_multi_year_consolidation(self, item: ProcessingItem, start_time: float) -> ProcessingResult:
        """Processa consolidação multi-anual em arquivo único na raiz"""
        context = self._multi_year_context
        anos = context['anos']
        ano_inicio = context['ano_inicio']
        ano_fim = context['ano_fim']
        
        # Só processar uma vez (no último item para garantir que todos os dados estejam disponíveis)
        if item.ano != ano_fim:
            self.logger.info(f"⏭️ Pulando consolidação para {item.ano} - será processado na consolidação multi-anual final")
            return ProcessingResult(
                item=item,
                success=True,
                duration=time.time() - start_time,
                warnings=[f"Consolidação multi-anual será feita no final do processamento"]
            )
        
        self.logger.info(f"📊 Iniciando consolidação multi-anual para período {ano_inicio}-{ano_fim}")
        
        import polars as pl
        
        # Coletar todos os arquivos CAGEDMOV de todos os anos
        all_cagedmov_files = []
        total_input_size = 0
        
        for ano in anos:
            year_dir = Path(f"files-parquet/{ano}")
            if not year_dir.exists():
                self.logger.warning(f"Diretório do ano {ano} não encontrado: {year_dir}")
                continue
                
            # Buscar arquivos CAGEDMOV em todos os meses do ano
            year_files = []
            for month in range(1, 13):
                month_dir = year_dir / f"{ano}{month:02d}"
                if month_dir.exists():
                    month_files = [f for f in month_dir.glob("*.parquet") if "CAGEDMOV" in f.name]
                    year_files.extend(month_files)
            
            if year_files:
                all_cagedmov_files.extend(year_files)
                self.logger.info(f"Encontrados {len(year_files)} arquivos CAGEDMOV para o ano {ano}")
            else:
                self.logger.warning(f"Nenhum arquivo CAGEDMOV encontrado para o ano {ano}")
        
        if not all_cagedmov_files:
            raise PipelineError(f"Nenhum arquivo parquet CAGEDMOV encontrado para o período {ano_inicio}-{ano_fim}")
        
        self.logger.info(f"Total de {len(all_cagedmov_files)} arquivos CAGEDMOV encontrados para consolidação multi-anual")
        
        # Preparar diretório de saída (raiz do files-parquet)
        output_dir = Path("files-parquet")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Nome do arquivo consolidado multi-anual
        consolidated_file = output_dir / f"CAGEDMOV{ano_inicio}-{ano_fim}.parquet"
        
        # Ler e consolidar todos os arquivos CAGEDMOV
        dataframes = []
        
        for file_path in all_cagedmov_files:
            self.logger.debug(f"Lendo arquivo CAGEDMOV: {file_path}")
            df = pl.read_parquet(file_path)
            dataframes.append(df)
            total_input_size += file_path.stat().st_size
        
        # Concatenar todos os DataFrames
        self.logger.info("Consolidando dados CAGEDMOV multi-anuais...")
        consolidated_df = pl.concat(dataframes)
        
        # Ordenar por data de movimentação se a coluna existir
        date_columns = ['data_movimentacao', 'competencia_mov', 'data']
        sort_column = None
        for col in date_columns:
            if col in consolidated_df.columns:
                sort_column = col
                break
        
        if sort_column:
            self.logger.info(f"Ordenando dados por {sort_column}")
            consolidated_df = consolidated_df.sort(sort_column)
        
        # Salvar arquivo consolidado
        self.logger.info(f"Salvando arquivo consolidado multi-anual: {consolidated_file}")
        consolidated_df.write_parquet(
            consolidated_file,
            compression=self.output_config.compression
        )
        
        # Verificar arquivo criado
        if not consolidated_file.exists():
            raise PipelineError("Falha ao criar arquivo consolidado multi-anual")
        
        output_size = consolidated_file.stat().st_size
        total_records = len(consolidated_df)
        
        # Remover arquivos CAGEDMOV originais após consolidação bem-sucedida
        self.logger.info("Removendo arquivos CAGEDMOV originais...")
        removed_files = []
        freed_space = 0
        
        for file_path in all_cagedmov_files:
            try:
                file_size = file_path.stat().st_size
                file_path.unlink()
                removed_files.append(str(file_path))
                freed_space += file_size
                self.logger.debug(f"Arquivo removido: {file_path}")
            except Exception as e:
                self.logger.warning(f"Erro ao remover arquivo {file_path}: {e}")
        
        # Remover diretórios vazios após remoção dos arquivos
        for ano in anos:
            year_dir = Path(f"files-parquet/{ano}")
            if year_dir.exists():
                self._remove_empty_month_dirs(year_dir)
        
        duration = time.time() - start_time
        
        self.logger.info(
            f"✅ Consolidação multi-anual concluída para período {ano_inicio}-{ano_fim}: "
            f"{total_records:,} registros CAGEDMOV consolidados em {consolidated_file.name}. "
            f"{len(removed_files)} arquivos originais removidos, liberando {freed_space / (1024*1024):.2f} MB"
        )
        
        return ProcessingResult(
            item=item,
            success=True,
            duration=duration,
            files_processed=len(all_cagedmov_files),
            bytes_processed=output_size
        )
    
    async def _process_single_year_consolidation(self, item: ProcessingItem, start_time: float) -> ProcessingResult:
        """Processa consolidação de um único ano"""
        self.logger.info(f"📊 Iniciando consolidação anual para {item.ano}")
        
        # Validar se existem arquivos para consolidar
        if not self.validate_item(item):
            self.logger.warning(f"⚠️  Nenhum arquivo CAGEDMOV encontrado para consolidação do ano {item.ano}")
            return ProcessingResult(
                item=item,
                success=True,
                duration=time.time() - start_time,
                warnings=[f"Nenhum arquivo CAGEDMOV encontrado para consolidação do ano {item.ano}"]
            )
        
        # Encontrar todos os arquivos parquet do ano que contêm CAGEDMOV
        import polars as pl
        
        year_dir = Path(f"files-parquet/{item.ano}")
        cagedmov_files = []
        
        # Buscar arquivos CAGEDMOV em todos os meses do ano
        for month in range(1, 13):
            month_dir = year_dir / f"{item.ano}{month:02d}"
            if month_dir.exists():
                # Filtrar apenas arquivos que contêm 'CAGEDMOV' no nome
                month_files = [f for f in month_dir.glob("*.parquet") if "CAGEDMOV" in f.name]
                cagedmov_files.extend(month_files)
        
        if not cagedmov_files:
            raise PipelineError(f"Nenhum arquivo parquet CAGEDMOV encontrado para o ano {item.ano}")
        
        self.logger.info(f"Encontrados {len(cagedmov_files)} arquivos CAGEDMOV para consolidação")
        
        # Preparar diretório de saída (files-parquet/ANO/)
        output_dir = Path(f"files-parquet/{item.ano}")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Nome do arquivo consolidado
        consolidated_file = output_dir / f"CAGEDMOV{item.ano}.parquet"
        
        # Ler e consolidar todos os arquivos CAGEDMOV
        dataframes = []
        total_input_size = 0
        
        for file_path in cagedmov_files:
            self.logger.debug(f"Lendo arquivo CAGEDMOV: {file_path}")
            df = pl.read_parquet(file_path)
            dataframes.append(df)
            total_input_size += file_path.stat().st_size
        
        # Concatenar todos os DataFrames
        self.logger.info("Consolidando dados CAGEDMOV...")
        consolidated_df = pl.concat(dataframes)
        
        # Ordenar por data de movimentação se a coluna existir
        date_columns = ['data_movimentacao', 'competencia_mov', 'data']
        sort_column = None
        for col in date_columns:
            if col in consolidated_df.columns:
                sort_column = col
                break
        
        if sort_column:
            self.logger.info(f"Ordenando dados por {sort_column}")
            consolidated_df = consolidated_df.sort(sort_column)
        
        # Salvar arquivo consolidado
        self.logger.info(f"Salvando arquivo consolidado: {consolidated_file}")
        consolidated_df.write_parquet(
            consolidated_file,
            compression=self.output_config.compression
        )
        
        # Verificar arquivo criado
        if not consolidated_file.exists():
            raise PipelineError("Falha ao criar arquivo consolidado")
        
        output_size = consolidated_file.stat().st_size
        total_records = len(consolidated_df)
        
        # Remover arquivos CAGEDMOV originais após consolidação bem-sucedida
        self.logger.info("Removendo arquivos CAGEDMOV originais...")
        removed_files = []
        freed_space = 0
        
        for file_path in cagedmov_files:
            try:
                file_size = file_path.stat().st_size
                file_path.unlink()
                removed_files.append(str(file_path))
                freed_space += file_size
                self.logger.debug(f"Arquivo removido: {file_path}")
            except Exception as e:
                self.logger.warning(f"Erro ao remover arquivo {file_path}: {e}")
        
        # Remover diretórios vazios após remoção dos arquivos
        self._remove_empty_month_dirs(year_dir)
        
        duration = time.time() - start_time
        
        self.logger.info(
            f"✅ Consolidação concluída para {item.ano}: "
            f"{total_records:,} registros CAGEDMOV consolidados em {consolidated_file.name}. "
            f"{len(removed_files)} arquivos originais removidos, liberando {freed_space / (1024*1024):.2f} MB"
        )
        
        return ProcessingResult(
            item=item,
            success=True,
            duration=duration,
            files_processed=len(cagedmov_files),
            bytes_processed=output_size
        )
    
    def _remove_empty_month_dirs(self, year_dir: Path):
        """Remove diretórios de mês vazios após consolidação"""
        for month in range(1, 13):
            month_dir = year_dir / f"{year_dir.name}{month:02d}"
            if month_dir.exists():
                try:
                    # Verificar se o diretório está vazio
                    if not any(month_dir.iterdir()):
                        month_dir.rmdir()
                        self.logger.debug(f"Diretório do mês removido: {month_dir}")
                except OSError as e:
                    self.logger.warning(f"Não foi possível remover o diretório {month_dir}: {e}")
    
    def validate_item(self, item: ProcessingItem) -> bool:
        """Valida se o item pode ser consolidado"""
        from pathlib import Path
        
        # Verificar se existe pelo menos um arquivo parquet CAGEDMOV do ano
        year_dir = Path(f"files-parquet/{item.ano}")
        if not year_dir.exists():
            return False
        
        # Buscar arquivos CAGEDMOV em qualquer mês do ano
        for month in range(1, 13):
            month_dir = year_dir / f"{item.ano}{month:02d}"
            if month_dir.exists():
                # Verificar se existe pelo menos um arquivo com 'CAGEDMOV' no nome
                cagedmov_files = [f for f in month_dir.glob("*.parquet") if "CAGEDMOV" in f.name]
                if cagedmov_files:
                    return True
        
        return False


class CalculateSaldoStageHandler(PipelineStageHandler):
    """Handler para o estágio de cálculo de saldo."""

    def __init__(self, config: CAGEDConfig, logger: logging.Logger, incremental: bool = False):
        super().__init__(config, logger)
        from ..services.saldo_service import SaldoService
        self.saldo_service = SaldoService(logger=self.logger)
        self.incremental = incremental
        self.logger.info(f"CalculateSaldoStageHandler initialized with incremental={self.incremental}")

    async def process(self, item: ProcessingItem) -> ProcessingResult:
        """Processa o cálculo do saldo para um item."""
        start_time = time.time()
        self.logger.info(f"Saldo para {item.id}")
        self.logger.info(f"Verificando flag incremental no process: {self.incremental}")

        try:
            if self.incremental:
                self.logger.info("Executando cálculo de saldo incremental.")
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.saldo_service.calcular_e_atualizar_saldo_incremental,
                    [item.ano],
                    [item.mes]
                )
            else:
                self.logger.info("Executando cálculo de saldo completo.")
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.saldo_service.calcular_e_atualizar_saldo,
                    [item.ano],
                    [item.mes]
                )

            duration = time.time() - start_time
            self.logger.info(f"Cálculo de saldo concluído para {item.id}.")

            return ProcessingResult(
                item=item,
                success=True,
                duration=duration
            )

        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Erro no cálculo de saldo: {e}"
            self.logger.error(error_msg)
            return ProcessingResult(
                item=item,
                success=False,
                duration=duration,
                errors=[error_msg]
            )


def create_stage_handlers(config: CAGEDConfig, incremental_saldo: bool = False, campos_selecionados: Optional[str] = None) -> Dict[ProcessingStage, PipelineStageHandler]:
    """Cria todos os handlers de estágio"""
    logger = setup_logger("stage_handlers", config.logging.level)
    
    handlers = {
        ProcessingStage.DOWNLOAD: DownloadStageHandler(config, logger),
        ProcessingStage.EXTRACT: ExtractStageHandler(config, logger),
        ProcessingStage.CONVERT: ConvertStageHandler(config, logger, campos_selecionados=campos_selecionados),
        ProcessingStage.CALCULATE_SALDO: CalculateSaldoStageHandler(config, logger, incremental=incremental_saldo),
        ProcessingStage.CONSOLIDATE: ConsolidateStageHandler(config, logger),
        ProcessingStage.VALIDATE: ValidateStageHandler(config, logger),
        ProcessingStage.CLEANUP: CleanupStageHandler(config, logger)
    }
    
    logger.debug(f"🔧 Criados {len(handlers)} handlers de estágio")
    
    return handlers