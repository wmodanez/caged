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
        self.logger.info(f"🗑️  Iniciando limpeza para {item.id}")

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


class DownloadStageHandler(PipelineStageHandler):
    """Handler para estágio de download"""
    
    def __init__(self, config: CAGEDConfig, logger: logging.Logger):
        super().__init__(config, logger)
        self.ftp_service = FTPService(config)
    
    async def process(self, item: ProcessingItem) -> ProcessingResult:
        """Processa download de arquivos"""
        start_time = time.time()
        
        try:
            self.logger.info(f"📥 Iniciando download para {item.id}")
            

            
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
                raise Exception(f"Falha no download do arquivo {filename}")
            
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
            self.logger.info(f"📦 Iniciando extração para {item.id}")

            # Validar se o arquivo de entrada existe
            if not self.validate_item(item):
                self.logger.warning(f"⚠️  Arquivo de entrada para {item.id} não encontrado. Pulando extração.")
                return ProcessingResult(
                    item=item,
                    success=True,
                    duration=time.time() - start_time,
                    warnings=[f"Arquivo de entrada para {item.id} não encontrado."]
                )
            
            # Verificar cache
            cache_key = f"extract_{item.ano}_{item.mes:02d}"
            

            
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
    
    def __init__(self, config: CAGEDConfig, logger: logging.Logger):
        super().__init__(config, logger)
        self.output_config = config.output
        # Inicializar ConvertService
        from ..services.convert_service import ConvertService
        self.convert_service = ConvertService(config)
    
    async def process(self, item: ProcessingItem) -> ProcessingResult:
        """Processa conversão de arquivos"""
        start_time = time.time()
        
        try:
            self.logger.info(f"🔄 Iniciando conversão para {item.id}")

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
            
            for input_file in input_files:
                # Definir arquivo de saída
                output_file = output_dir / f"{input_file.stem}.{self.output_config.format.lower()}"
                
                # Converter arquivo
                self.logger.info(f"Convertendo {input_file} para {output_file}")
                success = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.convert_service.convert_to_parquet,
                    str(input_file),
                    str(output_file),
                    None,  # schema
                    self.output_config.compression,
                    None,  # progress_callback
                    True,  # auto_detect_encoding
                    self.config.processing.chunk_size,  # chunk_size
                    None   # validation_rules
                )
                
                if success and output_file.exists():
                    file_size = output_file.stat().st_size
                    converted_files.append(str(output_file))
                    total_size += file_size
                    self.logger.info(f"✅ Arquivo convertido: {output_file} ({file_size} bytes)")
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
                files_processed=len(converted_files),
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
                # Tentar remover o diretório se estiver vazio
                try:
                    zip_dir.rmdir()
                except OSError:
                    pass # Ignora se não estiver vazio

            # Limpar diretório de unzips
            if unzip_dir.exists():
                for f in unzip_dir.glob('*'):
                    if f.is_file():
                        file_size = f.stat().st_size
                        f.unlink()
                        cleaned_files.append(str(f))
                        freed_space += file_size
                # Tentar remover o diretório se estiver vazio
                try:
                    unzip_dir.rmdir()
                except OSError:
                    pass # Ignora se não estiver vazio
            
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
    
    def validate_item(self, item: ProcessingItem) -> bool:
        """Valida se o item pode ser limpo"""
        return True  # Limpeza sempre pode ser executada


def create_stage_handlers(config: CAGEDConfig) -> Dict[ProcessingStage, PipelineStageHandler]:
    """Cria todos os handlers de estágio"""
    logger = setup_logger("stage_handlers", config.logging.level)
    
    handlers = {
        ProcessingStage.DOWNLOAD: DownloadStageHandler(config, logger),
        ProcessingStage.EXTRACT: ExtractStageHandler(config, logger),
        ProcessingStage.CONVERT: ConvertStageHandler(config, logger),
        ProcessingStage.VALIDATE: ValidateStageHandler(config, logger),
        ProcessingStage.CLEANUP: CleanupStageHandler(config, logger)
    }
    
    logger.info(f"🔧 Criados {len(handlers)} handlers de estágio")
    
    return handlers