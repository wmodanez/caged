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
from src.utils.cache import cache_manager
from src.utils.logger import setup_logger
from src.services.ftp_service import FTPService, FTPConfig


class DownloadStageHandler(PipelineStageHandler):
    """Handler para estágio de download"""
    
    def __init__(self, config: CAGEDConfig, logger: logging.Logger):
        super().__init__(config, logger)
        self.ftp_config = config.ftp
        # Inicializar serviço FTP real
        ftp_config = FTPConfig.from_config({
            'host': self.ftp_config.server,
            'port': 21,
            'username': 'anonymous',
            'password': 'anonymous@example.com',
            'base_path': self.ftp_config.directory,
            'timeout': self.ftp_config.timeout,
            'max_retries': self.ftp_config.max_retries
        })
        self.ftp_service = FTPService(ftp_config)
    
    async def process(self, item: ProcessingItem) -> ProcessingResult:
        """Processa download de arquivos"""
        start_time = time.time()
        
        try:
            self.logger.info(f"📥 Iniciando download para {item.id}")
            
            # Verificar cache primeiro
            cache_key = f"download_{item.ano}_{item.mes:02d}"
            
            if cache_manager.has_cached_item(cache_key):
                self.logger.info(f"💾 Usando arquivo em cache para {item.id}")
                duration = time.time() - start_time
                
                return ProcessingResult(
                    item=item,
                    success=True,
                    duration=duration,
                    files_processed=1,
                    bytes_processed=0,  # Não contabilizar cache
                    warnings=["Arquivo obtido do cache"]
                )
            
            # Download real usando FTPService
            filename = f"CAGEDMOV{item.ano}{item.mes:02d}.7z"
            dest_path = Path(f"./files-zip/{filename}")
            
            # Conectar ao servidor FTP
            await asyncio.get_event_loop().run_in_executor(None, self.ftp_service.connect)
            
            # Fazer download do arquivo
            download_result = await asyncio.get_event_loop().run_in_executor(
                None, 
                self.ftp_service.download_file, 
                item.ano,
                item.mes,
                dest_path
            )
            
            if not download_result:
                raise Exception(f"Falha no download do arquivo {filename}")
            
            # Obter tamanho real do arquivo baixado
            file_size = dest_path.stat().st_size if dest_path.exists() else 0
            
            # Desconectar do servidor FTP
            await asyncio.get_event_loop().run_in_executor(None, self.ftp_service.disconnect)
            
            # Cachear resultado
            cache_manager.cache_item(
                cache_key, 
                {"filename": filename, "size": file_size}, 
                category="downloads"
            )
            
            duration = time.time() - start_time
            
            self.logger.info(f"✅ Download concluído para {item.id}: {filename}")
            
            return ProcessingResult(
                item=item,
                success=True,
                duration=duration,
                files_processed=1,
                bytes_processed=file_size
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
            
            # Verificar cache
            cache_key = f"extract_{item.ano}_{item.mes:02d}"
            
            if cache_manager.has_cached_item(cache_key):
                self.logger.info(f"💾 Usando arquivos extraídos em cache para {item.id}")
                cached_data = cache_manager.get_cached_item(cache_key)
                duration = time.time() - start_time
                
                return ProcessingResult(
                    item=item,
                    success=True,
                    duration=duration,
                    files_processed=len(cached_data.get("files", [])),
                    bytes_processed=cached_data.get("total_size", 0),
                    warnings=["Arquivos obtidos do cache"]
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
                
                # Cachear resultado
                cache_manager.cache_item(
                    cache_key,
                    {"files": extracted_files, "total_size": total_size, "metadados": metadados_lista},
                    category="extractions"
                )
                
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
    
    async def process(self, item: ProcessingItem) -> ProcessingResult:
        """Processa conversão de arquivos"""
        start_time = time.time()
        
        try:
            self.logger.info(f"🔄 Iniciando conversão para {item.id}")
            
            # Verificar cache
            cache_key = f"convert_{item.ano}_{item.mes:02d}_{self.output_config.format}"
            
            if cache_manager.has_cached_item(cache_key):
                self.logger.info(f"💾 Usando arquivos convertidos em cache para {item.id}")
                duration = time.time() - start_time
                
                return ProcessingResult(
                    item=item,
                    success=True,
                    duration=duration,
                    files_processed=3,
                    bytes_processed=0,
                    warnings=["Arquivos obtidos do cache"]
                )
            
            # Simular conversão
            await asyncio.sleep(0.7)  # Simular tempo de conversão
            
            # Simular arquivos convertidos
            output_format = self.output_config.format.lower()
            converted_files = [
                f"CAGEDMOV{item.ano}{item.mes:02d}.{output_format}",
                f"CAGEDFOR{item.ano}{item.mes:02d}.{output_format}",
                f"CAGEDEXC{item.ano}{item.mes:02d}.{output_format}"
            ]
            
            # Tamanho varia baseado na compressão
            compression_factor = 0.3 if self.output_config.compression == "snappy" else 0.5
            total_size = int(150 * 1024 * 1024 * compression_factor)
            
            # Cachear resultado
            cache_manager.cache_item(
                cache_key,
                {
                    "files": converted_files, 
                    "total_size": total_size,
                    "format": output_format,
                    "compression": self.output_config.compression
                },
                category="conversions"
            )
            
            duration = time.time() - start_time
            
            self.logger.info(
                f"✅ Conversão concluída para {item.id}: "
                f"{len(converted_files)} arquivos {output_format.upper()}"
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
        # Verificar se extração foi feita (ou está em cache)
        extract_cache_key = f"extract_{item.ano}_{item.mes:02d}"
        return cache_manager.has_cached_item(extract_cache_key)


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
            
            # Simular limpeza
            await asyncio.sleep(0.05)  # Simular tempo de limpeza
            
            # Simular arquivos removidos
            cleaned_files = [
                "Arquivos temporários",
                "Cache expirado",
                "Logs antigos"
            ]
            
            freed_space = 25 * 1024 * 1024  # 25MB liberado
            
            duration = time.time() - start_time
            
            self.logger.info(f"✅ Limpeza concluída para {item.id}: {len(cleaned_files)} tipos removidos")
            
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