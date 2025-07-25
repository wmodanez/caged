#!/usr/bin/env python3
"""
Descompactador de Arquivos CAGED
Módulo para descompactar arquivos .7z baixados do CAGED
"""

import py7zr
import re
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from tqdm import tqdm
from datetime import datetime

# Importação das entidades
from src.Entity.movimentacao import Movimentacao
from src.Entity.saldo_mensal import SaldoMensal
from src.Entity.exclusao import Exclusao
from src.Entity.movimentacao_fora_prazo import MovimentacaoForaPrazo
from src.Entity.indicador import Indicador


class DescompactadorCaged:
    """
    Descompactador especializado para arquivos CAGED
    """
    
    def __init__(self, 
                 diretorio_origem: str = "files-zip",
                 diretorio_destino: str = "files-unzip"):
        """
        Inicializa o descompactador
        
        Args:
            diretorio_origem: Diretório com arquivos .7z
            diretorio_destino: Diretório para arquivos descompactados
        """
        self.diretorio_origem = Path(diretorio_origem)
        self.diretorio_destino = Path(diretorio_destino)
        self.metadados_arquivos: Dict[str, Dict] = {}
        
    def descompactar_arquivo(self, arquivo_7z: Path, destino: Optional[Path] = None) -> Tuple[bool, Dict]:
        """
        Descompacta um arquivo .7z específico
        
        Args:
            arquivo_7z: Caminho do arquivo .7z
            destino: Diretório destino (padrão: auto)
            
        Returns:
            Tuple[bool, Dict]: (Sucesso, Metadados do arquivo)
        """
        if not arquivo_7z.exists():
            print(f"❌ Arquivo não encontrado: {arquivo_7z}")
            return False, {}
        
        # Extrair metadados do nome do arquivo
        metadados = self._extrair_metadados_nome(arquivo_7z.name)
        
        if destino is None:
            # Criar subdiretório baseado no ano
            ano = self._extrair_ano_do_nome(arquivo_7z.name)
            destino = self.diretorio_destino / str(ano)
        
        destino.mkdir(parents=True, exist_ok=True)
        
        try:
            print(f"📦 Descompactando: {arquivo_7z.name}")
            
            with py7zr.SevenZipFile(arquivo_7z, mode='r') as archive:
                # Listar arquivos no .7z
                arquivos_internos = archive.getnames()
                print(f"   📋 {len(arquivos_internos)} arquivos encontrados")
                
                # Extrair todos os arquivos
                archive.extractall(path=destino)
                
                # Adicionar informações sobre arquivos extraídos aos metadados
                metadados["arquivos_extraidos"] = arquivos_internos
                metadados["data_descompactacao"] = datetime.now().isoformat()
                metadados["destino"] = str(destino)
                
            print(f"✅ Descompactado com sucesso em: {destino}")
            
            # Armazenar metadados
            self.metadados_arquivos[arquivo_7z.name] = metadados
            
            return True, metadados
            
        except Exception as e:
            print(f"❌ Erro ao descompactar {arquivo_7z.name}: {e}")
            return False, {"erro": str(e)}
    
    def descompactar_mensal(self, ano: int, mes: int) -> Tuple[bool, List[Dict]]:
        """
        Descompacta todos os arquivos de um mês específico
        
        Args:
            ano: Ano dos dados
            mes: Mês dos dados
            
        Returns:
            Tuple[bool, List[Dict]]: (Sucesso, Lista de metadados)
        """
        # Criar estrutura de pastas igual à origem
        diretorio_mes = f"{ano}{mes:02d}"
        diretorio_origem_mes = self.diretorio_origem / str(ano) / diretorio_mes
        diretorio_destino_mes = self.diretorio_destino / str(ano) / diretorio_mes
        
        # Verificar se o diretório de origem existe
        if not diretorio_origem_mes.exists():
            print(f"❌ Diretório não encontrado: {diretorio_origem_mes}")
            return False, []
        
        # Encontrar arquivos .7z no diretório específico do mês
        arquivos_7z = list(diretorio_origem_mes.glob("*.7z"))
        
        if not arquivos_7z:
            print(f"❌ Nenhum arquivo .7z encontrado para {ano}/{mes:02d}")
            return False, []
        
        print(f"🎯 Descompactando {len(arquivos_7z)} arquivos de {ano}/{mes:02d}")
        
        # Criar diretório de destino
        diretorio_destino_mes.mkdir(parents=True, exist_ok=True)
        
        sucessos = 0
        metadados_lista = []
        
        for arquivo in tqdm(arquivos_7z, desc="Descompactando"):
            # Descompactar no diretório específico do mês
            sucesso, metadados = self.descompactar_arquivo(arquivo, diretorio_destino_mes)
            if sucesso:
                sucessos += 1
                metadados_lista.append(metadados)
        
        print(f"📊 Descompactação concluída: {sucessos}/{len(arquivos_7z)} arquivos")
        
        # Criar indicador para a competência
        self._criar_indicador_competencia(ano, mes, sucessos, len(arquivos_7z))
        
        return sucessos > 0, metadados_lista
    
    def descompactar_todos(self, ano: Optional[int] = None) -> Tuple[bool, List[Dict]]:
        """
        Descompacta todos os arquivos .7z disponíveis
        
        Args:
            ano: Ano específico ou None para todos
            
        Returns:
            Tuple[bool, List[Dict]]: (Sucesso, Lista de metadados)
        """
        # Encontrar arquivos
        padrao = f"*{ano}*.7z" if ano else "*.7z"
        arquivos_7z = list(self.diretorio_origem.rglob(padrao))
        
        if not arquivos_7z:
            print(f"❌ Nenhum arquivo .7z encontrado" + (f" para {ano}" if ano else ""))
            return False, []
        
        print(f"🎯 Descompactando {len(arquivos_7z)} arquivos" + (f" de {ano}" if ano else ""))
        
        sucessos = 0
        metadados_lista = []
        
        for arquivo in tqdm(arquivos_7z, desc="Descompactando"):
            if self._arquivo_ja_descompactado(arquivo):
                print(f"⏭️  Já descompactado: {arquivo.name}")
                # Recuperar metadados se existirem
                metadados = self.metadados_arquivos.get(arquivo.name, self._extrair_metadados_nome(arquivo.name))
                metadados_lista.append(metadados)
                sucessos += 1
                continue
                
            sucesso, metadados = self.descompactar_arquivo(arquivo)
            if sucesso:
                sucessos += 1
                metadados_lista.append(metadados)
        
        print(f"📊 Descompactação concluída: {sucessos}/{len(arquivos_7z)} arquivos")
        
        # Agrupar por competência e criar indicadores
        if ano:
            self._criar_indicadores_ano(ano, metadados_lista)
        
        return sucessos > 0, metadados_lista
    
    def listar_arquivos_descompactados(self, ano: Optional[int] = None) -> List[Path]:
        """
        Lista arquivos já descompactados
        
        Args:
            ano: Ano específico ou None para todos
            
        Returns:
            List[Path]: Lista de arquivos descompactados
        """
        if ano:
            caminho = self.diretorio_destino / str(ano)
            if not caminho.exists():
                return []
            return list(caminho.glob("*.txt")) + list(caminho.glob("*.csv"))
        else:
            arquivos = []
            for subdir in self.diretorio_destino.iterdir():
                if subdir.is_dir():
                    arquivos.extend(subdir.glob("*.txt"))
                    arquivos.extend(subdir.glob("*.csv"))
            return arquivos
    
    def _extrair_ano_do_nome(self, nome_arquivo: str) -> int:
        """
        Extrai ano do nome do arquivo
        
        Args:
            nome_arquivo: Nome do arquivo
            
        Returns:
            int: Ano extraído ou ano atual como fallback
        """
        import re
        from datetime import datetime
        
        matches = re.findall(r'(20\d{2})', nome_arquivo)
        if matches:
            return int(matches[0])
        else:
            return datetime.now().year
    
    def _extrair_metadados_nome(self, nome_arquivo: str) -> Dict:
        """
        Extrai metadados do nome do arquivo
        
        Args:
            nome_arquivo: Nome do arquivo
            
        Returns:
            Dict: Metadados extraídos
        """
        metadados = {
            "nome_arquivo": nome_arquivo,
            "data_processamento": datetime.now().isoformat()
        }
        
        # Extrair ano
        ano_match = re.search(r'(20\d{2})', nome_arquivo)
        if ano_match:
            metadados["ano"] = int(ano_match.group(1))
        
        # Extrair mês
        mes_match = re.search(r'(20\d{2})(\d{2})', nome_arquivo)
        if mes_match and 1 <= int(mes_match.group(2)) <= 12:
            metadados["mes"] = int(mes_match.group(2))
        
        # Tentar identificar UF
        ufs = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", 
               "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", 
               "RO", "RR", "RS", "SC", "SE", "SP", "TO"]
        
        for uf in ufs:
            if uf in nome_arquivo:
                metadados["uf"] = uf
                break
        
        # Identificar tipo de arquivo
        if "MOVIMENTACAO" in nome_arquivo.upper():
            metadados["tipo"] = "movimentacao"
        elif "EXCLUSAO" in nome_arquivo.upper():
            metadados["tipo"] = "exclusao"
        elif "FORA_PRAZO" in nome_arquivo.upper() or "FORAPRAZO" in nome_arquivo.upper():
            metadados["tipo"] = "fora_prazo"
        else:
            metadados["tipo"] = "outros"
            
        # Criar competência formatada
        if "ano" in metadados and "mes" in metadados:
            metadados["competencia"] = f"{metadados['ano']}-{metadados['mes']:02d}"
        
        return metadados
    
    def _arquivo_ja_descompactado(self, arquivo_7z: Path) -> bool:
        """
        Verifica se arquivo já foi descompactado
        
        Args:
            arquivo_7z: Caminho do arquivo .7z
            
        Returns:
            bool: True se já foi descompactado
        """
        ano = self._extrair_ano_do_nome(arquivo_7z.name)
        destino = self.diretorio_destino / str(ano)
        
        if not destino.exists():
            return False
        
        # Verificar se há arquivos .txt ou .csv no destino
        arquivos_txt = list(destino.glob("*.txt"))
        arquivos_csv = list(destino.glob("*.csv"))
        
        return len(arquivos_txt) > 0 or len(arquivos_csv) > 0
    
    def _criar_indicador_competencia(self, ano: int, mes: int, arquivos_processados: int, total_arquivos: int) -> Indicador:
        """
        Cria um indicador para a competência processada
        
        Args:
            ano: Ano da competência
            mes: Mês da competência
            arquivos_processados: Quantidade de arquivos processados
            total_arquivos: Total de arquivos
            
        Returns:
            Indicador: Objeto com informações da competência
        """
        competencia = f"{ano}-{mes:02d}"
        taxa_sucesso = (arquivos_processados / total_arquivos) * 100 if total_arquivos > 0 else 0
        
        indicador = Indicador(
            id=0,  # ID temporário, deve ser atribuído na persistência
            cnpj="",  # Não aplicável neste contexto
            competencia=competencia,
            nome_indicador="taxa_descompactacao",
            valor=taxa_sucesso
        )
        
        print(f"📊 Indicador criado: {indicador}")
        return indicador
    
    def _criar_indicadores_ano(self, ano: int, metadados: List[Dict]) -> List[Indicador]:
        """
        Cria indicadores para o ano processado
        
        Args:
            ano: Ano dos dados
            metadados: Lista de metadados dos arquivos
            
        Returns:
            List[Indicador]: Lista de indicadores criados
        """
        # Agrupar por mês
        meses = {}
        for meta in metadados:
            if "mes" in meta:
                mes = meta["mes"]
                if mes not in meses:
                    meses[mes] = {"total": 0, "processados": 0}
                
                meses[mes]["total"] += 1
                if "arquivos_extraidos" in meta:
                    meses[mes]["processados"] += 1
        
        # Criar indicadores por mês
        indicadores = []
        for mes, dados in meses.items():
            indicador = self._criar_indicador_competencia(
                ano, mes, dados["processados"], dados["total"]
            )
            indicadores.append(indicador)
        
        return indicadores
    
    def limpar_arquivos_temporarios(self, ano: Optional[int] = None):
        """
        Remove arquivos temporários e de cache
        
        Args:
            ano: Ano específico ou None para todos
        """
        print("🧹 Limpando arquivos temporários...")
        
        if ano:
            caminhos = [self.diretorio_destino / str(ano)]
        else:
            caminhos = [self.diretorio_destino]
        
        for caminho in caminhos:
            if caminho.exists():
                # Remover arquivos temporários
                for temp_file in caminho.rglob("*.tmp"):
                    temp_file.unlink()
                    print(f"🗑️  Removido: {temp_file.name}")
    
    def gerar_relatorio_descompactacao(self, ano: Optional[int] = None) -> Dict:
        """
        Gera relatório sobre os arquivos descompactados
        
        Args:
            ano: Ano específico ou None para todos
            
        Returns:
            Dict: Relatório com estatísticas
        """
        arquivos = self.listar_arquivos_descompactados(ano)
        
        # Agrupar por tipo e extensão
        tipos = {}
        extensoes = {}
        competencias = {}
        
        for arquivo in arquivos:
            # Contar por extensão
            ext = arquivo.suffix.lower()
            extensoes[ext] = extensoes.get(ext, 0) + 1
            
            # Tentar extrair metadados do nome
            meta = self._extrair_metadados_nome(arquivo.name)
            
            # Contar por tipo
            tipo = meta.get("tipo", "desconhecido")
            tipos[tipo] = tipos.get(tipo, 0) + 1
            
            # Contar por competência
            comp = meta.get("competencia", "desconhecida")
            if comp not in competencias:
                competencias[comp] = 0
            competencias[comp] += 1
        
        # Gerar relatório
        return {
            "total_arquivos": len(arquivos),
            "por_extensao": extensoes,
            "por_tipo": tipos,
            "por_competencia": competencias
        }


# Função auxiliar para teste
def testar_descompactador():
    """
    Teste rápido do descompactador
    """
    print("🧪 Testando descompactador...")
    
    descompactador = DescompactadorCaged()
    
    # Listar arquivos .7z disponíveis
    arquivos_7z = list(descompactador.diretorio_origem.rglob("*.7z"))
    print(f"📋 Arquivos .7z encontrados: {len(arquivos_7z)}")
    
    # Listar arquivos já descompactados
    arquivos_descompactados = descompactador.listar_arquivos_descompactados()
    print(f"📁 Arquivos já descompactados: {len(arquivos_descompactados)}")
    
    # Gerar relatório
    if arquivos_descompactados:
        relatorio = descompactador.gerar_relatorio_descompactacao()
        print(f"📊 Relatório de descompactação: {relatorio}")
    
    return len(arquivos_7z) > 0


if __name__ == "__main__":
    testar_descompactador() 