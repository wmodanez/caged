"""Configurações para validação e tratamento de erros do sistema CAGED."""

from typing import Dict, List, Any

# Configurações de encoding
ENCODING_CONFIG = {
    'encodings_prioritarios': [
        'utf-8', 'latin1', 'cp1252', 'iso-8859-1', 'cp850'
    ],
    'confianca_minima': 0.7,
    'tamanho_amostra': 8192,
    'tentativas_maximas': 3
}

# Configurações de validação de campos
VALIDACAO_CAMPOS_CONFIG = {
    'cobertura_minima_essencial': 50.0,  # Porcentagem mínima de campos essenciais
    'cobertura_minima_opcional': 25.0,   # Porcentagem mínima de campos opcionais
    'permitir_processamento_parcial': True,
    'log_detalhado': True
}

# Configurações de tipos de movimentação
MOVIMENTACAO_CONFIG = {
    'codigos_admissao_principais': ["10", "20", "25", "35", "70", "97"],
    'codigos_admissao_alternativos': ["1", "2", "3", "5", "7"],
    'codigos_desligamento_principais': ["31", "32", "33", "40", "43", "45", "50", "60", "80", "90", "98", "99"],
    'codigos_desligamento_alternativos': ["4", "6", "8", "9"],
    'codigos_textuais_admissao': ["ADMISSAO", "ADMISSÃO", "ADMIT", "A"],
    'codigos_textuais_desligamento': ["DESLIGAMENTO", "DEMISSAO", "DEMISSÃO", "DESLIG", "D"],
    'log_tipos_nao_reconhecidos': True,
    'limite_log_tipos': 10
}

# Configurações de tratamento de colunas duplicadas
COLUNAS_DUPLICADAS_CONFIG = {
    'estrategia': 'sufixo_numerico',  # 'sufixo_numerico', 'manter_primeira', 'manter_ultima'
    'sufixo_padrao': '_dup',
    'log_duplicatas': True,
    'resolver_automaticamente': True
}

# Configurações de tipos de dados
TIPOS_DADOS_CONFIG = {
    'tentativas_conversao': 3,
    'usar_conversao_robusta': True,
    'preencher_nulos': True,
    'valores_padrao': {
        'int': 0,
        'float': 0.0,
        'string': '',
        'bool': False
    },
    'detectar_automaticamente': True
}

# Configurações de recuperação de erros
RECUPERACAO_CONFIG = {
    'tentativas_maximas': 3,
    'usar_fallbacks': True,
    'log_recuperacao': True,
    'continuar_com_erros_parciais': True,
    'limite_erros_por_arquivo': 100
}

# Mensagens de erro padronizadas
MENSAGENS_ERRO = {
    'encoding_nao_detectado': "Não foi possível detectar o encoding do arquivo. Usando fallback: {encoding}",
    'campos_essenciais_ausentes': "Campos essenciais ausentes: {campos}. Cobertura: {cobertura}%",
    'colunas_duplicadas': "Colunas duplicadas detectadas: {colunas}. Aplicando resolução automática.",
    'tipos_movimentacao_invalidos': "Tipos de movimentação não reconhecidos: {tipos}",
    'conversao_tipos_falhou': "Falha na conversão de tipos para coluna {coluna}: {erro}",
    'estrutura_invalida': "Estrutura do arquivo não atende aos requisitos mínimos do CAGED",
    'processamento_parcial': "Processamento continuará com estrutura parcial devido a {motivo}"
}

# Configurações de logging
LOGGING_CONFIG = {
    'nivel_detalhamento': 'INFO',  # 'DEBUG', 'INFO', 'WARNING', 'ERROR'
    'log_progresso': True,
    'log_estatisticas': True,
    'log_recuperacao': True,
    'formato_timestamp': '%Y-%m-%d %H:%M:%S'
}

def obter_config_completa() -> Dict[str, Any]:
    """Retorna todas as configurações em um dicionário único."""
    return {
        'encoding': ENCODING_CONFIG,
        'validacao_campos': VALIDACAO_CAMPOS_CONFIG,
        'movimentacao': MOVIMENTACAO_CONFIG,
        'colunas_duplicadas': COLUNAS_DUPLICADAS_CONFIG,
        'tipos_dados': TIPOS_DADOS_CONFIG,
        'recuperacao': RECUPERACAO_CONFIG,
        'mensagens': MENSAGENS_ERRO,
        'logging': LOGGING_CONFIG
    }

def validar_configuracao() -> bool:
    """Valida se todas as configurações estão corretas."""
    try:
        config = obter_config_completa()
        
        # Validações básicas
        assert 0 <= config['validacao_campos']['cobertura_minima_essencial'] <= 100
        assert 0 <= config['validacao_campos']['cobertura_minima_opcional'] <= 100
        assert config['recuperacao']['tentativas_maximas'] > 0
        assert config['encoding']['confianca_minima'] > 0
        
        return True
    except (AssertionError, KeyError) as e:
        print(f"Erro na validação da configuração: {e}")
        return False