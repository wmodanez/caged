# Sistema de Métricas CAGED

O sistema CAGED inclui um sistema abrangente de métricas para monitoramento de performance e análise de operações.

## Visão Geral

O sistema de métricas coleta automaticamente dados sobre:
- ⏱️ **Tempo de execução** de operações
- ✅ **Taxa de sucesso/falha** das operações
- 💻 **Recursos do sistema** (CPU, memória, disco)
- 📊 **Estatísticas detalhadas** por tipo de operação
- ⚠️ **Alertas de performance** automáticos

## Tipos de Operações Monitoradas

### Downloads
- Tempo de download por arquivo
- Taxa de sucesso/falha
- Tamanho dos arquivos
- Servidor de origem

### Extração/Descompactação
- Tempo de extração por arquivo
- Número de arquivos extraídos
- Tamanho total em bytes
- Método de descompactação (paralelo/sequencial)

### Conversão
- Tempo de conversão mensal
- Número de registros processados
- Arquivos processados vs. total
- Uso de paralelismo

## Usando o CLI de Métricas

### Comandos Básicos

```bash
# Visualizar todas as métricas em formato tabela
python main.py metrics

# Visualizar em formato JSON
python main.py metrics --format json

# Visualizar apenas resumo
python main.py metrics --format summary

# Visualizar apenas alertas de performance
python main.py metrics --alerts-only
```

### Filtros

```bash
# Filtrar por tipo de operação
python main.py metrics --operation download
python main.py metrics --operation extract_file
python main.py metrics --operation convert_monthly

# Filtrar por data (últimas 24 horas)
python main.py metrics --since "2024-01-01"

# Combinar filtros
python main.py metrics --operation download --since "2024-01-01" --format json
```

### Persistência

```bash
# Salvar métricas em arquivo
python main.py metrics --save metricas_backup.json

# Limpar todas as métricas
python main.py metrics --clear
```

## Usando Programaticamente

### Importação Básica

```python
from utils.metrics import get_metrics_collector, record_operation
import time

# Obter o coletor de métricas
metrics = get_metrics_collector()
```

### Registrando Operações

```python
# Registrar uma operação simples
record_operation(
    operation="download",
    success=True,
    duration=2.5,
    details={
        "arquivo": "CAGED_202401.7z",
        "tamanho_mb": 150
    }
)

# Registrar com contexto de tempo automático
inicio = time.time()
# ... sua operação aqui ...
record_operation(
    operation="extract_file",
    success=True,
    duration=time.time() - inicio,
    details={
        "arquivo": "CAGED_202401.7z",
        "arquivos_extraidos": 25,
        "tamanho_bytes": 157286400
    }
)
```

### Coletando Relatórios

```python
# Gerar relatório completo
relatorio = metrics.generate_report()
print(f"Total de operações: {relatorio['total_operations']}")
print(f"Taxa de sucesso: {relatorio['success_rate']:.1%}")

# Obter métricas do sistema
recursos = metrics.collect_system_metrics()
print(f"CPU: {recursos['cpu_percent']:.1f}%")
print(f"Memória: {recursos['memory_percent']:.1f}%")

# Obter operações recentes
recentes = metrics.get_recent_operations(limit=10)
for op in recentes:
    print(f"{op['operation']}: {op['duration']:.2f}s")

# Verificar alertas de performance
alertas = metrics.get_performance_alerts()
for alerta in alertas:
    print(f"⚠️ {alerta['type']}: {alerta['message']}")
```

## Alertas de Performance

O sistema gera alertas automáticos para:

### Operações Lentas
- Downloads > 30 segundos
- Extrações > 60 segundos
- Conversões > 300 segundos (5 minutos)

### Alta Taxa de Falha
- Taxa de falha > 20% em qualquer operação
- Múltiplas falhas consecutivas

### Recursos do Sistema
- CPU > 90% por período prolongado
- Memória > 85%
- Disco > 95%


## Configuração

### Limites de Alerta

Os limites podem ser configurados no arquivo de configuração:

```python
# Em config/settings.py
METRICS_CONFIG = {
    'alert_thresholds': {
        'slow_download': 30.0,      # segundos
        'slow_extraction': 60.0,    # segundos
        'slow_conversion': 300.0,   # segundos
        'high_failure_rate': 0.2,   # 20%
        'high_cpu': 0.9,           # 90%
        'high_memory': 0.85,       # 85%
        'high_disk': 0.95          # 95%
    },
    'retention_days': 30,          # dias para manter métricas
    'max_operations': 10000        # máximo de operações em memória
}
```

### Persistência Automática

```python
# Salvar métricas automaticamente a cada N operações
metrics.enable_auto_save(
    filename="metricas_auto.json",
    interval_operations=100
)

# Ou salvar periodicamente (a cada X minutos)
metrics.enable_periodic_save(
    filename="metricas_periodicas.json",
    interval_minutes=15
)
```

## Exemplos Práticos

### Monitoramento de Sessão Completa

```python
def processar_com_metricas(ano, mes):
    """Exemplo de processamento completo com métricas"""
    
    # Download
    inicio = time.time()
    sucesso_download = fazer_download(ano, mes)
    record_operation(
        operation="download",
        success=sucesso_download,
        duration=time.time() - inicio,
        details={"ano": ano, "mes": mes}
    )
    
    if not sucesso_download:
        return False
    
    # Extração
    inicio = time.time()
    sucesso_extracao = extrair_arquivos(ano, mes)
    record_operation(
        operation="extract_monthly",
        success=sucesso_extracao,
        duration=time.time() - inicio,
        details={"ano": ano, "mes": mes}
    )
    
    if not sucesso_extracao:
        return False
    
    # Conversão
    inicio = time.time()
    sucesso_conversao = converter_dados(ano, mes)
    record_operation(
        operation="convert_monthly",
        success=sucesso_conversao,
        duration=time.time() - inicio,
        details={"ano": ano, "mes": mes}
    )
    
    return sucesso_conversao
```

### Análise de Performance

```python
def analisar_performance():
    """Análise detalhada de performance"""
    metrics = get_metrics_collector()
    
    # Operações mais lentas
    operacoes = metrics.get_recent_operations(limit=100)
    mais_lentas = sorted(operacoes, key=lambda x: x['duration'], reverse=True)[:5]
    
    print("🐌 Operações mais lentas:")
    for op in mais_lentas:
        print(f"  {op['operation']}: {op['duration']:.2f}s")
    
    # Taxa de sucesso por tipo
    relatorio = metrics.generate_report()
    if 'success_rate_by_type' in relatorio:
        print("\n📊 Taxa de sucesso por tipo:")
        for tipo, taxa in relatorio['success_rate_by_type'].items():
            print(f"  {tipo}: {taxa:.1%}")
```

## Troubleshooting

### Problemas Comuns

1. **Métricas não aparecem**
   - Verifique se as operações estão sendo registradas com `record_operation()`
   - Confirme que o coletor de métricas está sendo inicializado

2. **Alertas excessivos**
   - Ajuste os limites em `METRICS_CONFIG`
   - Verifique se o sistema não está sobrecarregado

3. **Performance degradada**
   - Limite o número de métricas em memória
   - Use persistência automática para liberar memória
   - Configure retenção adequada

### Logs de Debug

```python
import logging
logging.getLogger('utils.metrics').setLevel(logging.DEBUG)
```

## Integração com Monitoramento Externo

O sistema pode ser integrado com ferramentas de monitoramento:

```python
# Exportar para Prometheus
metrics.export_prometheus_metrics()

# Enviar para InfluxDB
metrics.send_to_influxdb(
    host="localhost",
    port=8086,
    database="caged_metrics"
)

# Webhook para alertas
metrics.configure_webhook_alerts(
    url="https://hooks.slack.com/...",
    severity_filter="critical"
)
```