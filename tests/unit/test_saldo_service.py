# -*- coding: utf-8 -*-
"""
🧪 Testes Unitários para Serviço de Saldo - CAGED

Testes para o módulo de cálculo de saldo mensal:
- SaldoService
- Cálculo de estoque com baseline fixo para janeiro de 2020
- Validação de saldos mensais e estoque acumulado
"""

import unittest
import polars as pl
from pathlib import Path
import tempfile
import shutil
from unittest.mock import Mock, patch

from src.services.saldo_service import SaldoService
from src.entities.enums import TipoMovimentacao


class TestSaldoService(unittest.TestCase):
    """Testes para SaldoService"""
    
    def setUp(self):
        """Configuração inicial para cada teste"""
        self.temp_dir = tempfile.mkdtemp()
        self.saldo_service = SaldoService(parquet_path=self.temp_dir)
        
    def tearDown(self):
        """Limpeza após cada teste"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_estoque_inicial_jan_2020_definido(self):
        """Testa se o valor de estoque inicial para janeiro de 2020 está definido corretamente"""
        self.assertEqual(self.saldo_service.ESTOQUE_INICIAL_JAN_2020, 39567082)
    
    def test_calcular_estoque_com_baseline_janeiro_2020(self):
        """Testa o cálculo de estoque com baseline para dados que incluem janeiro de 2020"""
        # Dados de teste incluindo janeiro de 2020
        df_test = pl.DataFrame({
            "cnpj": ["12345678", "12345678", "12345678"],
            "competencia": ["202001", "202002", "202003"],
            "saldo_mensal": [112113, 50000, -30000]  # Valores do dashboard para jan/2020
        })
        
        resultado = self.saldo_service._calcular_estoque_com_baseline(df_test)
        
        # Verificar se o estoque para janeiro de 2020 é o valor fixo + saldo mensal
        jan_2020_row = resultado.filter(pl.col("competencia") == "202001")
        estoque_jan_2020 = jan_2020_row.select("saldo_final").item()
        
        # Estoque janeiro = baseline + saldo mensal de janeiro
        esperado_jan_2020 = 39567082 + 112113
        self.assertEqual(estoque_jan_2020, esperado_jan_2020)
        
        # Verificar se os meses seguintes são acumulativos
        fev_2020_row = resultado.filter(pl.col("competencia") == "202002")
        estoque_fev_2020 = fev_2020_row.select("saldo_final").item()
        esperado_fev_2020 = esperado_jan_2020 + 50000
        self.assertEqual(estoque_fev_2020, esperado_fev_2020)
    
    def test_calcular_estoque_sem_janeiro_2020_posterior(self):
        """Testa o cálculo de estoque para dados posteriores a janeiro de 2020"""
        # Dados de teste posteriores a janeiro de 2020
        df_test = pl.DataFrame({
            "cnpj": ["12345678", "12345678"],
            "competencia": ["202003", "202004"],
            "saldo_mensal": [25000, -15000]
        })
        
        resultado = self.saldo_service._calcular_estoque_com_baseline(df_test)
        
        # Verificar se o primeiro mês usa o baseline como ponto de partida
        mar_2020_row = resultado.filter(pl.col("competencia") == "202003")
        estoque_mar_2020 = mar_2020_row.select("saldo_final").item()
        
        # Estoque março = baseline + saldo acumulado até março
        esperado_mar_2020 = 39567082 + 25000
        self.assertEqual(estoque_mar_2020, esperado_mar_2020)
    
    def test_calcular_estoque_sem_janeiro_2020_anterior(self):
        """Testa o cálculo de estoque para dados anteriores a janeiro de 2020"""
        # Dados de teste anteriores a janeiro de 2020
        df_test = pl.DataFrame({
            "cnpj": ["12345678", "12345678"],
            "competencia": ["201911", "201912"],
            "saldo_mensal": [100000, 50000]
        })
        
        resultado = self.saldo_service._calcular_estoque_com_baseline(df_test)
        
        # Verificar se o cálculo é normal (sem baseline) para dados anteriores
        nov_2019_row = resultado.filter(pl.col("competencia") == "201911")
        estoque_nov_2019 = nov_2019_row.select("saldo_final").item()
        
        # Para dados anteriores, não deve usar baseline
        self.assertEqual(estoque_nov_2019, 100000)
        
        dez_2019_row = resultado.filter(pl.col("competencia") == "201912")
        estoque_dez_2019 = dez_2019_row.select("saldo_final").item()
        self.assertEqual(estoque_dez_2019, 150000)  # 100000 + 50000
    
    def test_ordenacao_por_competencia(self):
        """Testa se os dados são ordenados corretamente por competência"""
        # Dados desordenados
        df_test = pl.DataFrame({
            "cnpj": ["12345678", "12345678", "12345678"],
            "competencia": ["202003", "202001", "202002"],
            "saldo_mensal": [30000, 112113, 20000]
        })
        
        resultado = self.saldo_service._calcular_estoque_com_baseline(df_test)
        
        # Verificar se está ordenado
        competencias = resultado.select("competencia").to_series().to_list()
        self.assertEqual(competencias, ["202001", "202002", "202003"])
    
    def test_remocao_coluna_auxiliar(self):
        """Testa se a coluna auxiliar 'saldo_ajustado' é removida"""
        df_test = pl.DataFrame({
            "cnpj": ["12345678"],
            "competencia": ["202001"],
            "saldo_mensal": [112113]
        })
        
        resultado = self.saldo_service._calcular_estoque_com_baseline(df_test)
        
        # Verificar se a coluna auxiliar não está presente
        self.assertNotIn("saldo_ajustado", resultado.columns)
        
        # Verificar se as colunas esperadas estão presentes
        colunas_esperadas = ["cnpj", "competencia", "saldo_mensal", "saldo_final"]
        for coluna in colunas_esperadas:
            self.assertIn(coluna, resultado.columns)
    
    def test_coluna_estoque_criada(self):
        """Testa se a coluna 'estoque' é criada corretamente no arquivo final"""
        # Simular dados completos para teste do método principal
        with patch.object(self.saldo_service, '_carregar_dados') as mock_carregar:
            # Mock dos dados de entrada com códigos válidos
            mock_df = pl.DataFrame({
                "CNPJ_RAIZ": ["12345678", "12345678", "12345678"],
                "COMPETENCIA_MOV": ["202001", "202001", "202002"],
                "TIPO_MOVIMENTACAO": [10, 31, 20]  # 10=admissão, 31=desligamento, 20=admissão
            })
            
            mock_carregar.return_value = mock_df
            
            # Executar o método principal
            self.saldo_service.calcular_e_atualizar_saldo([2020], [1, 2])
            
            # Ler o arquivo salvo para verificar o resultado
            output_path = self.saldo_service.parquet_path / "SALDOMENSAL.parquet"
            resultado = pl.read_parquet(output_path)
            
            # Verificar se a coluna 'estoque' foi criada
            self.assertIn("estoque", resultado.columns)
            
            # Verificar se estoque é igual ao saldo_final
            for i in range(resultado.height):
                estoque = resultado.row(i, named=True)["estoque"]
                saldo_final = resultado.row(i, named=True)["saldo_final"]
                self.assertEqual(estoque, saldo_final)


if __name__ == '__main__':
    unittest.main()