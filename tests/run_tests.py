#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🧪 Script de Execução de Testes - CAGED

Script para executar testes automatizados com diferentes configurações:
- Testes unitários
- Testes de integração
- Testes com cobertura
- Testes paralelos
- Relatórios de qualidade

Parte da FASE 3.3 do Plano de Melhorias CAGED.

Uso:
    python run_tests.py [opções]
    
Exemplos:
    python run_tests.py --unit                    # Apenas testes unitários
    python run_tests.py --integration             # Apenas testes de integração
    python run_tests.py --coverage                # Com relatório de cobertura
    python run_tests.py --parallel                # Execução paralela
    python run_tests.py --all                     # Todos os testes
    python run_tests.py --quick                   # Testes rápidos apenas
"""

import argparse
import subprocess
import sys
import os
from pathlib import Path
import time
from datetime import datetime


class TestRunner:
    """Executor de testes automatizados"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent  # Subir um nível da pasta tests para a raiz
        self.test_dir = self.project_root / "tests"
        self.reports_dir = self.project_root / "test_reports"
        
        # Criar diretório de relatórios
        self.reports_dir.mkdir(exist_ok=True)
    
    def run_unit_tests(self, verbose=True, coverage=False):
        """Executa testes unitários"""
        print("🧪 Executando testes unitários...")
        
        cmd = ["python", "-m", "pytest", "tests/unit"]
        
        if verbose:
            cmd.append("-v")
        
        if coverage:
            cmd.extend([
                "--cov=src",
                "--cov-report=html:test_reports/htmlcov_unit",
                "--cov-report=term-missing"
            ])
        
        return self._run_command(cmd)
    
    def run_integration_tests(self, verbose=True, coverage=False):
        """Executa testes de integração"""
        print("🔗 Executando testes de integração...")
        
        cmd = ["python", "-m", "pytest", "tests/integration"]
        
        if verbose:
            cmd.append("-v")
        
        if coverage:
            cmd.extend([
                "--cov=src",
                "--cov-report=html:test_reports/htmlcov_integration",
                "--cov-report=term-missing"
            ])
        
        return self._run_command(cmd)
    
    def run_all_tests(self, verbose=True, coverage=True, parallel=False):
        """Executa todos os testes"""
        print("🚀 Executando todos os testes...")
        
        cmd = ["python", "-m", "pytest", "tests"]
        
        if verbose:
            cmd.append("-v")
        
        if coverage:
            cmd.extend([
                "--cov=src",
                "--cov-report=html:test_reports/htmlcov_all",
                "--cov-report=term-missing",
                "--cov-report=xml:test_reports/coverage.xml",
                "--cov-fail-under=80"
            ])
        
        if parallel:
            cmd.extend(["-n", "auto"])
        
        # Adicionar relatório JUnit para CI/CD
        cmd.extend(["--junitxml=test_reports/junit.xml"])
        
        return self._run_command(cmd)
    
    def run_quick_tests(self):
        """Executa apenas testes rápidos (exclui testes marcados como 'slow')"""
        print("⚡ Executando testes rápidos...")
        
        cmd = [
            "python", "-m", "pytest", "tests",
            "-m", "not slow",
            "-v",
            "--tb=short"
        ]
        
        return self._run_command(cmd)
    
    def run_specific_marker(self, marker, verbose=True):
        """Executa testes com marcador específico"""
        print(f"🏷️ Executando testes marcados como '{marker}'...")
        
        cmd = [
            "python", "-m", "pytest", "tests",
            "-m", marker
        ]
        
        if verbose:
            cmd.append("-v")
        
        return self._run_command(cmd)
    
    def run_performance_tests(self):
        """Executa testes de performance com profiling"""
        print("📊 Executando testes de performance...")
        
        cmd = [
            "python", "-m", "pytest", "tests",
            "-m", "slow or integration",
            "--durations=20",
            "-v"
        ]
        
        return self._run_command(cmd)
    
    def run_with_profiling(self):
        """Executa testes com profiling detalhado"""
        print("🔍 Executando testes com profiling...")
        
        try:
            # Instalar pytest-profiling se não estiver disponível
            subprocess.run(["pip", "install", "pytest-profiling"], 
                         capture_output=True, check=False)
            
            cmd = [
                "python", "-m", "pytest", "tests",
                "--profile",
                "--profile-svg",
                "-v"
            ]
            
            return self._run_command(cmd)
            
        except Exception as e:
            print(f"❌ Erro no profiling: {e}")
            return False
    
    def generate_quality_report(self):
        """Gera relatório de qualidade do código"""
        print("📋 Gerando relatório de qualidade...")
        
        reports = []
        
        # 1. Executar testes com cobertura
        print("  📊 Executando análise de cobertura...")
        coverage_result = self.run_all_tests(coverage=True, verbose=False)
        reports.append(("Cobertura de Testes", coverage_result))
        
        # 2. Análise estática com flake8 (se disponível)
        print("  🔍 Executando análise estática...")
        try:
            subprocess.run(["pip", "install", "flake8"], 
                         capture_output=True, check=False)
            

            flake8_cmd = [
                "flake8", "src", "tests",
                "--output-file=test_reports/flake8_report.txt",
                "--max-line-length=100",
                "--ignore=E203,W503"
            ]
            
            flake8_result = self._run_command(flake8_cmd, capture_output=True)
            reports.append(("Análise Estática (flake8)", flake8_result))
            
        except Exception as e:
            print(f"  ⚠️ Análise estática não disponível: {e}")
        
        # 3. Análise de complexidade (se disponível)
        print("  📈 Executando análise de complexidade...")
        try:
            subprocess.run(["pip", "install", "radon"], 
                         capture_output=True, check=False)
            
            radon_cmd = [
                "radon", "cc", "src",
                "--json",
                "--output-file=test_reports/complexity_report.json"
            ]
            
            radon_result = self._run_command(radon_cmd, capture_output=True)
            reports.append(("Análise de Complexidade (radon)", radon_result))
            
        except Exception as e:
            print(f"  ⚠️ Análise de complexidade não disponível: {e}")
        
        # 4. Gerar relatório consolidado
        self._generate_consolidated_report(reports)
        
        return all(result for _, result in reports)
    
    def run_ci_pipeline(self):
        """Executa pipeline completo para CI/CD"""
        print("🚀 Executando pipeline de CI/CD...")
        
        steps = [
            ("Testes Unitários", lambda: self.run_unit_tests(coverage=True)),
            ("Testes de Integração", lambda: self.run_integration_tests(coverage=True)),
            ("Testes Completos", lambda: self.run_all_tests(coverage=True, parallel=True)),
            ("Relatório de Qualidade", lambda: self.generate_quality_report())
        ]
        
        results = []
        start_time = time.time()
        
        for step_name, step_func in steps:
            print(f"\n📋 Executando: {step_name}")
            step_start = time.time()
            
            try:
                result = step_func()
                step_time = time.time() - step_start
                
                status = "✅ SUCESSO" if result else "❌ FALHA"
                print(f"  {status} ({step_time:.2f}s)")
                
                results.append((step_name, result, step_time))
                
                if not result:
                    print(f"❌ Pipeline interrompido na etapa: {step_name}")
                    break
                    
            except Exception as e:
                step_time = time.time() - step_start
                print(f"  ❌ ERRO: {e} ({step_time:.2f}s)")
                results.append((step_name, False, step_time))
                break
        
        total_time = time.time() - start_time
        
        # Relatório final
        print(f"\n📊 RELATÓRIO FINAL DO PIPELINE")
        print(f"⏱️ Tempo total: {total_time:.2f}s")
        print(f"📋 Etapas executadas: {len(results)}/{len(steps)}")
        
        success_count = sum(1 for _, success, _ in results if success)
        print(f"✅ Sucessos: {success_count}")
        print(f"❌ Falhas: {len(results) - success_count}")
        
        for step_name, success, step_time in results:
            status = "✅" if success else "❌"
            print(f"  {status} {step_name}: {step_time:.2f}s")
        
        return all(success for _, success, _ in results)
    
    def _run_command(self, cmd, capture_output=False):
        """Executa comando e retorna resultado"""
        try:
            print(f"🔧 Executando: {' '.join(cmd)}")
            
            if capture_output:
                result = subprocess.run(cmd, capture_output=True, text=True, cwd=self.project_root)
                return result.returncode == 0
            else:
                result = subprocess.run(cmd, cwd=self.project_root)
                return result.returncode == 0
                
        except Exception as e:
            print(f"❌ Erro ao executar comando: {e}")
            return False
    
    def _generate_consolidated_report(self, reports):
        """Gera relatório consolidado"""
        report_file = self.reports_dir / "quality_report.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"# Relatório de Qualidade - CAGED\n\n")
            f.write(f"**Data:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## Resumo\n\n")
            
            for report_name, success in reports:
                status = "✅ PASSOU" if success else "❌ FALHOU"
                f.write(f"- **{report_name}:** {status}\n")
            
            f.write("\n## Arquivos de Relatório\n\n")
            f.write("- [Cobertura HTML](htmlcov_all/index.html)\n")
            f.write("- [Relatório JUnit](junit.xml)\n")
            f.write("- [Cobertura XML](coverage.xml)\n")
            
            if (self.reports_dir / "flake8_report.txt").exists():
                f.write("- [Análise Estática](flake8_report.txt)\n")
            
            if (self.reports_dir / "complexity_report.json").exists():
                f.write("- [Análise de Complexidade](complexity_report.json)\n")
        
        print(f"📋 Relatório consolidado salvo em: {report_file}")


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(
        description="Executor de testes automatizados para CAGED",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python run_tests.py --unit                    # Apenas testes unitários
  python run_tests.py --integration             # Apenas testes de integração
  python run_tests.py --all --coverage          # Todos os testes com cobertura
  python run_tests.py --quick                   # Testes rápidos apenas
  python run_tests.py --marker cache            # Testes de cache apenas
  python run_tests.py --ci                      # Pipeline completo de CI/CD
        """
    )
    
    # Grupos de testes
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--unit", action="store_true", help="Executar apenas testes unitários")
    group.add_argument("--integration", action="store_true", help="Executar apenas testes de integração")
    group.add_argument("--all", action="store_true", help="Executar todos os testes")
    group.add_argument("--quick", action="store_true", help="Executar apenas testes rápidos")
    group.add_argument("--ci", action="store_true", help="Executar pipeline completo de CI/CD")
    
    # Opções específicas
    parser.add_argument("--marker", help="Executar testes com marcador específico")
    parser.add_argument("--coverage", action="store_true", help="Incluir relatório de cobertura")
    parser.add_argument("--parallel", action="store_true", help="Executar testes em paralelo")
    parser.add_argument("--performance", action="store_true", help="Executar testes de performance")
    parser.add_argument("--profile", action="store_true", help="Executar com profiling")
    parser.add_argument("--quality", action="store_true", help="Gerar relatório de qualidade")
    parser.add_argument("--verbose", "-v", action="store_true", default=True, help="Saída verbosa")
    parser.add_argument("--quiet", "-q", action="store_true", help="Saída silenciosa")
    
    args = parser.parse_args()
    
    # Ajustar verbosidade
    verbose = args.verbose and not args.quiet
    
    # Criar executor
    runner = TestRunner()
    
    # Verificar se pytest está instalado
    try:
        subprocess.run(["python", "-m", "pytest", "--version"], 
                      capture_output=True, check=True)
    except subprocess.CalledProcessError:
        print("❌ pytest não está instalado. Instale com: pip install pytest pytest-cov")
        return 1
    
    # Executar testes baseado nos argumentos
    success = True
    
    try:
        if args.ci:
            success = runner.run_ci_pipeline()
        elif args.unit:
            success = runner.run_unit_tests(verbose=verbose, coverage=args.coverage)
        elif args.integration:
            success = runner.run_integration_tests(verbose=verbose, coverage=args.coverage)
        elif args.all:
            success = runner.run_all_tests(verbose=verbose, coverage=args.coverage, parallel=args.parallel)
        elif args.quick:
            success = runner.run_quick_tests()
        elif args.marker:
            success = runner.run_specific_marker(args.marker, verbose=verbose)
        elif args.performance:
            success = runner.run_performance_tests()
        elif args.profile:
            success = runner.run_with_profiling()
        elif args.quality:
            success = runner.generate_quality_report()
        else:
            # Padrão: executar testes rápidos
            print("ℹ️ Nenhuma opção especificada. Executando testes rápidos...")
            print("   Use --help para ver todas as opções disponíveis.")
            success = runner.run_quick_tests()
        
        # Resultado final
        if success:
            print("\n✅ Todos os testes foram executados com sucesso!")
            return 0
        else:
            print("\n❌ Alguns testes falharam. Verifique a saída acima.")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário.")
        return 130
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())