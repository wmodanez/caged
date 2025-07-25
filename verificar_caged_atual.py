#!/usr/bin/env python3
"""
Script para verificar informações atualizadas sobre o CAGED
"""

import requests
import re

def verificar_caged_atual():
    """Verifica informações atualizadas sobre o CAGED"""
    
    print("🔍 Verificando informações atualizadas sobre o CAGED...")
    
    # URLs possíveis para informações sobre o CAGED
    urls = [
        "https://www.gov.br/trabalho-e-previdencia/pt-br",
        "https://www.gov.br/trabalho-e-previdencia/pt-br/assuntos/estatisticas/caged",
        "https://www.gov.br/trabalho-e-previdencia/pt-br/assuntos/estatisticas"
    ]
    
    for url in urls:
        try:
            print(f"\n📡 Verificando: {url}")
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                print(f"✅ URL acessível: {url}")
                
                # Procurar por informações sobre CAGED
                conteudo = response.text.lower()
                
                if "caged" in conteudo:
                    print("📋 Encontradas referências ao CAGED")
                    
                    # Extrair possíveis links FTP
                    links_ftp = re.findall(r'ftp://[^\s"<>]+', response.text)
                    if links_ftp:
                        print("🔗 Links FTP encontrados:")
                        for link in links_ftp:
                            print(f"   {link}")
                    
                    # Extrair possíveis caminhos de diretório
                    caminhos = re.findall(r'/[a-zA-Z0-9_/-]+caged[a-zA-Z0-9_/-]*', response.text, re.IGNORECASE)
                    if caminhos:
                        print("📁 Caminhos encontrados:")
                        for caminho in set(caminhos):
                            print(f"   {caminho}")
                
            else:
                print(f"❌ URL não acessível: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Erro ao acessar {url}: {e}")
    
    print("\n💡 Sugestões:")
    print("1. Verificar se o CAGED foi migrado para outro servidor")
    print("2. Verificar se há um novo formato de dados")
    print("3. Verificar se os dados estão disponíveis via API")
    print("4. Verificar se há restrições de acesso")

if __name__ == "__main__":
    verificar_caged_atual() 