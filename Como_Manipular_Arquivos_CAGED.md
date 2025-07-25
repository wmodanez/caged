# Como Manipular Arquivos do Novo CAGED

Este documento detalha o propósito, manipulação e tratamento dos arquivos CAGEDMOV, CAGEDFOR e CAGEDEXC, além de explicar como calcular o saldo de contratações considerando exclusões e movimentações fora do prazo, conforme a Nota Técnica do Ministério do Trabalho e Previdência (nov/2021).

## 0. Contexto: Novo CAGED e Legado

Desde janeiro de 2020, o Novo CAGED passou a utilizar majoritariamente o Sistema de Escrituração Digital das Obrigações Fiscais, Previdenciárias e Trabalhistas (eSocial) para a captação de dados de admissões e desligamentos, conforme Portaria SEPRT nº 1.127/2019. O envio pelo sistema CAGED tradicional permanece obrigatório apenas para órgãos públicos e organizações internacionais que contratam celetistas.

Durante o período de transição, muitas empresas deixaram de informar desligamentos ao eSocial. Para garantir a qualidade e integridade das estatísticas, foi adotada uma metodologia de imputação de dados, utilizando informações do eSocial, CAGED tradicional e Empregador Web. Assim, o Novo CAGED é um sistema híbrido, consolidando dados dessas três fontes.

A SEPRT realiza o ajuste técnico e a divulgação das estatísticas do emprego formal com base nesses registros administrativos, assegurando segurança metodológica e transparência.

Os microdados do Novo CAGED são disponibilizados mensalmente em formato .txt, a nível de estabelecimento, UF, município e setor de atividade econômica. Posteriormente, os dados também serão disponibilizados a nível de movimentações.

Para mais detalhes sobre a metodologia, consulte a Nota Técnica e os materiais oficiais disponíveis nos links do Ministério do Trabalho e Previdência.

## 1. Finalidade dos Arquivos

- **CAGEDMOVAAAAMM**: Movimentações declaradas dentro do prazo, com competência de declaração igual a AAAAMM.
- **CAGEDFORAAAAMM**: Movimentações declaradas fora do prazo, com competência de declaração igual a AAAAMM. Devem ser incorporadas retroativamente ao mês correto.
- **CAGEDEXCAAAAMM**: Movimentações excluídas, com competência de declaração da exclusão igual a AAAAMM. Cancelam movimentações informadas anteriormente.

## 2. Manipulação dos Arquivos

### 2.1. Download e Descompactação

1. Baixe os arquivos do FTP do Novo CAGED.
2. Descompacte os arquivos (geralmente em formato .zip).

### 2.2. Leitura dos Arquivos

- Os arquivos podem estar em formato texto (CSV/TXT) ou Parquet.
- Utilize bibliotecas como `pandas` para leitura:

### 2.3. Estrutura dos Dados

- Consulte os layouts oficiais na pasta `docs/` para entender os campos de cada arquivo.
- Utilize as chaves: CNPJ/CPF, competência e tipo de movimentação para cruzamento e evitar duplicidades.

## 2.4. Estrutura dos Arquivos CSV nas Subpastas de `docs/`

A pasta `docs/` contém subpastas com arquivos CSV que detalham os códigos e descrições de variáveis utilizadas nos microdados do Novo CAGED, tanto para o legado quanto para o novo modelo. Essas tabelas são essenciais para decodificar campos categóricos e compreender a estrutura dos dados. Abaixo, um resumo das principais subpastas e seus arquivos:

### 2.4.1. Layout Não-identificado Novo Caged Movimentação

- Contém arquivos CSV de dicionários de códigos para variáveis como: `categoria`, `cbo2002ocupação`, `graudeinstrução`, `indicadoraprendiz`, `indicadordeexclusão`, `indicadordeforadoprazo`, `indtrabintermitente`, `indtrabparcial`, `município`, `raçacor`, `região`, `seção`, `sexo`, `subclasse`, `tamestabjan`, `tipodedeficiência`, `tipoempregador`, `tipoestabelecimento`, `tipomovimentação`, `uf`, `unidadesaláriocódigo`, entre outros.
- O arquivo `Layout_Layout Não-identificado Novo Caged Movimentação_csv` traz o layout completo dos microdados, com as colunas: Variável, Descrição e Código. Exemplo de campos:
  - `competenciamov`: Competência da movimentação (AAAAMM)
  - `região`, `uf`, `município`: Localização segundo IBGE
  - `seção`, `subclasse`: CNAE 2.0
  - `categoria`, `cbo2002ocupação`, `graudeinstrução`, `raçacor`, `sexo`, etc.
  - `salário`, `valorsaláriofixo`, `unidadesaláriocódigo`
  - Indicadores: `indicadoraprendiz`, `indtrabintermitente`, `indtrabparcial`, `indicadordeexclusão`, `indicadordeforadoprazo`

### 2.4.2. LEGADO - Layout Novo Caged Movimentação

- Estrutura semelhante à anterior, mas referente ao modelo legado (até 2019).
- Arquivos CSV de códigos para variáveis como: `categoria`, `cbo2002ocupação`, `graudeinstrução`, `indicadoraprendiz`, `indtrabintermitente`, `indtrabparcial`, `município`, `raçacor`, `região`, `seção`, `sexo`, `subclasse`, `tamestabjan`, `tipodedeficiência`, `tipoempregador`, `tipoestabelecimento`, `tipomovimentação`, `uf`, `fonte_desl`.
- O arquivo `Layout_Layout Novo Caged Movimentação_csv` traz o layout dos microdados do legado, com campos como:
  - `competencia`, `região`, `uf`, `município`, `seção`, `subclasse`, `saldomovimentação`, `cbo2002ocupação`, `categoria`, `graudeinstrução`, `idade`, `horascontratuais`, `raçacor`, `sexo`, `tipoempregador`, `tipoestabelecimento`, `tipomovimentação`, `tipodedeficiência`, `indtrabintermitente`, `indtrabparcial`, `salário`, `tamestabjan`, `indicadoraprendiz`, `fonte`

### 2.4.3. LEGADO - Layout Novo Caged Estabelecimento

- Contém arquivos CSV de códigos para variáveis de estabelecimentos: `fonte_desl`, `subclasse`, `seção`, `Município`, `UF`, `Região`, `tipoempregador`, `tipoestabelecimento`, `tamestabjan`.
- O arquivo `Layout_Layout Novo Caged Estabelecimento_csv` traz o layout dos microdados de estabelecimentos, com campos como:
  - `competencia`, `região`, `uf`, `município`, `seção`, `subclasse`, `admitidos`, `desligados`, `fonte_desl`, `saldomovimentação`, `tipoempregador`, `tipoestabelecimento`, `tamestabjan`

#### Observação

Cada arquivo CSV de código serve como dicionário para decodificar os valores numéricos/categóricos presentes nos microdados. Consulte sempre o arquivo de layout correspondente para garantir a correta interpretação dos campos.

---

## 3. Consolidação e Imputação de Dados

- O Novo CAGED utiliza dados do eSocial, CAGED tradicional e Empregador Web.
- Quando há duplicidade (mesma movimentação em mais de uma fonte), prevalece o dado do eSocial.
- Demissões do Empregador Web são usadas apenas se não houver registro em eSocial/CAGED.
- A consolidação é feita cruzando CNPJ/CPF, competência e tipo de movimentação.

## 4. Tratamento de Exclusões e Movimentações Fora do Prazo

### 4.1. Exclusões (CAGEDEXC)

- Cancelam movimentações informadas anteriormente.
- Exclusão de admissão reduz o saldo.
- Exclusão de desligamento aumenta o saldo.
- Devem ser aplicadas no mês em que foram informadas.

### 4.2. Movimentações Fora do Prazo (CAGEDFOR)

- Devem ser incorporadas retroativamente ao mês de competência original.
- Ajustam os saldos históricos.

## 5. Cálculo do Saldo de Contratações

O saldo de contratações é dado por:

```
Saldo = (Admissões - Desligamentos) + (Exclusão de Desligamentos - Exclusão de Admissões)
```

### Passos para o cálculo':'

1. **Some todas as admissões** dos arquivos CAGEDMOV e CAGEDFOR.
2. **Some todos os desligamentos** dos arquivos CAGEDMOV e CAGEDFOR.
3. **Some as exclusões de admissões** do arquivo CAGEDEXC (diminuem o saldo).
4. **Some as exclusões de desligamentos** do arquivo CAGEDEXC (aumentam o saldo).
5. **Aplique a fórmula acima** para obter o saldo final do mês.

### Observações Importantes

- As movimentações fora do prazo devem ser incorporadas retroativamente ao mês correto.
- As exclusões devem ser aplicadas no mês em que foram informadas, ajustando o saldo daquele mês.
- O saldo deve ser recalculado sempre que houver atualização retroativa (ex: inclusão de movimentação fora do prazo ou exclusão).

#### Sobre a Tabela de Saldo Mensal e Atualizações Fora do Prazo

- Para cada mês, recomenda-se manter uma tabela (ou registro) de saldo, pois o saldo depende dos eventos daquele mês (admissões, desligamentos, exclusões, etc.).
- Caso ocorram retificações ou exclusões de eventos de meses anteriores (registros fora do prazo), o saldo do mês de referência do evento deve ser atualizado, mesmo que esse mês já tenha passado.
- Dependendo do controle adotado, pode ser necessário recalcular os saldos dos meses seguintes, pois um ajuste em um mês pode afetar o saldo acumulado.
- O fluxo sugerido é:
  1. Criar uma tabela de saldo para cada mês (ou uma tabela única com coluna de mês).
  2. Ao processar um novo mês, calcular o saldo normalmente.
  3. Se houver registros fora do prazo, atualizar o saldo do mês de referência do evento.
  4. (Opcional) Recalcular os saldos dos meses seguintes, se trabalhar com saldo acumulado.
- Dessa forma, garante-se que os saldos mensais e acumulados reflitam corretamente todas as movimentações, inclusive as retroativas.

## 6. Recomendações Práticas

- Sempre utilize as chaves CNPJ/CPF/Competência/Tipo de movimentação para evitar duplicidades.
- Mantenha controle de versões dos dados processados, pois os saldos históricos podem ser alterados por movimentações retroativas.
- Consulte os layouts oficiais para garantir a correta identificação dos campos de cada arquivo.
- Documente eventuais ajustes ou regras de negócio aplicadas.

## 7. Casos Práticos e Situações Comuns

### 7.1. Duplicidade de Movimentações

- **Situação:** Uma mesma admissão/desligamento aparece tanto no eSocial quanto no CAGED tradicional.
- **Como tratar:** Utilize as chaves (CNPJ/CPF/Competência/Tipo de movimentação) para identificar duplicidade. Sempre dê prioridade ao registro do eSocial.

### 7.2. Movimentações Retroativas (Fora do Prazo)

- **Situação:** Uma empresa declara uma admissão de março apenas em junho (fora do prazo).
- **Como tratar:** Incorpore essa movimentação ao saldo de março, ajustando o histórico. O saldo de junho não é afetado diretamente, mas o saldo acumulado de meses posteriores deve ser recalculado.

### 7.3. Exclusões em Massa

- **Situação:** Uma empresa percebe que enviou várias admissões erradas e faz exclusão em lote em um mês posterior.
- **Como tratar:** As exclusões devem ser aplicadas no mês em que foram informadas, ajustando o saldo daquele mês. O histórico deve indicar que houve correção retroativa.

### 7.4. Divergências entre Fontes (eSocial, CAGED, Empregador Web)

- **Situação:** Um desligamento aparece apenas no Empregador Web, mas não no eSocial nem no CAGED.
- **Como tratar:** Utilize o dado do Empregador Web apenas se não houver registro nas outras fontes. Isso garante que desligamentos não declarados sejam contabilizados.

### 7.5. Atualização de Saldos Históricos

- **Situação:** Após a divulgação dos dados, novas movimentações fora do prazo ou exclusões são informadas.
- **Como tratar:** Recalcule os saldos dos meses afetados e de todos os meses subsequentes, garantindo que o saldo acumulado reflita as alterações.

### 7.6. Mudança de Identificador (CNPJ/CPF)

- **Situação:** Uma empresa muda de CNPJ raiz ou um trabalhador tem CPF corrigido.
- **Como tratar:** Faça o cruzamento considerando o novo identificador, mas mantenha o histórico vinculado ao trabalhador/empresa correta para evitar perda de informação.

### 7.7. Erros de Layout ou Campos Faltantes

- **Situação:** Um arquivo vem com campos em ordem diferente ou faltando campos obrigatórios.
- **Como tratar:** Consulte sempre o layout oficial e valide os arquivos antes de processar. Implemente rotinas de validação e tratamento de exceções.

---

Esses são exemplos de situações comuns na manipulação dos microdados do Novo CAGED. Sempre documente as decisões tomadas e mantenha logs das alterações para garantir rastreabilidade e transparência no processamento dos dados.

**Referências:**
- Leia-me.txt
- Nota Técnica – Tratamentos aplicados nos dados do Novo Caged (nov/2021)
- Documentação oficial do Novo CAGED (pasta docs/) 

## 0.1 Diferença entre as Séries Históricas: CAGED Legado x Novo CAGED

### Diferenças Metodológicas

- **CAGED Legado:** Até dezembro de 2019, os dados eram captados exclusivamente pelo sistema CAGED tradicional, com envio direto das empresas. A metodologia, os layouts e as regras de validação eram diferentes, e não havia integração com o eSocial.
- **Novo CAGED:** A partir de janeiro de 2020, a principal fonte passou a ser o eSocial, complementado por dados do CAGED tradicional (para órgãos públicos e internacionais) e do Empregador Web. O Novo CAGED utiliza metodologia híbrida, com regras de consolidação e imputação para garantir a integridade dos dados.

### Impactos nas Séries

- **Quebra de Série:** O início do Novo CAGED marca uma nova série histórica. Os dados do legado e do novo não são diretamente comparáveis devido às diferenças de captação, cobertura, periodicidade e regras de validação.
- **Cobertura:** O Novo CAGED tende a ter maior cobertura e qualidade, mas pode apresentar diferenças em relação ao legado, especialmente nos primeiros meses de transição.
- **Disponibilização:** As tabelas históricas do CAGED legado continuam disponíveis separadamente. O Novo CAGED possui sua própria série, iniciando em 2020.

### Recomendações para Análise

- **Evite Comparações Diretas:** Não compare diretamente indicadores (saldo, admissões, desligamentos) entre o legado e o novo sem considerar as diferenças metodológicas.
- **Análise de Tendências:** Para análises de tendências de longo prazo, destaque a quebra de série em 2020 e, se necessário, utilize métodos estatísticos para ajustar ou sinalizar a transição.
- **Documentação:** Sempre documente a origem dos dados (legado ou novo) em relatórios e análises.

### Como Tratar Dados do Legado vs. Novo

- **Separação de Bases:** Mantenha bases de dados separadas para o legado e para o novo. Só una as séries se for estritamente necessário e com as devidas ressalvas metodológicas.
- **Atenção aos Layouts:** Os layouts dos arquivos do legado e do novo são diferentes. Consulte sempre a documentação específica de cada período.
- **Ajustes e Imputações:** O Novo CAGED pode conter ajustes retroativos e exclusões, o que não era comum no legado. Considere isso ao analisar saldos e movimentações.
- **Transparência:** Ao apresentar resultados, seja transparente sobre a origem dos dados e as limitações de comparabilidade.

---

Essas orientações ajudam a garantir análises corretas e transparentes ao lidar com as duas séries históricas do emprego formal no Brasil. 

## 0.2 Exemplos de Análise Comparativa: CAGED Legado x Novo CAGED

### Exemplo 1: Visualização de Tendências com Quebra de Série

```python
import pandas as pd
import matplotlib.pyplot as plt

# Supondo que você tenha dois DataFrames: caged_legado e novo_caged
# Cada um com as colunas ['competencia', 'admissoes', 'desligamentos', 'saldo']

# Concatenar as séries, destacando a origem
caged_legado['origem'] = 'Legado'
novo_caged['origem'] = 'Novo'
df = pd.concat([caged_legado, novo_caged], ignore_index=True)

# Plotar o saldo ao longo do tempo
plt.figure(figsize=(12,6))
for origem, grupo in df.groupby('origem'):
    plt.plot(grupo['competencia'], grupo['saldo'], label=origem)
plt.axvline('2020-01', color='red', linestyle='--', label='Início Novo CAGED')
plt.title('Saldo de Emprego Formal: Legado x Novo CAGED')
plt.xlabel('Competência')
plt.ylabel('Saldo')
plt.legend()
plt.show()
```

### Exemplo 2: Cálculo de Variação Percentual e Destaque da Transição

```python
# Calcular a variação percentual do saldo mês a mês
caged_legado['var_perc'] = caged_legado['saldo'].pct_change() * 100
novo_caged['var_perc'] = novo_caged['saldo'].pct_change() * 100

# Exibir as variações próximas à transição
print(caged_legado.tail(3))
print(novo_caged.head(3))
```

### Exemplo 3: Relatório com Observação de Quebra de Série

> "Observa-se que a partir de janeiro de 2020 há uma quebra de série devido à adoção do Novo CAGED, com mudanças metodológicas e de cobertura. Recomenda-se cautela na comparação direta dos indicadores antes e depois dessa data."

### Dicas para Análise Comparativa

- Sempre destaque visualmente a transição entre as séries (ex: linha vertical, cor diferente, anotação).
- Compare tendências e não valores absolutos entre as séries.
- Utilize médias móveis para suavizar variações e facilitar a visualização de tendências.
- Documente claramente a origem dos dados em gráficos e tabelas.

---

Esses exemplos ajudam a realizar análises comparativas responsáveis e transparentes entre o CAGED Legado e o Novo CAGED. 

## 8. Indicadores e Análises Avançadas com Dados do CAGED

### 8.1 Indicadores Relevantes

1. **Admissões e Desligamentos Absolutos**
   - Total de admissões e desligamentos por mês, setor, UF, município, faixa etária, sexo, etc.

2. **Taxa de Rotatividade (Turnover)**
   - Mede a dinâmica do mercado de trabalho:
     ```
     Turnover = (Admissões + Desligamentos) / Estoque Médio de Empregados
     ```

3. **Estoque de Empregados**
   - Número de vínculos ativos em determinado mês (pode ser estimado acumulando saldos).

4. **Salário Médio de Admissão e Desligamento**
   - Média dos salários dos admitidos e desligados em cada período.

5. **Tempo Médio de Permanência**
   - Média de tempo que os trabalhadores permanecem no emprego antes do desligamento.

6. **Movimentação por Setor Econômico**
   - Análise de saldo, admissões e desligamentos por CNAE (setor de atividade).

7. **Movimentação por Faixa Etária, Sexo, Escolaridade**
   - Indicadores de inclusão, diversidade e perfil da força de trabalho.

8. **Admissões e Desligamentos por Tipo de Contrato**
   - CLT, temporário, aprendiz, etc.

9. **Saldo Acumulado no Ano**
   - Soma dos saldos mensais para análise de tendência anual.

10. **Indicadores Regionais**
    - Comparação entre estados, regiões e municípios.

### 8.2 Análises Avançadas

1. **Sazonalidade**
   - Identificação de padrões sazonais (ex: aumento de admissões em determinados meses/setores).

2. **Análise de Cohort**
   - Acompanhamento de grupos de trabalhadores admitidos em determinado período para estudar sua trajetória.

3. **Sobrevivência no Emprego (Análise de Kaplan-Meier)**
   - Probabilidade de permanência no emprego ao longo do tempo.

4. **Modelagem de Fluxos de Trabalho**
   - Análise de transições entre setores, regiões ou tipos de contrato.

5. **Impacto de Políticas Públicas**
   - Avaliação de efeitos de mudanças legislativas, incentivos fiscais, programas de emprego, etc.

6. **Análise de Disparidades**
   - Estudo de desigualdades de gênero, raça, escolaridade, região, etc.

7. **Machine Learning para Previsão**
   - Modelos para prever admissões, desligamentos ou saldo futuro com base em variáveis históricas e econômicas.

8. **Clusterização de Estabelecimentos ou Trabalhadores**
   - Identificação de perfis típicos de empresas ou trabalhadores com base em características e movimentações.

9. **Análise de Impacto de Eventos Externos**
   - Efeitos de crises econômicas, pandemias, desastres naturais, etc., sobre o emprego formal.

10. **Comparação Internacional**
    - Benchmarking com indicadores de emprego de outros países, ajustando metodologias.

---

Esses indicadores e análises permitem extrair o máximo valor dos microdados do CAGED, apoiando estudos, políticas públicas e decisões estratégicas sobre o mercado de trabalho brasileiro. 