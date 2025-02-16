# **PoD Cartões - Data Lake Project**

## **Introdução**
A PoD Cartões é uma empresa de cartões de crédito que busca otimizar o uso de seus dados, porém enfrenta desafios devido à fragmentação das informações em múltiplos sistemas lentos e a uma infraestrutura que não suporta Big Data. Essa limitação dificulta o consumo de dados organizados e de alta qualidade, prejudicando o desenvolvimento de modelos preditivos. Para solucionar esse problema, a empresa pretende implementar um Data Lake escalável e acessível, garantindo a unificação, governança e segurança dos dados. Além disso, será desenvolvido um Book de Variáveis para apoiar a criação de modelos analíticos mais eficazes.


A solução utiliza serviços da AWS para ingestão, processamento e organização de dados em zonas específicas (Raw, Trusted e Curated), além de orquestração de pipelines com o **Apache Airflow**.

## **Dados**
![dados relacionamento](imgs/dados.jpg)


## **Arquitetura**
A arquitetura do projeto está ilustrada abaixo:

![Arquitetura](imgs/Arquitetura.png)

## **Data Lake Zonas**

| **Zona**      | **Descrição**                                                                                                                                                                                                                                                                          |
|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Ingestion** | Dados brutos coletados diretamente do sistema transacional.                                                                                                                                                          |
| **Raw**       | Dados pré-processados que passaram por uma limpeza e validação iniciais, particionados por data e armazenados em formato Parquet. Uma tabela de controle é criada para registrar a data de processamento..                           |
| **Curated**   | Dados refinados, deduplicados e integrados, com enriquecimento por meio da criação de variáveis customizadas alinhadas aos requisitos do negócio. Essa camada inclui uma etapa pré-book (stage) para desenvolvimento de variáveis, seguida da agregação final conforme os períodos de análise e armazenamento na camada de Book para análises avançadas e modelagem preditiva. |
                                                                                                                
                                                                                                                
                                                                                                                
## **Serviços Utilizados**

| **Serviço**         | **Descrição**                                                                                                                                                      |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Apache Airflow**  | Orquestração de pipelines de ingestão e transformação de dados, permitindo o agendamento, monitoramento e automação dos fluxos de trabalho.                        |
| **EMR**             | Plataforma gerenciada de Big Data que facilita a execução de processos ETL e análises distribuídas, utilizando frameworks como Spark e Hadoop.                      |
| **Glue**            | Serviço para catalogação que automatiza a descoberta dos dados armazenados no S3 para consulta e análise.                             |
| **Athena**          | Serviço de consulta interativa que permite executar queries SQL diretamente sobre os dados armazenados no S3, facilitando análises ad hoc e geração de relatórios. |
| **S3**              | Armazenamento escalável e durável que hospeda as zonas Raw, Trusted e Curated do Data Lake.                                                                         |
| **IAM**             | Gerenciamento de identidades e acessos que assegura controles de segurança e permissões adequadas aos recursos AWS.                                                   |
| **CloudWatch**      | Serviço de monitoramento que coleta métricas e logs da infraestrutura AWS, contribuindo para o controle de custos e desempenho.                                      |
| **Docker**          | Plataforma de contêinerização que facilita a criação, implantação e execução de aplicações em ambientes isolados e consistentes, otimizando o desenvolvimento e a integração. |

## **Bibliotecas**

- pandas
- pyspark
- datetime
- time
- boto3
- configparser
- os

## **DAGS**

### **Ingestão**
- Responsável pela extração de dados do SGBD e armazenamento na primeira camada do Data Lake (Ingestion).
- A DAG é programada para executar diariamente às 05:00h, garantindo que os dados mais recentes sejam transferidos para o Data Lake.
  
![dag ingestão](imgs/dag_ingestion.jpg)
  

### **Processamento de dados por tabela**
- Esta DAG inicia um cluster EMR que executa um step job para realizar o processamento dos dados por assunto (tabelas individuais).
- Após o processamento, o cluster é finalizado automaticamente para evitar custos adicionais.
- Programada para executar diariamente às 08:00h, mantendo os dados processados atualizados.
  
![dag processamento](imgs/dag_processamento.jpg)

### **Processamento de Book**
- Focada na criação e atualização das tabelas stage e book de variáveis, essenciais para análises e modelos preditivos.
- A DAG sobe um cluster EMR, executa o step job e encerra o cluster ao final do processamento.
- Programada para rodar mensalmente, todo dia 01, às 22:00h, garantindo que os dados do book estejam preparados para análises estratégicas.
  
![dag book](imgs/dag_book.jpg)

## **BOOK DE VARIÁVEIS**

### **STAGE**
Na etapa 'Stage', foram criadas as seguintes variáveis para explicar o comportamento de uso do cartão de crédito pelos clientes:

- **Classificação de Dias de Atraso**  
  Classificação que indica se o cliente:  
  - Pagou em dia,  
  - Pagou com atraso,  
  - Não realizou o pagamento, ou  
  - Pagou adiantado.

- **Número de Dias em Atraso**  
  - Em caso de "pagamento atrasado", indica quantos dias o cliente atrasou o pagamento.  
  - Em caso de "não pagamento", indica há quantos dias o pagamento está atrasado.

- **Classificação do Valor Pago em Relação à Fatura**  
  Determina como o cliente pagou a fatura:  
  - Pagamento total,  
  - Pagamento mínimo,  
  - Pagamento abaixo do mínimo,  
  - Pagamento acima do mínimo, mas abaixo do total, ou  
  - Não realizou pagamento.

- **Porcentagem da Fatura Paga**  
  Percentual pago em relação ao valor total da fatura, utilizado em casos de pagamento parcial.

- **Quantidade de Transações**  
  Número de transações realizadas pelo cliente, considerando os períodos de análise definidos após a agregação em janelas de tempo.

---

### **BOOK**
Na criação do 'Book', os valores numéricos foram agregados por categoria e janelas de tempo, com a data de referência definida como **'2024-02-01'**. A análise utiliza uma visão mensal para os períodos: **U1M, U3M, U6M, U9M e U12M** (últimos 1, 3, 6, 9 e 12 meses).
Foram criadas 665 variáveis.

# 📊 Visões Criadas para o Book

Este repositório contém a lógica para criação de variáveis e métricas relacionadas ao comportamento de pagamento dos clientes. O código processa dados de faturas e pagamentos, gerando insights valiosos para análise de risco e comportamento financeiro.

---

## 🔹 1. Classificação por Prazo de Pagamento (`fbc_classificacao_dias_pagamento`)  
Define a situação do pagamento com base na data de vencimento:  
- **`SEM_PAGAMENTO`** → Nenhum pagamento registrado  
- **`PAGAMENTO_ATRASADO`** → Pago após o vencimento  
- **`PAGAMENTO_NO_PRAZO`** → Pago exatamente no vencimento  
- **`PAGAMENTO_ANTECIPADO`** → Pago antes do vencimento  

## 🔹 2. Classificação por Valor Pago (`fbc_classificacao_vlr_pagamento`)  
Agrupa os pagamentos conforme o valor pago em relação ao total da fatura:  
- **`PAGAMENTO_INSUFICIENTE`** → Pago menos que o mínimo  
- **`PAGAMENTO_MINIMO`** → Pago exatamente o mínimo  
- **`PAGAMENTO_TOTAL`** → Pago o valor total da fatura  
- **`PAGAMENTO_PARCIAL`** → Pago mais que o mínimo, mas menos que o total  

## 🔹 3. Indicadores Financeiros Calculados (`fvls`)  
Cada métrica de pagamento é analisada com base nas seguintes variáveis:  
- 📌 **`fvl_valor_fatura`** → Valor total da fatura  
- 📌 **`fvl_valor_pagamento_minimo`** → Valor mínimo exigido  
- 📌 **`fvl_valor_pagamento`** → Valor efetivamente pago  
- 📌 **`fvl_numero_dias_atraso`** → Dias de atraso  
- 📌 **`fvl_qtd_transacao`** → Número de transações  
- 📌 **`fvl_pct_fatura_pgto`** → Percentual da fatura paga  

## 🔹 4. Janelas Temporais (`janelas`)  
As métricas são analisadas considerando diferentes períodos históricos:  
- 🕒 **Último mês (`flg_u1m`)**  
- 🕒 **Últimos 3 meses (`flg_u3m`)**  
- 🕒 **Últimos 6 meses (`flg_u6m`)**  
- 🕒 **Últimos 9 meses (`flg_u9m`)**  
- 🕒 **Últimos 12 meses (`flg_u12m`)**  

## 🔹 5. Métricas Agregadas (`aggs`)  
Para cada variável financeira e janela temporal, são aplicadas funções estatísticas:  
- **`SUM`** → Soma dos valores no período  
- **`AVG`** → Média dos valores no período  
- **`MAX`** → Valor máximo no período  
- **`MIN`** → Valor mínimo no período  

## 🔹 6. Regras de Exclusão de Métricas  
Para manter a coerência dos cálculos, algumas combinações não são permitidas:  
❌ `fvl_numero_dias_atraso` **não faz sentido somar dias de atraso das faturas durante os meses** 
❌ `fvl_qtd_transacao` **só faz sentido ser somado, ja que é 1 transação por mês**
❌ `fvl_pct_fatura_pgto` **não faz sentido somar o percentual de fatura paga durante os meses**   
❌ `flg_u1m` **só permite soma, ja que analisando 1 mês SUM,AVG,MAX, e MIN são os mesmos**  
❌ `SEM_PAGAMENTO` e `PAGAMENTO_TOTAL` **não terão métricas sobre percentual pago pois gerariam uma coluna constante**  
