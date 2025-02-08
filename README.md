# **PoD Cartões - Data Lake Project**

## **Introdução**
A PoD Cartões é uma empresa de cartões de crédito que busca otimizar o uso de seus dados, porém enfrenta desafios devido à fragmentação das informações em múltiplos sistemas lentos e a uma infraestrutura que não suporta Big Data. Essa limitação dificulta o consumo de dados organizados e de alta qualidade, prejudicando o desenvolvimento de modelos preditivos. Para solucionar esse problema, a empresa pretende implementar um Data Lake escalável e acessível, garantindo a unificação, governança e segurança dos dados. Além disso, será desenvolvido um Book de Variáveis para apoiar a criação de modelos analíticos mais eficazes.


A solução utiliza serviços da AWS para ingestão, processamento e organização de dados em zonas específicas (Raw, Trusted e Curated), além de orquestração de pipelines com o **Apache Airflow**.

## **Dados**


## **Arquitetura**
A arquitetura do projeto está ilustrada abaixo:

![Architecture](docs/Arquitetura.jpg)

### **Data Lake Zonas**

| **Zona**      | **Descrição**                                                                                                                                                                                                                                                                          |
|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Ingestion** | Dados brutos coletados diretamente do sistema transacional.                                                                                                                                                          |
| **Raw**       | Dados pré-processados que passaram por uma limpeza e validação iniciais, particionados por data e armazenados em formato Parquet. Uma tabela de controle é criada para registrar a data de processamento..                           |
| **Curated**   | Dados refinados, deduplicados e integrados, com enriquecimento por meio da criação de variáveis customizadas alinhadas aos requisitos do negócio. Essa camada inclui uma etapa pré-book (stage) para desenvolvimento de variáveis, seguida da agregação final conforme os períodos de análise e armazenamento na camada de Book para análises avançadas e modelagem preditiva. |
                                                                                                                 |



### **Serviços Utilizados**

| **Serviço**         | **Descrição**                                                                                                                                                      |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Apache Airflow**  | Orquestração de pipelines de ingestão e transformação de dados, permitindo o agendamento, monitoramento e automação dos fluxos de trabalho.                        |
| **EMR**             | Plataforma gerenciada de Big Data que facilita a execução de processos ETL e análises distribuídas, utilizando frameworks como Spark e Hadoop.                      |
| **PySpark**         | Ferramenta para processamento e transformação de grandes volumes de dados, empregada na limpeza, agregação e enriquecimento dos dados.                               |
| **Glue**            | Serviço para catalogação que automatiza a descoberta dos dados armazenados no S3 para consulta e análise.                             |
| **Athena**          | Serviço de consulta interativa que permite executar queries SQL diretamente sobre os dados armazenados no S3, facilitando análises ad hoc e geração de relatórios. |
| **S3**              | Armazenamento escalável e durável que hospeda as zonas Raw, Trusted e Curated do Data Lake.                                                                         |
| **IAM**             | Gerenciamento de identidades e acessos que assegura controles de segurança e permissões adequadas aos recursos AWS.                                                   |
| **CloudWatch**      | Serviço de monitoramento que coleta métricas e logs da infraestrutura AWS, contribuindo para o controle de custos e desempenho.                                      |
| **Docker**          | Plataforma de contêinerização que facilita a criação, implantação e execução de aplicações em ambientes isolados e consistentes, otimizando o desenvolvimento e a integração. |


## **DAGS**

### **Ingestão**
- 

### **Dados de Faturas**
- **Raw Zone**: Dados brutos das faturas de clientes.
- **Curated Zone**: Dados transformados para análises de pagamentos, atrasos e consumo.

### **Book de Variáveis**
- **Trusted Zone**: Variáveis que explicam o comportamento de pagamento no prazo, atrasos e inadimplências.
- **Curated Zone**: Dados agregados por cliente e períodos (U1M, U3M, U6M, U12M).
