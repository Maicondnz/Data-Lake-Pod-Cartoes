# Importando Libs
from pyspark.sql import SparkSession
from datetime import datetime

# Cria ou obtém uma sessão do Spark
spark = SparkSession.builder.getOrCreate()

# Define um timestamp para o processamento
dt_proc = datetime.now().strftime("%Y%m%d%H%M%S")

# Função para gerar logs com timestamp
def log():
  return datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' >>>'

# Lê o arquivo CSV de pagamentos do Amazon S3
df_pagamento = spark.read.csv('s3://maicon-donza-lake-586794485137/00_ingestion/tb_pagamentos.csv', header=True)

# Cria uma visão temporária para consultas SQL no Spark
df_pagamento.createOrReplaceTempView("df_pagamento")

# Conta a quantidade de registros no dataframe e imprime no log
qtd_registros = df_pagamento.count()
print(log(), 'Quantidade de registros: ', qtd_registros)

# Processa e formata os dados de pagamento usando SQL no Spark
df_pagamento_formated = spark.sql(f"""
select
  replace(id_pagamento, "-", "") as id_pagamento,  -- Remove os hifens do ID do pagamento
  cast(split(id_fatura, '-')[1] as bigint) as id_fatura,  -- Extrai o ID da fatura
  cast(id_cliente as bigint) as id_cliente,  -- Converte o ID do cliente para bigint
  "{dt_proc}" as dt_proc,  -- Adiciona a data de processamento
  substring(replace(data_pagamento, "-", ""), 1, 6) as ref,  -- Gera referência de data no formato AAAAMM
  to_date(data_pagamento, 'yyyy-MM-dd') as data_pagamento,  -- Converte a data de pagamento para o formato adequado
  cast(valor_pagamento as decimal(15, 2)) as valor_pagamento  -- Converte o valor do pagamento para decimal
from df_pagamento
""")

# Cria uma visão temporária com os dados formatados
df_pagamento_formated.createOrReplaceTempView("df_pagamento_formated")

# Escreve os dados formatados no S3 em formato Parquet, particionado por "ref"
df_pagamento_formated.write.mode('overwrite').partitionBy("ref").parquet('s3://maicon-donza-lake-586794485137/01_raw/pagamentos')

# Cria um dataframe de controle com informações do processamento
df_controle_pagamento = spark.sql(f"""
select
    'tb_001_pagamento' as tb_name,  -- Nome da tabela de origem
    '{dt_proc}' as dt_proc,  -- Data do processamento
    '{qtd_registros}' as qtd_registros  -- Quantidade de registros processados
""")

# Cria uma visão temporária para os dados de controle
df_controle_pagamento.createOrReplaceTempView('df_controle_pagamento')

# Escreve os dados de controle no S3 em formato CSV com modo append
df_controle_pagamento.write.mode('append').csv("s3://maicon-donza-lake-586794485137/03_controle/controle_pagamentos")
