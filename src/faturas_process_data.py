# Importando Libs
from pyspark.sql import SparkSession
from datetime import datetime

# Cria a sessao do Spark
spark = SparkSession.builder.getOrCreate()

# Variavel para controle
dt_proc = datetime.now().strftime("%Y%m%d%H%M%S")

# Funcao para registrar logs com timestamp
def log():
  return datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' >>>'

# Lendo faturas
df_fatura = spark.read.csv('s3://maicon-donza-lake-586794485137/00_ingestion/tb_faturas.csv', header=True)

# Criando View
df_fatura.createOrReplaceTempView("df_fatura")

# Exibir log com quantidade de registros
qtd_registros = df_fatura.count()
print(log(), 'Quantidade de registros: ', qtd_registros)

# Padronizando Colunas e dados e tipificando
df_fatura_formated = spark.sql(f"""
    select
        cast(substring(id_fatura, 3, 2) as bigint) as id_fatura,
        cast(id_cliente as bigint) as id_cliente,
        "{dt_proc}" as dt_proc,
        substring(replace(data_emissao,"-", ""),1, 6) as ref,
        to_date(data_emissao, 'yyyy-mm-dd') as data_emissao,
        to_date(data_vencimento, 'yyyy-mm-dd') as data_vencimento,
        cast(valor_fatura as decimal(15, 2)) as valor_fatura,
        cast(valor_pagamento_minimo as decimal(15, 2)) as valor_pagamento_minimo
    from df_fatura
""")
df_fatura_formated.createOrReplaceTempView('df_fatura_formated')

# Salvando na camada raw particionado por ref
df_fatura_formated.write.mode('append').partitionBy("ref").parquet('s3://maicon-donza-lake-586794485137/01_raw/faturas')

# Criando tabela de controle
df_controle_fatura =spark.sql(f"""
  select
    'tb_001_fatura' as tb_name,
    '{dt_proc}' as dt_proc,
    '{qtd_registros}' as qtd_registros
""")
df_controle_fatura.createOrReplaceTempView('df_controle_fatura')

# Salvando tabela de contole
df_controle_fatura.write.mode('append').csv('s3://maicon-donza-lake-586794485137/03_controle/controle_faturas')