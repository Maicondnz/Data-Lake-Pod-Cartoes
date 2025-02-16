#importando libs
from pyspark.sql import SparkSession
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Cria a sessao do Spark
spark = SparkSession.builder.getOrCreate()

# Funcao para registrar logs com timestamp
def log():
  return datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' >>>'

# Criando Vars de Execucao
dt_proc = datetime.now().strftime("%Y%m%d%H%M%S")
current_date = datetime.now().strftime("%Y%m%d")

dt_proc_book = datetime.strptime('2024-02-01', '%Y-%m-%d')
dt_ini_proc_book = dt_proc_book - relativedelta(months=12)

# Converte as datas para formato numerico
dt_proc_book = int(dt_proc_book.strftime("%Y%m"))
dt_ini_proc_book = int(dt_ini_proc_book.strftime("%Y%m"))

dt_proc_book, dt_ini_proc_book

# Converte a data de processamento para string formatada
dt_proc_book_str = datetime.strptime('2024-02-01', '%Y-%m-%d').strftime("%Y-%m-%d")

# Leitura Pagamentos
tb_pagamento = spark.read.parquet('s3://maicon-donza-lake-586794485137/001_raw/pagamentos/')
tb_pagamento_00 = tb_pagamento.where(f'ref >= {dt_ini_proc_book} and ref <= {dt_proc_book}')
tb_pagamento_00.createOrReplaceTempView("tb_pagamento_00")

# Deduplicacao pagamentos
tb_pagamento_dedup = spark.sql("""
select
ref || dt_proc as dedup_key,
*
from tb_pagamento_00
""")
tb_pagamento_dedup.createOrReplaceTempView("tb_pagamento_dedup")

# Obtem o ultimo tb_pagamentos
tb_pagamento_dedup_00 = spark.sql("""
select
  id_cliente,
  id_fatura,
  max(dedup_key) as max_dedup_key
from tb_pagamento_dedup
group by 1,2
""")
tb_pagamento_dedup_00.createOrReplaceTempView("tb_pagamento_dedup_00")

# Junta as tabelas de pagamentos para manter os registros mais recentes
tb_pagamento_dedup_01 = spark.sql("""
select b.*
from tb_pagamento_dedup_00 a
left join tb_pagamento_dedup b
on a.id_cliente = b.id_cliente and a.id_fatura = b.id_fatura
""")
tb_pagamento_dedup_01.createOrReplaceTempView("tb_pagamento_dedup_01")

# Leitura Faturas
tb_fatura = spark.read.parquet('s3://maicon-donza-lake-586794485137/01_raw/faturas/')
tb_fatura_00 = tb_fatura.where(f'ref >= {dt_ini_proc_book} and ref <= {dt_proc_book}')
tb_fatura_00.createOrReplaceTempView("tb_fatura_00")

# Deduplicacao Faturas
tb_fatura_dedup = spark.sql("""
select
ref || dt_proc as dedup_key,
*
from tb_fatura_00
""")
tb_fatura_dedup.createOrReplaceTempView("tb_fatura_dedup")

# Obtem a tb_faturas mais recente
tb_fatura_dedup_00 = spark.sql("""
select
  id_cliente,
  id_fatura,
  max(dedup_key) as max_dedup_key
from tb_fatura_dedup
group by 1,2
""")
tb_fatura_dedup_00.createOrReplaceTempView("tb_fatura_dedup_00")

# Junta as tabelas de faturas mantendo registros mais recentes
tb_fatura_dedup_01 = spark.sql("""
select b.*
from tb_fatura_dedup_00 a
left join tb_fatura_dedup b
on a.id_cliente = b.id_cliente and a.id_fatura = b.id_fatura
""")
tb_fatura_dedup_01.createOrReplaceTempView("tb_fatura_dedup_01")

## Stage

# Faz a junção entre faturas e pagamentos
df_join = spark.sql("""
select
  a.id_fatura,
  a.id_cliente,
  a.data_emissao,
  a.data_vencimento,
  a.valor_fatura,
  a.valor_pagamento_minimo,
  b.data_pagamento,
  b.valor_pagamento
from tb_fatura_dedup_01 a
left join tb_pagamento_dedup_01 b
on a.id_fatura = b.id_fatura and a.id_cliente = b.id_cliente
""")
df_join.createOrReplaceTempView("df_join")

# Classifica os pagamentos com base na data
tb_classificacao_dias_pagamento = spark.sql("""
select
  *,
    case
      when data_pagamento is null then 'SEM_PAGAMENTO'
      when data_pagamento > data_vencimento then 'PAGAMENTO_ATRASADO'
      when data_pagamento = data_vencimento then 'PAGAMENTO_NO_PRAZO'
      when data_pagamento < data_vencimento then 'PAGAMENTO_ANTECIPADO'
    end as fbc_classificacao_dias_pagamento
from df_join
""")
tb_classificacao_dias_pagamento.createOrReplaceTempView("tb_classificacao_dias_pagamento")

# Calcula os dias de atraso
tb_dias_atraso = spark.sql(f"""
select
  *,
    case
      when fbc_classificacao_dias_pagamento = 'SEM_PAGAMENTO' then datediff("{dt_proc_book_str}", data_vencimento)
      when fbc_classificacao_dias_pagamento = 'PAGAMENTO_ATRASADO' then datediff(data_pagamento, data_vencimento)
      when fbc_classificacao_dias_pagamento = 'PAGAMENTO_NO_PRAZO' then 0
      when fbc_classificacao_dias_pagamento = 'PAGAMENTO_ANTECIPADO' then -1
    end as fvl_numero_dias_atraso
from tb_classificacao_dias_pagamento
""")
tb_dias_atraso.createOrReplaceTempView("tb_dias_atraso")

# Classifica os pagamentos com base no valor pago
tb_classificacao_vlr_pagamento = spark.sql("""
SELECT
  *,
  CASE
    WHEN valor_pagamento IS NULL THEN 'SEM_PAGAMENTO'
    WHEN valor_pagamento = valor_pagamento_minimo THEN 'PAGAMENTO_MINIMO'
    WHEN valor_pagamento < valor_pagamento_minimo THEN 'PAGAMENTO_INSUFICIENTE'
    WHEN valor_pagamento = valor_fatura THEN 'PAGAMENTO_TOTAL'
    WHEN valor_pagamento > valor_pagamento_minimo AND valor_pagamento < valor_fatura THEN 'PAGAMENTO_PARCIAL'
  END AS fbc_classificacao_vlr_pagamento
FROM tb_dias_atraso
""")
tb_classificacao_vlr_pagamento.createOrReplaceTempView("tb_classificacao_vlr_pagamento")

# Calcula o percentual do pagamento em relacao a fatura
tb_pct_pagamento = spark.sql("""
SELECT
  *,
  CASE
    WHEN fbc_classificacao_vlr_pagamento = 'Sem_Pagamento' THEN 0
    WHEN fbc_classificacao_vlr_pagamento = 'Pagamento_Total' THEN 100
  ELSE valor_pagamento / valor_fatura *100
  END AS fvl_pct_fatura_pgto
FROM tb_classificacao_vlr_pagamento
""")
tb_pct_pagamento.createOrReplaceTempView("tb_pct_pagamento")

# Criacao da tabela stage com as novas variaveis
stage = spark.sql(f"""
select
  id_cliente,
  substring(replace("{dt_proc_book_str}","-",""),1,6) as ref,
  '{dt_proc}' as dt_proc,
  fbc_classificacao_dias_pagamento,
  data_emissao,
  valor_fatura as fvl_valor_fatura,
  valor_pagamento_minimo as fvl_valor_pagamento_minimo,
  valor_pagamento as fvl_valor_pagamento,
  fbc_classificacao_vlr_pagamento,
  CAST(fvl_pct_fatura_pgto as float) as fvl_pct_fatura_pgto,
  fvl_numero_dias_atraso,
  1 as fvl_qtd_transacao
from tb_pct_pagamento
""")
stage.createOrReplaceTempView("stage")

# Salvando Tabela Stage particionada por ref
stage.write.mode('overwrite').partitionBy("ref").parquet("s3://maicon-donza-lake-586794485137/02_curated/stage/")

## BOOK DE VARIAVEIS

# Criando flags de acordo com o periodo a ser que o book sera analisado 
janelas_tempo = spark.sql(f"""
select
  *,
  (case when months_between("{dt_proc_book_str}",data_emissao) <= 1 then 1 else 0 end) flg_u1m,
  (case when months_between("{dt_proc_book_str}",data_emissao) <= 3 then 1 else 0 end) flg_u3m,
  (case when months_between("{dt_proc_book_str}",data_emissao) <= 6 then 1 else 0 end) flg_u6m,
  (case when months_between("{dt_proc_book_str}",data_emissao) <= 9 then 1 else 0 end) flg_u9m,
  (case when months_between("{dt_proc_book_str}",data_emissao) <= 12 then 1 else 0 end) flg_u12m
from stage
""")
janelas_tempo.createOrReplaceTempView("janelas_tempo")

# Criando codigo para agregacoes do book
fbcs_dias = ["SEM_PAGAMENTO","PAGAMENTO_ATRASADO","PAGAMENTO_NO_PRAZO","PAGAMENTO_ANTECIPADO"]
fbcs_vlr = ["PAGAMENTO_INSUFICIENTE","PAGAMENTO_TOTAL","PAGAMENTO_PARCIAL"]
fvls = ["fvl_valor_fatura","fvl_valor_pagamento_minimo","fvl_valor_pagamento","fvl_numero_dias_atraso", "fvl_qtd_transacao", "fvl_pct_fatura_pgto"]
janelas = ["flg_u1m","flg_u3m","flg_u6m","flg_u9m","flg_u12m"]
aggs = ["SUM", "AVG", "MAX", "MIN"]
count = 1

for fbc_dias in fbcs_dias:
  for fvl in fvls:
    for janela in janelas:
      for agg in aggs:
        if (fvl == 'fvl_numero_dias_atraso' and agg == 'SUM') or (fvl == 'fvl_qtd_transacao' and agg != 'SUM') or (fvl == 'fvl_pct_fatura_pgto' and agg == 'SUM') or (janelas == 'flg_u1m' and agg != 'SUM' ) or (fbcs_dias == "SEM_PAGAMENTO" and agg == ("fvl_pct_fatura_pgto" or "fvl_valor_pagamento")):
          continue
        else:
          print(f'{agg}(CASE WHEN fbc_classificacao_dias_pagamento = "{fbc_dias}" AND {janela} = 1 THEN {fvl} END) as VAR_{count},')
          count += 1

for fbc_vlr in fbcs_vlr:
  for fvl in fvls:
    for janela in janelas:
      for agg in aggs:
        if (fvl == 'fvl_numero_dias_atraso' and agg == 'SUM') or (fvl == 'fvl_qtd_transacao' and agg != 'SUM') or (fvl == 'fvl_pct_fatura_pgto' and agg == 'SUM') or (janelas == 'flg_u1m' and agg != 'SUM' ) or (fbcs_vlr == "PAGAMENTO_TOTAL" and agg == ("fvl_pct_fatura_pgto" or "fvl_valor_pagamento")):
          continue
        else:
          print(f'{agg}(CASE WHEN fbc_classificacao_vlr_pagamento = "{fbc_vlr}" AND {janela} = 1 THEN {fvl} END) as VAR_{count},')
          count += 1

# Criando Book
book = spark.sql("""
select
  id_cliente,
  ref,
  dt_proc,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_1,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_2,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_3,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_4,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_5,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_6,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_7,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_8,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_9,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_10,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_11,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_12,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_13,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_14,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_15,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_16,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_17,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_18,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_19,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_20,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_21,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_22,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_23,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_24,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_25,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_26,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_27,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_28,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_29,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_30,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_31,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_32,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_33,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_34,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_35,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_36,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_37,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_38,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_39,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_40,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_41,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_42,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_43,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_44,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_45,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_46,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_47,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_48,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_49,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_50,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_51,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_52,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_53,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_54,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_55,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_56,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_57,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_58,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_59,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_60,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_61,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_62,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_63,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_64,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_65,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_66,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_67,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_68,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_69,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_70,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_71,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_72,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_73,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_74,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_75,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_qtd_transacao END) as VAR_76,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_qtd_transacao END) as VAR_77,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_qtd_transacao END) as VAR_78,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_qtd_transacao END) as VAR_79,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_qtd_transacao END) as VAR_80,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_81,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_82,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_83,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_84,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_85,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_86,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_87,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_88,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_89,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_90,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_91,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_92,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_93,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_94,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "SEM_PAGAMENTO" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_95,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_96,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_97,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_98,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_99,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_100,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_101,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_102,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_103,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_104,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_105,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_106,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_107,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_108,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_109,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_110,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_111,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_112,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_113,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_114,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_115,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_116,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_117,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_118,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_119,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_120,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_121,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_122,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_123,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_124,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_125,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_126,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_127,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_128,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_129,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_130,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_131,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_132,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_133,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_134,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_135,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_136,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_137,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_138,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_139,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_140,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_141,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_142,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_143,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_144,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_145,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_146,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_147,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_148,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_149,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_150,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_151,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_152,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_153,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_154,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_155,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_156,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_157,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_158,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_159,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_160,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_161,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_162,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_163,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_164,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_165,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_166,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_167,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_168,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_169,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_170,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_qtd_transacao END) as VAR_171,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_qtd_transacao END) as VAR_172,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_qtd_transacao END) as VAR_173,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_qtd_transacao END) as VAR_174,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_qtd_transacao END) as VAR_175,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_176,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_177,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_178,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_179,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_180,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_181,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_182,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_183,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_184,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_185,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_186,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_187,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_188,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_189,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ATRASADO" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_190,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_191,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_192,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_193,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_194,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_195,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_196,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_197,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_198,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_199,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_200,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_201,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_202,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_203,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_204,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_205,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_206,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_207,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_208,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_209,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_210,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_211,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_212,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_213,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_214,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_215,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_216,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_217,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_218,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_219,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_220,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_221,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_222,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_223,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_224,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_225,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_226,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_227,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_228,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_229,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_230,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_231,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_232,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_233,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_234,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_235,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_236,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_237,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_238,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_239,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_240,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_241,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_242,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_243,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_244,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_245,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_246,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_247,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_248,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_249,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_250,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_251,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_252,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_253,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_254,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_255,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_256,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_257,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_258,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_259,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_260,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_261,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_262,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_263,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_264,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_265,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_qtd_transacao END) as VAR_266,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_qtd_transacao END) as VAR_267,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_qtd_transacao END) as VAR_268,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_qtd_transacao END) as VAR_269,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_qtd_transacao END) as VAR_270,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_271,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_272,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_273,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_274,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_275,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_276,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_277,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_278,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_279,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_280,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_281,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_282,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_283,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_284,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_NO_PRAZO" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_285,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_286,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_287,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_288,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_289,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_290,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_291,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_292,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_293,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_294,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_295,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_296,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_297,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_298,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_299,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_300,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_301,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_302,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_303,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_304,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_305,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_306,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_307,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_308,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_309,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_310,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_311,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_312,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_313,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_314,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_315,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_316,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_317,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_318,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_319,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_320,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_321,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_322,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_323,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_324,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_325,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_326,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_327,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_328,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_329,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_330,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_331,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_332,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_333,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_334,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_335,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_336,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_337,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_338,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_339,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_340,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_341,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_342,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_343,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_344,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_345,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_346,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_347,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_348,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_349,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_350,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_351,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_352,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_353,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_354,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_355,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_356,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_357,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_358,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_359,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_360,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_qtd_transacao END) as VAR_361,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_qtd_transacao END) as VAR_362,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_qtd_transacao END) as VAR_363,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_qtd_transacao END) as VAR_364,
  SUM(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_qtd_transacao END) as VAR_365,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_366,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_367,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_368,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_369,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_370,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_371,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_372,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_373,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_374,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_375,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_376,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_377,
  AVG(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_378,
  MAX(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_379,
  MIN(CASE WHEN fbc_classificacao_dias_pagamento = "PAGAMENTO_ANTECIPADO" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_380,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_381,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_382,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_383,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_384,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_385,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_386,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_387,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_388,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_389,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_390,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_391,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_392,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_393,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_394,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_395,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_396,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_397,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_398,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_399,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_400,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_401,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_402,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_403,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_404,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_405,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_406,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_407,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_408,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_409,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_410,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_411,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_412,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_413,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_414,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_415,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_416,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_417,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_418,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_419,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_420,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_421,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_422,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_423,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_424,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_425,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_426,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_427,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_428,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_429,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_430,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_431,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_432,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_433,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_434,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_435,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_436,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_437,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_438,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_439,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_440,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_441,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_442,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_443,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_444,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_445,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_446,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_447,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_448,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_449,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_450,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_451,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_452,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_453,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_454,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_455,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_qtd_transacao END) as VAR_456,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_qtd_transacao END) as VAR_457,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_qtd_transacao END) as VAR_458,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_qtd_transacao END) as VAR_459,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_qtd_transacao END) as VAR_460,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_461,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_462,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_463,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_464,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_465,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_466,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_467,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_468,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_469,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_470,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_471,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_472,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_473,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_474,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_INSUFICIENTE" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_475,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_476,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_477,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_478,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_479,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_480,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_481,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_482,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_483,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_484,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_485,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_486,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_487,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_488,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_489,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_490,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_491,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_492,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_493,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_494,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_495,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_496,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_497,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_498,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_499,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_500,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_501,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_502,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_503,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_504,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_505,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_506,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_507,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_508,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_509,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_510,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_511,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_512,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_513,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_514,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_515,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_516,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_517,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_518,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_519,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_520,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_521,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_522,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_523,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_524,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_525,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_526,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_527,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_528,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_529,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_530,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_531,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_532,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_533,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_534,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_535,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_536,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_537,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_538,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_539,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_540,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_541,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_542,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_543,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_544,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_545,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_546,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_547,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_548,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_549,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_550,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_qtd_transacao END) as VAR_551,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_qtd_transacao END) as VAR_552,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_qtd_transacao END) as VAR_553,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_qtd_transacao END) as VAR_554,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_qtd_transacao END) as VAR_555,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_556,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_557,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_558,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_559,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_560,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_561,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_562,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_563,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_564,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_565,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_566,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_567,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_568,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_569,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_TOTAL" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_570,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_571,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_572,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_573,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_valor_fatura END) as VAR_574,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_575,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_576,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_577,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_valor_fatura END) as VAR_578,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_579,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_580,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_581,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_valor_fatura END) as VAR_582,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_583,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_584,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_585,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_valor_fatura END) as VAR_586,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_587,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_588,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_589,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_valor_fatura END) as VAR_590,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_591,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_592,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_593,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_594,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_595,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_596,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_597,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_598,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_599,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_600,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_601,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_602,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_603,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_604,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_605,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_606,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_607,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_608,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_609,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_valor_pagamento_minimo END) as VAR_610,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_611,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_612,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_613,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_valor_pagamento END) as VAR_614,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_615,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_616,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_617,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_valor_pagamento END) as VAR_618,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_619,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_620,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_621,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_valor_pagamento END) as VAR_622,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_623,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_624,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_625,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_valor_pagamento END) as VAR_626,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_627,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_628,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_629,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_valor_pagamento END) as VAR_630,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_631,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_632,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_numero_dias_atraso END) as VAR_633,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_634,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_635,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_numero_dias_atraso END) as VAR_636,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_637,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_638,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_numero_dias_atraso END) as VAR_639,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_640,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_641,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_numero_dias_atraso END) as VAR_642,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_643,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_644,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_numero_dias_atraso END) as VAR_645,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_qtd_transacao END) as VAR_646,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_qtd_transacao END) as VAR_647,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_qtd_transacao END) as VAR_648,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_qtd_transacao END) as VAR_649,
  SUM(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_qtd_transacao END) as VAR_650,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_651,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_652,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u1m = 1 THEN fvl_pct_fatura_pgto END) as VAR_653,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_654,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_655,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u3m = 1 THEN fvl_pct_fatura_pgto END) as VAR_656,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_657,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_658,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u6m = 1 THEN fvl_pct_fatura_pgto END) as VAR_659,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_660,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_661,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u9m = 1 THEN fvl_pct_fatura_pgto END) as VAR_662,
  AVG(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_663,
  MAX(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_664,
  MIN(CASE WHEN fbc_classificacao_vlr_pagamento = "PAGAMENTO_PARCIAL" AND flg_u12m = 1 THEN fvl_pct_fatura_pgto END) as VAR_665
from janelas_tempo
group by 1, 2, 3
""")
book.createOrReplaceTempView("book")

# Salvando book particionado pela ref
book.write.mode("append").partitionBy("ref").parquet("s3://maicon-donza-lake-586794485137/02_curated/book/")