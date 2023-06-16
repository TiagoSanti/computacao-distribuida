from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql import Window
from google.cloud import storage
from google.cloud import bigquery

base_path = 'gs://brazil-in-out-2/datasets/'
spark = SparkSession.builder.appName('brazil-in-out-2').getOrCreate()
client = storage.Client()
bq_client = bigquery.Client()
bucket = client.get_bucket('brazil-in-out-2')

files = ['CO_NCM.csv', 'CO_PAIS.csv', 'CO_UNID.csv', 'CO_VIA.csv', 'CO_URF.csv', 'NCM_UNIDADE.csv', 'EXP_COMPLETA.csv', 'IMP_COMPLETA.csv']
dfs = {}

print()
for file in files:
    print(f'Carregando arquivo {file}...', end=' ')
    df = spark.read.format('csv').option('header', 'true').option('delimiter', ';').load(base_path + file)
    print(f'{df.count()} linhas e {len(df.columns)} colunas')

    for col in df.columns:
        df = df.withColumn(col, F.regexp_replace(F.col(col), '\"', ''))

    if file in ['EXP_COMPLETA.csv', 'IMP_COMPLETA.csv']:
        df = df.withColumn('VL_FOB', F.regexp_replace(F.col('VL_FOB'), '\\.', '').cast(DoubleType()))
        
    dfs[file] = df
print()

df_pais = dfs['CO_PAIS.csv']
df_produtos = dfs['CO_NCM.csv']

def save_bigquery(df, table_name):
    table_id = 'computacao-distribuida-2.analysis.' + table_name

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        field_delimiter=';',
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    print(f'Loading {table_name} to BigQuery...')
    uri = f'gs://brazil-in-out-2/analysis-results/{table_name}.csv'
    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)

    load_job.result()

    print(f"{table_name} loaded to BigQuery!\n")

def save_results(df, file_name):
    print('Saving the analysis...')
    df.coalesce(1).write.option('header', 'true').option('delimiter', ';').mode('overwrite').csv(f'gs://brazil-in-out-2/analysis-results/{file_name}.csv')

    blobs = bucket.list_blobs(prefix=f'analysis-results/{file_name}.csv/')

    for blob in blobs:
        if '.' in blob.name.split('/')[-1]:
            print(f'Renaming {blob.name} ...')
            bucket.rename_blob(blob, f'analysis-results/{file_name}.csv')
        else:
            print(f'Removing {blob.name} ...')
            bucket.delete_blobs([blob.name])
    print()

    save_bigquery(df, file_name)

print('Análise 1: O valor FOB total de exportações ao longo do tempo')
df_exports_year = dfs['EXP_COMPLETA.csv'].groupBy('CO_ANO').agg(F.sum('VL_FOB').alias('Total Exports')).orderBy('CO_ANO')
df_exports_year.show(5)
save_results(df_exports_year, 'totals-exp')

print('Análise 2: O valor FOB total de importações ao longo do tempo')
df_imports_year = dfs['IMP_COMPLETA.csv'].groupBy('CO_ANO').agg(F.sum('VL_FOB').alias('Total Imports')).orderBy('CO_ANO')
df_imports_year.show(5)
save_results(df_imports_year, 'totals-imp')

print('Análise 3: O país com maior valor FOB total de exportação ao longo do tempo')
windowSpec = Window.partitionBy('CO_ANO').orderBy(F.desc('Total Exports'))
df_country_exports = dfs['EXP_COMPLETA.csv'].groupBy('CO_ANO','CO_PAIS').agg(F.sum('VL_FOB').alias('Total Exports')).withColumn('rank', F.rank().over(windowSpec)).orderBy('CO_ANO')
df_country_exports = df_country_exports.join(df_pais, df_country_exports.CO_PAIS == df_pais.CO_PAIS, how='inner').select(df_country_exports.CO_ANO, df_pais.NO_PAIS, df_country_exports['Total Exports']).orderBy('CO_ANO')
df_country_exports.show(5)
save_results(df_country_exports, 'countries-exp')

print('Análise 4: O país com maior valor FOB total de importação ao longo do tempo')
windowSpec = Window.partitionBy('CO_ANO').orderBy(F.desc('Total Imports'))
df_country_imports = dfs['IMP_COMPLETA.csv'].groupBy('CO_ANO','CO_PAIS').agg(F.sum('VL_FOB').alias('Total Imports')).withColumn('rank', F.rank().over(windowSpec)).orderBy('CO_ANO')
df_country_imports = df_country_imports.join(df_pais, df_country_imports.CO_PAIS == df_pais.CO_PAIS, how='inner').select(df_country_imports.CO_ANO, df_pais.NO_PAIS, df_country_imports['Total Imports']).orderBy('CO_ANO')
df_country_imports.show(5)
save_results(df_country_imports, 'countries-imp')

print('Análise 5: O produto com maior valor FOB total de exportação ao longo do tempo')
windowSpec = Window.partitionBy('CO_ANO').orderBy(F.desc('Total Exports'))
df_product_exports = dfs['EXP_COMPLETA.csv'].groupBy('CO_ANO','CO_NCM').agg(F.sum('VL_FOB').alias('Total Exports')).withColumn('rank', F.rank().over(windowSpec)).orderBy('CO_ANO')
df_product_exports = df_product_exports.join(df_produtos, df_product_exports.CO_NCM == df_produtos.CO_NCM, how='inner').select(df_product_exports.CO_ANO, df_produtos.NO_NCM_POR, df_product_exports['Total Exports']).orderBy('CO_ANO')
df_product_exports.show(5)
save_results(df_product_exports, 'products-exp')

print('Análise 6: O produto com maior valor FOB total de importação ao longo do tempo')
windowSpec = Window.partitionBy('CO_ANO').orderBy(F.desc('Total Imports'))
df_product_imports = dfs['IMP_COMPLETA.csv'].groupBy('CO_ANO','CO_NCM').agg(F.sum('VL_FOB').alias('Total Imports')).withColumn('rank', F.rank().over(windowSpec)).orderBy('CO_ANO')
df_product_imports = df_product_imports.join(df_produtos, df_product_imports.CO_NCM == df_produtos.CO_NCM, how='inner').select(df_product_imports.CO_ANO, df_produtos.NO_NCM_POR, df_product_imports['Total Imports']).orderBy('CO_ANO')
df_product_imports.show(5)
save_results(df_product_imports, 'products-imp')

print('Análise 7: A região com maior valor FOB total de exportação ao longo do tempo')
windowSpec = Window.partitionBy('CO_ANO').orderBy(F.desc('Total Exports'))
df_region_exports = dfs['EXP_COMPLETA.csv'].groupBy('CO_ANO','SG_UF_NCM').agg(F.sum('VL_FOB').alias('Total Exports')).withColumn('rank', F.rank().over(windowSpec)).orderBy('CO_ANO')
df_region_exports = df_region_exports.select('CO_ANO', 'SG_UF_NCM', 'Total Exports').orderBy('CO_ANO')
df_region_exports.show(5)
save_results(df_region_exports, 'regions-exp')

print('Análise 8: As região com maior valor FOB total de importação ao longo do tempo')
windowSpec = Window.partitionBy('CO_ANO').orderBy(F.desc('Total Imports'))
df_region_imports = dfs['IMP_COMPLETA.csv'].groupBy('CO_ANO','SG_UF_NCM').agg(F.sum('VL_FOB').alias('Total Imports')).withColumn('rank', F.rank().over(windowSpec))
df_region_imports = df_region_imports.select('CO_ANO', 'SG_UF_NCM', 'Total Imports').orderBy('CO_ANO')
df_region_imports.show(5)
save_results(df_region_imports, 'regions-imp')

spark.stop()
