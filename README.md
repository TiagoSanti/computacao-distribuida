# Análise de Dados de Exportação e Importação do Brasil
Este projeto utiliza PySpark para analisar dados de exportação e importação do Brasil, disponibilizados em sua [página do Kaggle](https://www.kaggle.com/datasets/daniellecd/dados-de-exportao-e-importao-do-brasil).
As análises são realizadas em um ambiente de processamento distribuído no Google Cloud Dataproc, os resultados são salvos no Google Cloud Storage e no BigQuery, e são visualizadas no [Looker Studio](https://lookerstudio.google.com/reporting/ae5a333c-5251-4bb4-9a22-68448a5586a9).
Abaixo está a descrição dos passos realizados no código.

## Pré-requisitos
- Importando as bibliotecas necessárias
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql import Window
from google.cloud import storage
from google.cloud import bigquery
```

## Configuração do Ambiente
- Inicializando uma sessão Spark com o nome 'brazil-in-out-2' usando o seguinte comando:
```python
spark = SparkSession.builder.appName('brazil-in-out-2').getOrCreate()
```

- Inicializando os clientes do Google Cloud Storage e do BigQuery:
```python
client = storage.Client()
bq_client = bigquery.Client()
```

- Obtendo o bucket do Google Cloud Storage onde os dados estão armazenados:
```python
bucket = client.get_bucket('brazil-in-out-2')
```
## Carregamento dos Dados
- Definição da lista de arquivos a serem carregados:
```python
files = ['CO_NCM.csv', 'CO_PAIS.csv', 'CO_UNID.csv', 'CO_VIA.csv', 'CO_URF.csv', 'NCM_UNIDADE.csv', 'EXP_COMPLETA.csv', 'IMP_COMPLETA.csv']
```

- Carregando cada arquivo do Google Cloud Storage, removendo as aspas duplas dos dados e convertendo o valor FOB para o tipo Double (para os arquivos 'EXP_COMPLETA.csv' e 'IMP_COMPLETA.csv'):
```python
for file in files:
    df = spark.read.format('csv').option('header', 'true').option('delimiter', ';').load(base_path + file)
    
    for col in df.columns:
        df = df.withColumn(col, F.regexp_replace(F.col(col), '\"', ''))
        
    if file in ['EXP_COMPLETA.csv', 'IMP_COMPLETA.csv']:
        df = df.withColumn('VL_FOB', F.regexp_replace(F.col('VL_FOB'), '\\.', '').cast(DoubleType()))
        
    dfs[file] = df
```

## Análises
Realizando as análises necessárias. Cada análise envolve o agrupamento dos dados por um determinado atributo (por exemplo, o ano ou o país), o cálculo do valor FOB total para cada grupo e a ordenação dos resultados. Além disso, para algumas análises, o script também calcula o rank de cada grupo com base no valor FOB total.

### Análise 1: O valor FOB total de exportações ao longo do tempo
```python
df_exports_year = dfs['EXP_COMPLETA.csv'].groupBy('CO_ANO').agg(F.sum('VL_FOB').alias('Total Exports')).orderBy('CO_ANO')
df_exports_year.show(5)
save_results(df_exports_year, 'totals-exp')
```

### Análise 2: O valor FOB total de importações ao longo do tempo
```python
df_imports_year = dfs['IMP_COMPLETA.csv'].groupBy('CO_ANO').agg(F.sum('VL_FOB').alias('Total Imports')).orderBy('CO_ANO')
df_imports_year.show(5)
save_results(df_imports_year, 'totals-imp')
```

### Análise 3: O país com maior valor FOB total de exportação ao longo do tempo
```python
windowSpec = Window.partitionBy('CO_ANO').orderBy(F.desc('Total Exports'))
df_country_exports = dfs['EXP_COMPLETA.csv'].groupBy('CO_ANO','CO_PAIS').agg(F.sum('VL_FOB').alias('Total Exports')).withColumn('rank', F.rank().over(windowSpec)).orderBy('CO_ANO')
df_country_exports = df_country_exports.join(df_pais, df_country_exports.CO_PAIS == df_pais.CO_PAIS, how='inner').select(df_country_exports.CO_ANO, df_pais.NO_PAIS, df_country_exports['Total Exports']).orderBy('CO_ANO')
df_country_exports.show(5)
save_results(df_country_exports, 'countries-exp')
```

### Análise 4: O país com maior valor FOB total de importação ao longo do tempo
```python
windowSpec = Window.partitionBy('CO_ANO').orderBy(F.desc('Total Imports'))
df_country_imports = dfs['IMP_COMPLETA.csv'].groupBy('CO_ANO','CO_PAIS').agg(F.sum('VL_FOB').alias('Total Imports')).withColumn('rank', F.rank().over(windowSpec)).orderBy('CO_ANO')
df_country_imports = df_country_imports.join(df_pais, df_country_imports.CO_PAIS == df_pais.CO_PAIS, how='inner').select(df_country_imports.CO_ANO, df_pais.NO_PAIS, df_country_imports['Total Imports']).orderBy('CO_ANO')
df_country_imports.show(5)
save_results(df_country_imports, 'countries-imp')
```

### Análise 5: O produto com maior valor FOB total de exportação ao longo do tempo
```python
windowSpec = Window.partitionBy('CO_ANO').orderBy(F.desc('Total Exports'))
df_product_exports = dfs['EXP_COMPLETA.csv'].groupBy('CO_ANO','CO_NCM').agg(F.sum('VL_FOB').alias('Total Exports')).withColumn('rank', F.rank().over(windowSpec)).orderBy('CO_ANO')
df_product_exports = df_product_exports.join(df_produtos, df_product_exports.CO_NCM == df_produtos.CO_NCM, how='inner').select(df_product_exports.CO_ANO, df_produtos.NO_NCM_POR, df_product_exports['Total Exports']).orderBy('CO_ANO')
df_product_exports.show(5)
save_results(df_product_exports, 'products-exp'
```

### Análise 6: O produto com maior valor FOB total de importação ao longo do tempo
```python
windowSpec = Window.partitionBy('CO_ANO').orderBy(F.desc('Total Imports'))
df_product_imports = dfs['IMP_COMPLETA.csv'].groupBy('CO_ANO','CO_NCM').agg(F.sum('VL_FOB').alias('Total Imports')).withColumn('rank', F.rank().over(windowSpec)).orderBy('CO_ANO')
df_product_imports = df_product_imports.join(df_produtos, df_product_imports.CO_NCM == df_produtos.CO_NCM, how='inner').select(df_product_imports.CO_ANO, df_produtos.NO_NCM_POR, df_product_imports['Total Imports']).orderBy('CO_ANO')
df_product_imports.show(5)
save_results(df_product_imports, 'products-imp')
```

### Análise 7: A região com maior valor FOB total de exportação ao longo do tempo
```python
windowSpec = Window.partitionBy('CO_ANO').orderBy(F.desc('Total Exports'))
df_region_exports = dfs['EXP_COMPLETA.csv'].groupBy('CO_ANO','SG_UF_NCM').agg(F.sum('VL_FOB').alias('Total Exports')).withColumn('rank', F.rank().over(windowSpec)).orderBy('CO_ANO')
df_region_exports = df_region_exports.select('CO_ANO', 'SG_UF_NCM', 'Total Exports').orderBy('CO_ANO')
df_region_exports.show(5)
save_results(df_region_exports, 'regions-exp')
```

### Análise 8: As região com maior valor FOB total de importação ao longo do tempo
``` python
windowSpec = Window.partitionBy('CO_ANO').orderBy(F.desc('Total Imports'))
df_region_imports = dfs['IMP_COMPLETA.csv'].groupBy('CO_ANO','SG_UF_NCM').agg(F.sum('VL_FOB').alias('Total Imports')).withColumn('rank', F.rank().over(windowSpec))
df_region_imports = df_region_imports.select('CO_ANO', 'SG_UF_NCM', 'Total Imports').orderBy('CO_ANO')
df_region_imports.show(5)
save_results(df_region_imports, 'regions-imp')
```

## Salvamento das análises
- Definição das funções save_bigquery e save_results para salvar os resultados das análises no Google Cloud Storage e no BigQuery, respectivamente:
```python
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
```

## Apresentação das Análises
As análises resultantes salvas em tabelas do BigQuery são conectadas no Looker Studio e lá representadas por gráficos de barras, geográficos e lineares em suas [páginas](https://lookerstudio.google.com/reporting/ae5a333c-5251-4bb4-9a22-68448a5586a9).
