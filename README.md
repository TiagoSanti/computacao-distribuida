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

## Exemplo de Saída da Execução de um Job
```output
23/06/15 03:56:56 INFO org.apache.spark.SparkEnv: Registering MapOutputTracker
23/06/15 03:56:56 INFO org.apache.spark.SparkEnv: Registering BlockManagerMaster
23/06/15 03:56:56 INFO org.apache.spark.SparkEnv: Registering BlockManagerMasterHeartbeat
23/06/15 03:56:56 INFO org.apache.spark.SparkEnv: Registering OutputCommitCoordinator
23/06/15 03:56:56 INFO org.sparkproject.jetty.util.log: Logging initialized @4049ms to org.sparkproject.jetty.util.log.Slf4jLog
23/06/15 03:56:56 INFO org.sparkproject.jetty.server.Server: jetty-9.4.40.v20210413; built: 2021-04-13T20:42:42.668Z; git: b881a572662e1943a14ae12e7e1207989f218b74; jvm 1.8.0_372-b07
23/06/15 03:56:56 INFO org.sparkproject.jetty.server.Server: Started @4175ms
23/06/15 03:56:56 INFO org.sparkproject.jetty.server.AbstractConnector: Started ServerConnector@164c450b{HTTP/1.1, (http/1.1)}{0.0.0.0:45317}
23/06/15 03:56:57 INFO org.apache.hadoop.yarn.client.RMProxy: Connecting to ResourceManager at cluster-cbd7-m/10.128.0.2:8032
23/06/15 03:56:57 INFO org.apache.hadoop.yarn.client.AHSProxy: Connecting to Application History server at cluster-cbd7-m/10.128.0.2:10200
23/06/15 03:56:58 INFO org.apache.hadoop.conf.Configuration: resource-types.xml not found
23/06/15 03:56:58 INFO org.apache.hadoop.yarn.util.resource.ResourceUtils: Unable to find 'resource-types.xml'.
23/06/15 03:56:59 INFO org.apache.hadoop.yarn.client.api.impl.YarnClientImpl: Submitted application application_1686765813871_0016
23/06/15 03:57:00 INFO org.apache.hadoop.yarn.client.RMProxy: Connecting to ResourceManager at cluster-cbd7-m/10.128.0.2:8030
23/06/15 03:57:03 INFO com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl: Ignoring exception of type GoogleJsonResponseException; verified object already exists with desired state.

Carregando arquivo CO_NCM.csv... 13127 linhas e 14 colunas
Carregando arquivo CO_PAIS.csv... 282 linhas e 6 colunas
Carregando arquivo CO_UNID.csv... 15 linhas e 3 colunas
Carregando arquivo CO_VIA.csv... 17 linhas e 2 colunas
Carregando arquivo CO_URF.csv... 276 linhas e 2 colunas
Carregando arquivo NCM_UNIDADE.csv... 15 linhas e 3 colunas
Carregando arquivo EXP_COMPLETA.csv... 23358760 linhas e 11 colunas
Carregando arquivo IMP_COMPLETA.csv... 34884277 linhas e 11 colunas

Análise 1: O valor FOB total de exportações ao longo do tempo
+------+---------------+
|CO_ANO|  Total Exports|
+------+---------------+
|  1997|5.2947495532E10|
|  1998|5.1076603549E10|
|  1999| 4.794590931E10|
|  2000|5.4993159648E10|
|  2001|5.8032294243E10|
+------+---------------+
only showing top 5 rows

Saving the analysis...
23/06/15 03:58:43 INFO com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem: Successfully repaired 'gs://brazil-in-out-2/analysis-results/totals-exp.csv/' directory.
Removing analysis-results/totals-exp.csv/ ...
Removing analysis-results/totals-exp.csv/_SUCCESS ...
Renaming analysis-results/totals-exp.csv/part-00000-b5d6d48d-db53-4bdb-840a-9c0f096f0c58-c000.csv ...

Loading totals-exp to BigQuery...
totals-exp loaded to BigQuery!

Análise 2: O valor FOB total de importações ao longo do tempo
+------+---------------+
|CO_ANO|  Total Imports|
+------+---------------+
|  1997|6.0537962059E10|
|  1998|5.8672860908E10|
|  1999|5.0259540356E10|
|  2000| 5.697635017E10|
|  2001|5.6569020182E10|
+------+---------------+
only showing top 5 rows

Saving the analysis...
23/06/15 04:00:09 INFO com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem: Successfully repaired 'gs://brazil-in-out-2/analysis-results/totals-imp.csv/' directory.
Removing analysis-results/totals-imp.csv/ ...
Removing analysis-results/totals-imp.csv/_SUCCESS ...
Renaming analysis-results/totals-imp.csv/part-00000-291256ce-f851-47df-b67c-84abc0d518f8-c000.csv ...

Loading totals-imp to BigQuery...
totals-imp loaded to BigQuery!

Análise 3: O país com maior valor FOB total de exportação ao longo do tempo
+------+-------------+-------------+
|CO_ANO|      NO_PAIS|Total Exports|
+------+-------------+-------------+
|  1997|       Uganda|    1705706.0|
|  1997|   Bangladesh|  5.3057888E7|
|  1997|Norfolk, Ilha|       1374.0|
|  1997|África do Sul| 3.31645711E8|
|  1997| Burkina Faso|     629638.0|
+------+-------------+-------------+
only showing top 5 rows

Saving the analysis...
23/06/15 04:01:36 INFO com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem: Successfully repaired 'gs://brazil-in-out-2/analysis-results/countries-exp.csv/' directory.
Removing analysis-results/countries-exp.csv/ ...
Removing analysis-results/countries-exp.csv/_SUCCESS ...
Renaming analysis-results/countries-exp.csv/part-00000-d72165c2-b742-4395-ae1d-37ea52cb27e2-c000.csv ...

Loading countries-exp to BigQuery...
countries-exp loaded to BigQuery!

Análise 4: O país com maior valor FOB total de importação ao longo do tempo
+------+--------------------+-------------+
|CO_ANO|             NO_PAIS|Total Imports|
+------+--------------------+-------------+
|  1997|       Nova Zelândia|  6.5988494E7|
|  1997|Cocos (Keeling), ...|     261426.0|
|  1997|        Burkina Faso|    2815468.0|
|  1997|       África do Sul|  3.4955749E8|
|  1997|          Bangladesh|  1.8162106E7|
+------+--------------------+-------------+
only showing top 5 rows

Saving the analysis...
23/06/15 04:03:24 INFO com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem: Successfully repaired 'gs://brazil-in-out-2/analysis-results/countries-imp.csv/' directory.
Removing analysis-results/countries-imp.csv/ ...
Removing analysis-results/countries-imp.csv/_SUCCESS ...
Renaming analysis-results/countries-imp.csv/part-00000-a71fcddb-35ab-4fae-ab2c-6523ce2e351e-c000.csv ...

Loading countries-imp to BigQuery...
countries-imp loaded to BigQuery!

Análise 5: O produto com maior valor FOB total de exportação ao longo do tempo
+------+--------------------+-------------+
|CO_ANO|          NO_NCM_POR|Total Exports|
+------+--------------------+-------------+
|  1997|Geradores de corr...|     751644.0|
|  1997|Correias de trans...|     158623.0|
|  1997|Partes de outras ...|  1.7110269E7|
|  1997|Borracha misturad...|     190838.0|
|  1997|Outros artigos e ...|    2937379.0|
+------+--------------------+-------------+
only showing top 5 rows

Saving the analysis...
23/06/15 04:04:40 INFO com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem: Successfully repaired 'gs://brazil-in-out-2/analysis-results/products-exp.csv/' directory.
Removing analysis-results/products-exp.csv/ ...
Removing analysis-results/products-exp.csv/_SUCCESS ...
Renaming analysis-results/products-exp.csv/part-00000-fc74fc4c-81bc-419f-b66b-c8127088c0dd-c000.csv ...

Loading products-exp to BigQuery...
products-exp loaded to BigQuery!

Análise 6: O produto com maior valor FOB total de importação ao longo do tempo
+------+--------------------+-------------+
|CO_ANO|          NO_NCM_POR|Total Imports|
+------+--------------------+-------------+
|  1997|Tintas de outros ...|    2787025.0|
|  1997|Capacetes e artef...|    1482936.0|
|  1997|Sortidos de artig...|    1101713.0|
|  1997|   Outras bijuterias|    2800371.0|
|  1997|Outros politerpen...|    7456121.0|
+------+--------------------+-------------+
only showing top 5 rows

Saving the analysis...
23/06/15 04:06:32 INFO com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem: Successfully repaired 'gs://brazil-in-out-2/analysis-results/products-imp.csv/' directory.
Removing analysis-results/products-imp.csv/ ...
Removing analysis-results/products-imp.csv/_SUCCESS ...
Renaming analysis-results/products-imp.csv/part-00000-9b0cb48f-4f0a-4995-a117-5034a202c875-c000.csv ...

Loading products-imp to BigQuery...
products-imp loaded to BigQuery!

Análise 7: A região com maior valor FOB total de exportação ao longo do tempo
+------+---------+-------------+
|CO_ANO|SG_UF_NCM|Total Exports|
+------+---------+-------------+
|  1997|       AL| 3.40730844E8|
|  1997|       PI|  6.0806519E7|
|  1997|       RS|6.267496953E9|
|  1997|       MA| 7.44456825E8|
|  1997|       GO| 4.75435888E8|
+------+---------+-------------+
only showing top 5 rows

Saving the analysis...
23/06/15 04:07:43 INFO com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem: Successfully repaired 'gs://brazil-in-out-2/analysis-results/regions-exp.csv/' directory.
Removing analysis-results/regions-exp.csv/ ...
Removing analysis-results/regions-exp.csv/_SUCCESS ...
Renaming analysis-results/regions-exp.csv/part-00000-51aa5568-3487-4939-b514-18272740c232-c000.csv ...

Loading regions-exp to BigQuery...
regions-exp loaded to BigQuery!

Análise 8: As região com maior valor FOB total de importação ao longo do tempo
+------+---------+-------------+
|CO_ANO|SG_UF_NCM|Total Imports|
+------+---------+-------------+
|  1997|       SE| 1.26539998E8|
|  1997|       AM|4.248237144E9|
|  1997|       PR|3.359610552E9|
|  1997|       PB| 2.07907171E8|
|  1997|       SC|1.489815204E9|
+------+---------+-------------+
only showing top 5 rows

Saving the analysis...
23/06/15 04:09:30 INFO com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem: Successfully repaired 'gs://brazil-in-out-2/analysis-results/regions-imp.csv/' directory.
Removing analysis-results/regions-imp.csv/ ...
Removing analysis-results/regions-imp.csv/_SUCCESS ...
Renaming analysis-results/regions-imp.csv/part-00000-b0402493-790e-4eab-914f-86202bbe0c1c-c000.csv ...

Loading regions-imp to BigQuery...
regions-imp loaded to BigQuery!

23/06/15 04:09:34 INFO org.sparkproject.jetty.server.AbstractConnector: Stopped Spark@164c450b{HTTP/1.1, (http/1.1)}{0.0.0.0:0}
```

## Apresentação das Análises
As análises resultantes salvas em tabelas do BigQuery são conectadas no Looker Studio e lá representadas por gráficos de barras, geográficos e lineares em suas [páginas](https://lookerstudio.google.com/reporting/ae5a333c-5251-4bb4-9a22-68448a5586a9).
