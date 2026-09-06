# Databricks notebook source
# MAGIC %md
# MAGIC ## CREATE FLAT PARAMETER

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREATING DIMENTION MODEL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetching the Relative columns

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@datalakecar.dfs.core.windows.net/Carsales`

# COMMAND ----------

df_src = spark.sql('''
                   select distinct(Model_ID) as Model_ID, model_category 
from parquet.`abfss://silver@datalakecar.dfs.core.windows.net/Carsales`
''')

# COMMAND ----------

display(df_src)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_ model sink - Initial and incremental loading

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

if spark.catalog.tableExists('cars_catelog.gold.dim_model'):

    df_sink1 = spark.sql('''
        SELECT dim_model_key, Model_ID, model_category
        from cars_catelog.gold.dim_model
    ''')

else:

    df_sink1 = spark.sql('''
        SELECT 1 as dim_model_key, Model_ID, model_category
        from parquet.`abfss://silver@caranshdatlake.dfs.core.windows.net/carsales`
        where 1=0
    ''')

# COMMAND ----------

### schema = StructType([
    # StructField("Model_ID", StringType(), True),
    # StructField("dim_model_key", IntegerType(), True),
    # StructField("model_category", StringType(), True)
# ])
# df_sink1 = spark.createDataFrame([], schema)

# print(type(df_sink1))
# df_sink1.printSchema()

# COMMAND ----------

display(df_sink1)

# COMMAND ----------

print(type(df_src))

# COMMAND ----------

print(type(df_sink1))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering new vs old records - by joins

# COMMAND ----------

df_filter = df_src.join(df_sink1, df_src['Model_ID'] == df_sink1['Model_ID'], "left").select(df_src['Model_ID'], df_src['model_category'], df_sink1['dim_model_key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter old data

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_model_key').isNotNull())

# COMMAND ----------

display(df_filter_old)

# COMMAND ----------

# MAGIC %md
# MAGIC df_filter_new data

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_model_key').isNull())

# COMMAND ----------

display(df_filter_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create sarrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch the max sarrogate key

# COMMAND ----------

if (incremental_flag == '0'):
    max_value = 1
else:
    max_value_df = spark.sql("select max(dim_model_key) from cars_catelog.gold.dim_model")
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create sarrogate key column and add the max sarrogate key

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_model_key',max_value+monotonically_increasing_id())

# COMMAND ----------

display(df_filter_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create final_df = df_filter_old+df_filter_new

# COMMAND ----------

df_final = df_filter_old.union(df_filter_new)

# COMMAND ----------

display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD TYPE 1 - UPSERT

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# Incremental run
if spark.catalog.tableExists('cars_catelog.gold.dim_model'):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@datalakecar.dfs.core.windows.net/dim_model")

    delta_tbl.alias("trg").merge(df_final.alias("src"), "trg.dim_model_key == src.dim_model_key")\
        .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
                .execute()

  

#Initial run
else:
    df_final.write.format("delta")\
        .mode('overwrite')\
            .option("path","abfss://gold@datalakecar.dfs.core.windows.net/dim_model")\
                .saveAsTable("cars_catelog.gold.dim_model")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catelog.gold.dim_model