# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREATE FLAT PARAMETER

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
                   select distinct(Branch_ID) as Branch_ID, BranchName
from parquet.`abfss://silver@datalakecar.dfs.core.windows.net/Carsales`
''')

# COMMAND ----------

display(df_src)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_ model sink - Initial and incremental loading(Bring only schema)

# COMMAND ----------

if spark.catalog.tableExists('cars_catelog.gold.dim_branch'):

    df_sink1 = spark.sql('''
        SELECT dim_branch_key, Branch_ID, BranchName
        from cars_catelog.gold.dim_branch
        ''')

else:

    df_sink1 = spark.sql("""
        SELECT 1 as dim_branch_key, Branch_ID, BranchName
        from parquet.`abfss://silver@caranshdatlake.dfs.core.windows.net/carsales`
        where 1=0
    """)

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

df_filter = df_src.join(df_sink1, df_src['Branch_ID'] == df_sink1['Branch_ID'], "left").select(df_src['Branch_ID'], df_src['BranchName'], df_sink1['dim_branch_key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter old data

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_branch_key').isNotNull())

# COMMAND ----------

display(df_filter_old)

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new data

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_branch_key').isNull())

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
    max_value_df = spark.sql("select max(dim_branch_key) from cars_catelog.gold.dim_branch")
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create sarrogate key column and add the max sarrogate key

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_branch_key',max_value+monotonically_increasing_id())

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
if spark.catalog.tableExists('cars_catelog.gold.dim_branch'):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@datalakecar.dfs.core.windows.net/dim_branch")

    delta_tbl.alias("trg").merge(df_final.alias("src"), "trg.dim_branch_key == src.dim_branch_key")\
        .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
                .execute()

  

#Initial run
else:
    df_final.write.format("delta")\
        .mode('overwrite')\
            .option("path","abfss://gold@datalakecar.dfs.core.windows.net/dim_branch")\
                .saveAsTable("cars_catelog.gold.dim_branch")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catelog.gold.dim_branch