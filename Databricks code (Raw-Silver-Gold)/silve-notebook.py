# Databricks notebook source
# MAGIC %md
# MAGIC ## # Data Reading

# COMMAND ----------

df = spark.read.format('parquet')\
    .option('Inferschema',True)\
            .load('abfss://bronze@datalakecar.dfs.core.windows.net/RawData')

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## # Data Transformations

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = df.withColumn('model_category',split(col('Model_ID'),'-')[0])

# COMMAND ----------

df.show()

# COMMAND ----------

df.withColumn('Units_Solds',col('Units_Sold').cast(StringType())).display()

# COMMAND ----------

df = df.withColumn('Revenue_avg',col('Revenue')/col('Units_Sold'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregations

# COMMAND ----------

display(df.groupBy('BranchName','Month').agg(sum('Units_Sold').alias('Total_Sold')).sort('Month','Total_Sold',ascending=[1,0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Writing

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
        .option('path','abfss://silver@datalakecar.dfs.core.windows.net/Carsales')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quering Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@datalakecar.dfs.core.windows.net/Carsales`

# COMMAND ----------

# MAGIC %md
# MAGIC