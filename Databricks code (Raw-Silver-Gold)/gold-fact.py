# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Fact Table

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading silver data

# COMMAND ----------

df_silver = spark.sql("select * from parquet.`abfss://silver@datalakecar.dfs.core.windows.net/Carsales`")

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading all DFs

# COMMAND ----------

df_dealer = spark.sql("select * from cars_catelog.gold.dim_dealer")
df_date = spark.sql("select * from cars_catelog.gold.dim_date")
df_model = spark.sql("select * from cars_catelog.gold.dim_model")
df_branch = spark.sql("select * from cars_catelog.gold.dim_branch")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bringing all the dim keys to Fact table

# COMMAND ----------

df_fact = df_silver.join(df_dealer, df_silver.Dealer_ID == df_dealer.Dealer_ID, "left") \
                  .join(df_date, df_silver.Date_Id == df_date.Date_ID, "left") \
                  .join(df_model, df_silver.Model_ID == df_model.Model_ID, "left") \
                  .join(df_branch, df_silver.Branch_ID == df_branch.Branch_ID, "left") \
                      .select(df_silver.Revenue, df_silver.Units_Sold, df_silver.Revenue_avg, df_branch.dim_branch_key, df_dealer.dim_dealer_key, df_model.dim_model_key, df_date.dim_date_key)
    

# COMMAND ----------

display(df_fact)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing Fact table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('FactSales'):
    deltatbl = DeltaTable.forPath(spark, 'cars_catelog.gold.Factsales')

    deltatbl.alias('trg').merge(df_fact.alias('src'),'trg.dim_branch_key = src.dim_branch_key and trg.dim_dealer_key = src.dim_dealer_key and trg.dim_model_key = src.dim_model_key and trg.dim_date_key = src.dim_date_key')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()


else:
    df_fact.write.format('delta')\
        .mode('overwrite')\
        .option("path","abfss://gold@datalakecar.dfs.core.windows.net/Factsales")\
        .saveAsTable('cars_catelog.gold.Factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catelog.gold.FactSales