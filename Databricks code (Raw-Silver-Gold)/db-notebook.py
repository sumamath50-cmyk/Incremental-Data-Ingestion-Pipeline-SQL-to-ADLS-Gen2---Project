# Databricks notebook source
# MAGIC %md
# MAGIC # create catelog

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog cars_catelog;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catelog.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catelog.gold;