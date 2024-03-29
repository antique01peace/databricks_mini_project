# Databricks notebook source
data = [
    (1, 2, 100),
    (1, 2, 150),
    (2, 1, 110),
    (2, 2, 200)
]

columns = ["Category A", "Category B", "Value"]
df = spark.createDataFrame(data, columns)

# COMMAND ----------

from pyspark.sql.functions import col, sum
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md #####Explore non-cumulative sum

# COMMAND ----------

WindowSpec = Window.partitionBy(["Category A", "Category B"])

# COMMAND ----------

df_csum = df.withColumn("sum", sum("value").over(WindowSpec))
display(df_csum)

# COMMAND ----------

# MAGIC %md #####Explore non-cumulative sum

# COMMAND ----------

#WindowSpec = Window.partitionBy(["Category A", "Category B"]).orderBy("value")
WindowSpec = Window.partitionBy("Category A").orderBy("value")

# COMMAND ----------

df_ncsum = df.withColumn("sum", sum("value").over(WindowSpec))
display(df_ncsum)

# COMMAND ----------


