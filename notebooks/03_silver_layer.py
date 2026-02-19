# Databricks notebook source
# Configure Azure storage account authentication
storage_account_name = "stinsuranceanalytics"
storage_account_key = os.getenv("AZURE_STORAGE_KEY")  # Replace with actual key

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# COMMAND ----------

# MAGIC %md
# MAGIC Étape 1 — Lire les données Bronze (Delta)

# COMMAND ----------

df = spark.read.format("delta").load(
    "abfss://bronze@stinsuranceanalytics.dfs.core.windows.net/insurance_raw"
)

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC Étape 2 — Conversion Yes/No → Boolean

# COMMAND ----------

from pyspark.sql.functions import when, col

bool_columns = [c for c in df.columns if c.startswith("is_")]

for c in bool_columns:
    df = df.withColumn(
        c,
        when(col(c) == "Yes", True)
        .when(col(c) == "No", False)
        .otherwise(None)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC Étape 3 — Parser max_torque

# COMMAND ----------

from pyspark.sql.functions import regexp_extract

df = df.withColumn(
    "torque_nm",
    regexp_extract(col("max_torque"), r"(\d+)", 1).cast("int")
)

df = df.withColumn(
    "torque_rpm",
    regexp_extract(col("max_torque"), r"@(\d+)", 1).cast("int")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Étape 4 — Parser max_power

# COMMAND ----------

df = df.withColumn(
    "power_bhp",
    regexp_extract(col("max_power"), r"([\d\.]+)", 1).cast("double")
)

df = df.withColumn(
    "power_rpm",
    regexp_extract(col("max_power"), r"@(\d+)", 1).cast("int")
)


# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Étape 5 — Nettoyage colonnes inutiles

# COMMAND ----------

df = df.drop("max_torque", "max_power")


# COMMAND ----------

# MAGIC %md
# MAGIC Étape 6 — Vérifications qualité

# COMMAND ----------

df.filter(col("customer_age") < 18).count()
df.filter(col("vehicle_age") < 0).count()


# COMMAND ----------

# MAGIC %md
# MAGIC Étape 7 — Écriture Silver en Delta

# COMMAND ----------

df.write.format("delta").mode("overwrite").save(
    "abfss://silver@stinsuranceanalytics.dfs.core.windows.net/insurance_clean"
)
