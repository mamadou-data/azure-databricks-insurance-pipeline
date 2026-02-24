# Databricks notebook source
# Configure Azure storage account authentication
storage_account_name = "stinsuranceanalytics"
storage_account_key = os.getenv("AZURE_STORAGE_KEY")  # Replace with actual key

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Étape 1 — Lire les données Bronze (Delta)

df = spark.read.format("delta").load(
    "abfss://bronze@stinsuranceanalytics.dfs.core.windows.net/insurance_raw"
)

display(df)

# Étape 2 — Conversion Yes/No → Boolean

from pyspark.sql.functions import when, col

bool_columns = [c for c in df.columns if c.startswith("is_")]

for c in bool_columns:
    df = df.withColumn(
        c,
        when(col(c) == "Yes", True)
        .when(col(c) == "No", False)
        .otherwise(None)
    )


# Étape 3 — Parser max_torque

from pyspark.sql.functions import regexp_extract

df = df.withColumn(
    "torque_nm",
    regexp_extract(col("max_torque"), r"(\d+)", 1).cast("int")
)

df = df.withColumn(
    "torque_rpm",
    regexp_extract(col("max_torque"), r"@(\d+)", 1).cast("int")
)


# Étape 4 — Parser max_power

df = df.withColumn(
    "power_bhp",
    regexp_extract(col("max_power"), r"([\d\.]+)", 1).cast("double")
)

df = df.withColumn(
    "power_rpm",
    regexp_extract(col("max_power"), r"@(\d+)", 1).cast("int")
)

display(df)

# Étape 5 — Nettoyage colonnes inutiles

df = df.drop("max_torque", "max_power")

# Étape 6 — Vérifications qualité

df.filter(col("customer_age") < 18).count()
df.filter(col("vehicle_age") < 0).count()

# MAGIC Étape 7 — Écriture Silver en Delta

df.write.format("delta").mode("overwrite").save(
    "abfss://silver@stinsuranceanalytics.dfs.core.windows.net/insurance_clean"
)
