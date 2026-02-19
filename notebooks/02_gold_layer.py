# Databricks notebook source
# Configure Azure storage account authentication
storage_account_name = "stinsuranceanalytics"
storage_account_key = os.getenv("AZURE_STORAGE_KEY")

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objectif : créer des tables analytiques propres pour Power BI

# COMMAND ----------

# MAGIC %md
# MAGIC 1) Lire la Silver

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id

silver_path = "abfss://silver@stinsuranceanalytics.dfs.core.windows.net/insurance_clean"
df = spark.read.format("delta").load(silver_path)

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC 2) DIMENSIONS

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1 dim_customer
# MAGIC
# MAGIC On prend les attributs client/contrat “stables”.

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws

dim_customer = (
    df.select("customer_age", "subscription_length")
      .dropDuplicates()
      .withColumn("customer_key", sha2(concat_ws("||", col("customer_age"), col("subscription_length")), 256))
      .select("customer_key", "customer_age", "subscription_length")
)

display(dim_customer.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC 2.2 dim_region

# COMMAND ----------

dim_region = (
    df.select("region_code", "region_density")
      .dropDuplicates()
      .withColumn("region_key", sha2(concat_ws("||", col("region_code"), col("region_density")), 256))
      .select("region_key", "region_code", "region_density")
)

display(dim_region.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC 2.3 dim_vehicle
# MAGIC
# MAGIC On regroupe les attributs véhicule + les champs parsés.

# COMMAND ----------

vehicle_cols = [
    "segment", "model", "fuel_type", "engine_type", "transmission_type",
    "steering_type", "rear_brakes_type",
    "displacement", "cylinder", "vehicle_age",
    "turning_radius", "length", "width", "gross_weight",
    "ncap_rating",
    "torque_nm", "torque_rpm", "power_bhp", "power_rpm"
]

dim_vehicle = (
    df.select(*vehicle_cols)
      .dropDuplicates()
      .withColumn("vehicle_key", sha2(concat_ws("||", *[col(c).cast("string") for c in vehicle_cols]), 256))
      .select(["vehicle_key"] + vehicle_cols)
)

display(dim_vehicle.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC 3) FACT TABLE


# MAGIC %md
# MAGIC 3.1 fact_policy
# MAGIC
# MAGIC On relie la policy aux dimensions + la mesure claim_status.
# MAGIC
# MAGIC On va faire des joins avec les mêmes règles de clés.

# COMMAND ----------

fact_base = df.select(
    "policy_id", "claim_status",
    "customer_age", "subscription_length",
    "region_code", "region_density",
    *vehicle_cols
)

# Join dim_customer
fact = fact_base.join(
    dim_customer,
    on=["customer_age", "subscription_length"],
    how="left"
)

# Join dim_region
fact = fact.join(
    dim_region,
    on=["region_code", "region_density"],
    how="left"
)

# Join dim_vehicle
fact = fact.join(
    dim_vehicle,
    on=vehicle_cols,
    how="left"
)

fact_policy = fact.select(
    "policy_id",
    "customer_key",
    "region_key",
    "vehicle_key",
    "claim_status"
)

display(fact_policy.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC 4) Écrire en Gold (Delta)

# COMMAND ----------

gold_base = "abfss://gold@stinsuranceanalytics.dfs.core.windows.net/insurance_star"

(dim_customer.write.format("delta").mode("overwrite")
 .save(f"{gold_base}/dim_customer"))

(dim_region.write.format("delta").mode("overwrite")
 .save(f"{gold_base}/dim_region"))

(dim_vehicle.write.format("delta").mode("overwrite")
 .save(f"{gold_base}/dim_vehicle"))

(fact_policy.write.format("delta").mode("overwrite")
 .save(f"{gold_base}/fact_policy"))


# COMMAND ----------

# MAGIC %md
# MAGIC 5) Contrôles rapides qualité (à faire)
# MAGIC
# MAGIC 5.1 Vérifier les clés nulles (joins)

# COMMAND ----------

from pyspark.sql.functions import sum as fsum, when

fact_policy.select(
    fsum(when(col("customer_key").isNull(), 1).otherwise(0)).alias("null_customer_key"),
    fsum(when(col("region_key").isNull(), 1).otherwise(0)).alias("null_region_key"),
    fsum(when(col("vehicle_key").isNull(), 1).otherwise(0)).alias("null_vehicle_key")
).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Enregistrer tes tables Gold dans le Metastore

# COMMAND ----------

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  storage_account_key
)

# COMMAND ----------

gold_base = "abfss://gold@stinsuranceanalytics.dfs.core.windows.net/insurance_star"
