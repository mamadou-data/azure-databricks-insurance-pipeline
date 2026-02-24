# Databricks notebook source
storage_account_name = "stinsuranceanalytics"
storage_account_key = os.getenv("AZURE_STORAGE_KEY")

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)


# Teste
dbutils.fs.mounts()

files = dbutils.fs.ls("abfss://bronze@stinsuranceanalytics.dfs.core.windows.net/")
if files:
    display(files)
else:
    print("The bronze container is empty (no files or folders found)")


# Afficher le répertoire

display(
    dbutils.fs.ls("abfss://bronze@stinsuranceanalytics.dfs.core.windows.net/")
)



# Lecture du fichier

df_raw = spark.read.csv(
    "abfss://bronze@stinsuranceanalytics.dfs.core.windows.net/insurance_claims.csv",
    header=True,
    inferSchema=True
)

display(df_raw)


# On écrit en format Delta

df_raw.write.format("delta").mode("overwrite").save(
    "abfss://bronze@stinsuranceanalytics.dfs.core.windows.net/insurance_raw"
)
