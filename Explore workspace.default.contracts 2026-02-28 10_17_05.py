# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `workspace`.`default`.`claims`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `workspace`.`default`.`contracts`;

# COMMAND ----------

# DBTITLE 1,Cell 2
#Bronze Tables (Unity Catalog Managed Tables)
from pyspark.sql.functions import *

# Read contracts and claims from managed Unity Catalog tables
df_contracts = spark.table("workspace.default.contracts")
df_claims = spark.table("workspace.default.claims")

# Save as managed Unity Catalog tables
df_contracts.write.mode("overwrite").format("delta").saveAsTable("workspace.default.contracts_bronze")
df_claims.write.mode("overwrite").format("delta").saveAsTable("workspace.default.claims_bronze")



# COMMAND ----------

from pyspark.sql import SparkSession

def create_bronze_tables(spark: SparkSession):

    # Assuming raw data already uploaded to volume
    contracts = spark.read.option("header", "true") \
        .csv("/Volumes/workspace/default/raw/contracts/")

    claims = spark.read.option("header", "true") \
        .csv("/Volumes/workspace/default/raw/claims/")

    contracts.write.mode("overwrite") \
        .format("delta") \
        .saveAsTable("workspace.default.contracts")

    claims.write.mode("overwrite") \
        .format("delta") \
        .saveAsTable("workspace.default.claims")

    print("Bronze tables created successfully.")

# COMMAND ----------

# DBTITLE 1,Cell 3
#Silver Transformation (Catalog-Based)
contracts = spark.table("workspace.default.contracts_bronze")
claims = spark.table("workspace.default.claims_bronze")

# Rename columns in contracts to avoid duplicate names
contracts = contracts.withColumnRenamed("CONTRACT_ID", "CONTRACT_ID_contr")
contracts = contracts.withColumnRenamed("CREATION_DATE", "CONTRACT_CREATION_DATE")
contracts = contracts.withColumnRenamed("SOURCE_SYSTEM", "SOURCE_SYSTEM_CONTRACT")

silver = claims.join(
    contracts,
    (claims.CONTRACT_ID == contracts.CONTRACT_ID_contr) &
    (claims.CONTRACT_SOURCE_SYSTEM == contracts.SOURCE_SYSTEM_CONTRACT),
    "inner"
)

silver.write.mode("overwrite").format("delta") \
    .saveAsTable("workspace.default.europe3_silver")

# COMMAND ----------

# DBTITLE 1,Cell 4
#Gold: Transactions Partition by BUSINESS_DATE
from pyspark.sql.functions import *
from delta.tables import DeltaTable

silver = spark.table("workspace.default.europe3_silver")

gold_df = (
    silver
    .withColumn("CONTRACT_SOURCE_SYSTEM", lit("Europe 3"))
    .withColumn("CONTRACT_SOURCE_SYSTEM_ID", col("CONTRACT_ID_contr").cast("long")) # updated to use CONTRACT_ID_contr
    .withColumn("SOURCE_SYSTEM_ID",
        regexp_replace("CLAIM_ID", "^[A-Z_]+", "").cast("int"))
    .withColumn("TRANSACTION_TYPE",
        when(col("CLAIM_TYPE") == "2", "Corporate")
        .when(col("CLAIM_TYPE") == "1", "Private")
        .otherwise("Unknown"))
    .withColumn("TRANSACTION_DIRECTION",
        when(col("CLAIM_ID").startswith("CL"), "COINSURANCE")
        .when(col("CLAIM_ID").startswith("RX"), "REINSURANCE"))
    .withColumn("CONFORMED_VALUE", col("AMOUNT").cast("decimal(16,5)"))
    .withColumn("BUSINESS_DATE", to_date("DATE_OF_LOSS", "dd.MM.yyyy"))
    .withColumn("CONTRACT_CREATION_DATE",
        to_timestamp("CONTRACT_CREATION_DATE", "dd.MM.yyyy HH:mm")) # updated to use renamed contract creation date
    .withColumn("SYSTEM_TIMESTAMP", current_timestamp())
    .withColumn("NSE_ID", md5(col("CLAIM_ID")))
)

# Create table if not exists
if not spark.catalog.tableExists("workspace.default.transactions"):
    gold_df.write.format("delta").partitionBy("BUSINESS_DATE").saveAsTable("workspace.default.transactions")
else:
    delta = DeltaTable.forName(spark, "workspace.default.transactions")
    delta.alias("t").merge(
        gold_df.alias("s"),
        "t.NSE_ID = s.NSE_ID"
    ).whenNotMatchedInsertAll().execute()


# COMMAND ----------

# DBTITLE 1,Get transaction output with exact schema
from pyspark.sql.functions import col

# Read the gold transactions table
transactions = spark.table("workspace.default.transactions")

# Make TRANSACTION_TYPE non-nullable by filling nulls
transactions = transactions.na.fill({'TRANSACTION_TYPE': 'Unknown'})

# Select, rename, and cast columns to match required output
result = (transactions
    .select(
        col("CONTRACT_SOURCE_SYSTEM").cast("string"),
        col("CONTRACT_SOURCE_SYSTEM_ID").cast("long"),
        col("SOURCE_SYSTEM_ID").cast("int"),
        col("TRANSACTION_TYPE").cast("string"),
        col("TRANSACTION_DIRECTION").cast("string"),
        col("CONFORMED_VALUE").cast("decimal(16,5)"),
        col("BUSINESS_DATE").cast("date"),
        col("CONTRACT_CREATION_DATE").alias("CREATION_DATE").cast("timestamp"), # rename column
        col("SYSTEM_TIMESTAMP").cast("timestamp"),
        col("NSE_ID").cast("string")
    )
)

display(result)

# COMMAND ----------

assert result.count() > 0
assert all(field in result.schema.names for field in [
    "CONTRACT_SOURCE_SYSTEM", "CONTRACT_SOURCE_SYSTEM_ID",
    "SOURCE_SYSTEM_ID", "TRANSACTION_TYPE", "TRANSACTION_DIRECTION",
    "CONFORMED_VALUE", "BUSINESS_DATE", "CREATION_DATE",
    "SYSTEM_TIMESTAMP", "NSE_ID"])
assert result.schema["TRANSACTION_TYPE"].nullable is False