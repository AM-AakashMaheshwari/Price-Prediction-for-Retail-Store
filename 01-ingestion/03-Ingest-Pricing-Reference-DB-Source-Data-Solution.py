# Databricks notebook source
# MAGIC %md
# MAGIC ####Notebook Name : 03-Ingest-Pricing-Reference-DB-Source-Data
# MAGIC ##### Source Table Details
# MAGIC Source Tables:
# MAGIC masterdata.market_address
# MAGIC masterdata.country_profile
# MAGIC masterdata.exchange_rates
# MAGIC masterdata.domestic_product_codes
# MAGIC masterdata.global_item_codes
# MAGIC
# MAGIC
# MAGIC ##### Source Tables Ingestion Path In Bronze Layer:
# MAGIC  "abfss://bronze@datalakestorageaccountname.dfs.core.windows.net/reference-data/"
# MAGIC

# COMMAND ----------

pricingReferenceSourceTableName = dbutils.widgets.get("prm_pricingReferenceSourceTableName")

pricingReferenceSinkLayerName = 'bronze'
pricingReferenceSinkStorageAccountName = 'pricepredictionadls'
pricingReferenceSinkFolderName = 'reference-data'

# COMMAND ----------


JDBCconnectionUrl = "jdbc:sqlserver://asqludacoursesserver.database.windows.net;encrypt=true;databaseName=asqludacourses;user=sourcereader;password=DBReader@2024";

print(JDBCconnectionUrl)



pricingReferenceSourceTableDF =  (spark
                                 .read
                                 .format('jdbc')
                                 .option("url",JDBCconnectionUrl)
                                 .option("dbtable",pricingReferenceSourceTableName)
                                 .load()
)



# COMMAND ----------

pricingReferenceSinkTableFolder = pricingReferenceSourceTableName.replace('.','/')

# COMMAND ----------

pricingReferenceSinkFolderPath = f"abfss://{pricingReferenceSinkLayerName}@{pricingReferenceSinkStorageAccountName}.dfs.core.windows.net/{pricingReferenceSinkFolderName}/{pricingReferenceSinkTableFolder}"

(
    pricingReferenceSourceTableDF
    .write
    .mode("overwrite")
    .json(pricingReferenceSinkFolderPath)
)


