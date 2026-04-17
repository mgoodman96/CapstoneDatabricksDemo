# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # ETL Pipelie for LongLaMP dataset into Databricks Unity Catalog
# MAGIC https://huggingface.co/datasets/LongLaMP/LongLaMP

# COMMAND ----------

# MAGIC %md
# MAGIC Start by creating your schema in Unity Catalog.
# MAGIC
# MAGIC A **schema** is a logical container within a catalog that organizes tables, views, and other data objects. Schemas help manage permissions, structure data, and separate environments (e.g., dev, test, prod).
# MAGIC
# MAGIC Schemas can be structured using the **medallion architecture**:
# MAGIC - **Bronze**: Raw, unprocessed data ingested from source systems.
# MAGIC - **Silver**: Cleaned and enriched data, ready for analytics.
# MAGIC - **Gold**: Curated, business-level data for reporting and advanced analytics.
# MAGIC
# MAGIC This layered approach improves data quality, governance, and scalability.

# COMMAND ----------

#Create Schema in UC
catalog_name = "longlamp_catalog"
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{gold_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## What is a Volume?
# MAGIC
# MAGIC A **volume** in Databricks Unity Catalog is a managed storage location within a schema that allows you to store and organize files (such as CSV, Parquet, images, or other unstructured data) alongside your tables and views. Volumes provide a secure, governed, and scalable way to manage files directly in Unity Catalog.
# MAGIC
# MAGIC ### Purpose of Volumes
# MAGIC
# MAGIC - **Data Organization**: Store raw or intermediate files in a structured, discoverable way within your data lake.
# MAGIC - **Access Control**: Apply fine-grained permissions to files using Unity Catalog's security model.
# MAGIC - **Collaboration**: Enable teams to share and access files securely across notebooks and workflows.
# MAGIC - **ETL Pipelines**: Use volumes to stage data before loading into tables, or to export results for downstream processing.
# MAGIC
# MAGIC Volumes help unify file and table data management, making it easier to build robust, governed data pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# Create a volume in the bronze schema
bronze_volume = "longlamp_raw"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{bronze_schema}.{bronze_volume}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LongLampDataset
# MAGIC **Dataset Summary**
# MAGIC LongLaMP is a comprehensive benchmark for personalized long-form text generation. The dataset is designed to evaluate and improve the performance of language models in generating extended, personalized content across various domains and tasks.
# MAGIC
# MAGIC
# MAGIC {kumar2024longlampbenchmarkpersonalizedlongform,
# MAGIC       title={LongLaMP: A Benchmark for Personalized Long-form Text Generation}, 
# MAGIC       author={Ishita Kumar and Snigdha Viswanathan and Sushrita Yerra and Alireza Salemi and Ryan A. Rossi and Franck Dernoncourt and Hanieh Deilamsalehy and Xiang Chen and Ruiyi Zhang and Shubham Agarwal and Nedim Lipka and Chien Van Nguyen and Thien Huu Nguyen and Hamed Zamani},
# MAGIC       year={2024},
# MAGIC       eprint={2407.11016},
# MAGIC       archivePrefix={arXiv},
# MAGIC       primaryClass={cs.CL},
# MAGIC       url={https://arxiv.org/abs/2407.11016}, 
# MAGIC }

# COMMAND ----------

import requests

url = "https://huggingface.co/api/datasets/LongLaMP/LongLaMP/parquet/abstract_generation_temporal/train"
response = requests.get(url)
data = response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC URL has multiple parquet files as can be seen below, each link is a partition

# COMMAND ----------

data

# COMMAND ----------

# DBTITLE 1,Cell 7
#load the parquets in data into volume, each link is a get request
for i in range(len(data)):
  url = data[i]
  response = requests.get(url)
  volume_path = f"/Volumes/{catalog_name}/{bronze_schema}/{bronze_volume}/{i}.parquet"
  with open(volume_path, "wb") as f:
    f.write(response.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Once data is in Volume, you can load the data into your UC Layers

# COMMAND ----------

volume_path = f"/Volumes/{catalog_name}/{bronze_schema}/{bronze_volume}/*.parquet"
df = spark.read.parquet(volume_path)

# COMMAND ----------

df.display()

# COMMAND ----------

table_name = "longlamp_abstract_generation_temporal"
df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{bronze_schema}.{table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from longlamp_catalog.bronze.longlamp_abstract_generation_temporal limit 10;
