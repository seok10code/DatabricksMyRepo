# Databricks notebook source
# MAGIC %md ### <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> Background
# MAGIC **Common ETL Workload:** 새로운 데이터가 Cloud Blob 스토리지에 저장되면 신규분에 대한 incremental한 프로세싱 제공 (Ex: IoT data streams)
# MAGIC
# MAGIC **Out-of-the-box Solution:** 
# MAGIC * The naive file-based streaming source (Azure | AWS) 의 경우 신규 파일의 도착을 주기적으로 해당 cloud 디렉토리를 listing하여 tracking함. 
# MAGIC * 파일수가 많아질수록 **비용** 과  **latency** 이 급증할 수 있음 
# MAGIC <br />
# MAGIC
# MAGIC **Typical Workarounds...** 
# MAGIC * **Schedule & Batch Process:** Though data is arriving every few minutes, you batch the data together in a directory and then process them in a schedule. Using day or hour based partition directories is a common technique. This lengthens the SLA for making the data available to downstream consumers. Leads to high end-to-end data latencies.
# MAGIC * **Manual DevOps Approach:** To keep the SLA low, you can alternatively leverage cloud notification service and message queue service to notify when new files arrive to a message queue and then process the new files. This approach not only involves a manual setup process of required cloud services, but can also quickly become complex to manage when there are multiple ETL jobs that need to load data. Furthermore, re-processing existing files in a directory involves manually listing the files and handling them in addition to the cloud notification setup thereby adding more complexity to the setup.  
# MAGIC
# MAGIC <br />
# MAGIC   
# MAGIC **The SOLUTION = AutoLoader**
# MAGIC * An optimized file source that overcomes all the above limitations and provides a seamless way for data teams to load the raw data at low cost and latency with minimal DevOps effort. 
# MAGIC * Just provide a source directory path and start a streaming job. 
# MAGIC * The new structured streaming source, called “cloudFiles”, will automatically set up file notification services that subscribe file events from the input directory and process new files as they arrive, with the option of also processing existing files in that directory.
# MAGIC
# MAGIC **Benefits**
# MAGIC * **No file state management:** The source incrementally processes new files as they land on cloud storage. You don’t need to manage any state information on what files arrived.
# MAGIC * **Scalable:** The source will efficiently track the new files arriving by leveraging cloud services and RocksDB without having to list all the files in a directory. This approach is scalable even with millions of files in a directory.
# MAGIC * **Easy to use:** The source will automatically set up notification and message queue services required for incrementally processing the files. No setup needed on your side.
# MAGIC <br /> <img src="https://www.staples-3p.com/s7/is/image/Staples/sp36619013_sc7?wid=200&hei=200">

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/autoloader.png">

# COMMAND ----------

# MAGIC %md ### <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> Getting your data into Delta Lake with Auto Loader and COPY INTO
# MAGIC Incrementally and efficiently load new data files into Delta Lake tables as soon as they arrive in your data lake (S3/Azure Data Lake/Google Cloud Storage).
# MAGIC
# MAGIC <!-- <img src="https://databricks.com/wp-content/uploads/2021/02/telco-accel-blog-2-new.png" width=800/> -->
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-data-ingestion.png" width=1000/>
# MAGIC
# MAGIC <!-- <img src="https://databricks.com/wp-content/uploads/2020/02/dl-workflow2.png" width=750/> -->

# COMMAND ----------

# MAGIC %md ### Setup

# COMMAND ----------

# clean up the workspace
dbutils.fs.rm("/mnt/damount/iot_stream/", recurse=True)
dbutils.fs.rm("/mnt/damount/iot_stream_chkpts/", recurse=True)
spark.sql(f"DROP TABLE IF EXISTS iot_stream")
spark.sql(f"DROP TABLE IF EXISTS iot_devices")
spark.sql("SET spark.databricks.cloudFiles.schemaInference.enabled=true")
dbutils.fs.cp("/databricks-datasets/iot-stream/data-device/part-00000.json.gz",
              "/mnt/damount/iot_stream/part-00000.json.gz", recurse=True)

# COMMAND ----------

input_data_path = "/mnt/damount/iot_stream/"
chkpt_path = "/mnt/damount/iot_stream_chkpts/"
schema_location = "/mnt/damount/schema_location/" # added this

# COMMAND ----------

# MAGIC %md  `'streamfiles.py` 를 수행해서 타겟디렉토리에 파일 landing 을 시뮬레이션합니다

# COMMAND ----------

# MAGIC %md ### <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> Auto Loader

# COMMAND ----------

df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", schema_location) # added this
      .load(input_data_path))

(df.writeStream.format("delta")
 .option("checkpointLocation", chkpt_path)
   .table("iot_stream"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from accounts

# COMMAND ----------

spark.conf.set(
  "spark.sql.streaming.stateStore.providerClass",
  "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------


