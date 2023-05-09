# Databricks notebook source
import time

#Copies a bunch of iot files into our path
def insert_new_files(path):
    path = path if path.endswith("/") else path + "/"
        
    for i in range(1, 20):
        i = str(i).zfill(2)
        target_path = str(f"{path}part-000{i}.json.gz")
        print(f"Inserted new file: {target_path}")
        dbutils.fs.cp(f"dbfs:/databricks-datasets/iot-stream/data-device/part-000{i}.json.gz", target_path)
        time.sleep(3)

    for i in range(21, 100):
        i = str(i).zfill(2)
        target_path = str(f"{path}part-000{i}.json.gz")
        print(f"Inserted new file: {target_path}")
        dbutils.fs.cp("dbfs:/databricks-datasets/iot-stream/data-device/part-00019.json.gz", target_path)
        time.sleep(7)
        
#dbutils.fs.cp(source, target) copies file from "source" into "target"

# COMMAND ----------

insert_new_files(path="/mnt/damount/iot_stream/")

# COMMAND ----------


