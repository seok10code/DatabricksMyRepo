# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# JSON_PATH 확인 할 수 있습니다.
# dbutils.fs.ls('{DIR}')
json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"

# COMMAND ----------

# Bronze Table
# def 함수명이 Table 명입니다.

@dlt.table(
  comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
)
def clickstream_raw():
    return (spark.read.format("json").load(json_path))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Delta Live Table - Data Quality 기능
# MAGIC ####
# MAGIC
# MAGIC 1. @dlt.expect: 유효하지 않은 레코드가 대상에 기록됩니다. 해당 데이터 세트에 대한 메트릭으로 보고됩니다. (WARNING)
# MAGIC 2. @dlt.expect_or_drop: 데이터가 대상에 기록되기 전에 유효하지 않은 레코드가 삭제됩니다. 실패는 데이터 세트에 대한 메트릭으로 보고됩니다. (DROP)
# MAGIC 3. @dlt.expect_or_fail: 잘못된 레코드로 인해 업데이트가 성공하지 못합니다. 재처리하기 전에 수동 개입이 필요합니다. (FAIL)
# MAGIC
# MAGIC ###
# MAGIC * 여러개의 경우 예시: @dlt.expect_all({"valid_count": "count > 0", "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"}) 

# COMMAND ----------

#Silver Table
@dlt.table(
  comment="Wikipedia clickstream data cleaned and prepared for analysis."
)
@dlt.expect("valid_current_page_title", "current_page_title IS NOT NULL") #WARN
@dlt.expect_or_fail("valid_count", "click_count > 0") #fail
def clickstream_prepared():
    return (
    dlt.read("clickstream_raw")
      .withColumn("click_count", expr("CAST(n AS INT)"))
      .withColumnRenamed("curr_title", "current_page_title")
      .withColumnRenamed("prev_title", "previous_page_title")
      .select("current_page_title", "click_count", "previous_page_title")
  )

# COMMAND ----------

# Gold Table
@dlt.table(
  comment="A table containing the top pages linking to the Apache Spark page."
)
def top_spark_referrers():
    return (
    dlt.read("clickstream_prepared")
      .filter(expr("current_page_title == 'Apache_Spark'"))
      .withColumnRenamed("previous_page_title", "referrer")
      .sort(desc("click_count"))
      .select("referrer", "click_count")
      .limit(10)
  )
