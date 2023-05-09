-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Delta Table Inside

-- COMMAND ----------

-- DBTITLE 1,describe detail <delta table> 
-- MAGIC %sql 
-- MAGIC describe detail iot_stream

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/iot_stream

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/iot_stream/_delta_log/

-- COMMAND ----------

select * from iot_stream limit 20;

-- COMMAND ----------

create table iot_stream_silver using delta partitioned by (device_id,user_id)
as select device_id, user_id, avg(calories_burnt) as avg_cal,avg(miles_walked) as avg_walked, avg(num_steps) as avg_steps from iot_stream group by device_id, user_id ;

-- COMMAND ----------

select * from iot_stream_silver

-- COMMAND ----------

select device_id ,count(*) from iot_stream_silver group by device_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Full DML Support

-- COMMAND ----------

delete from iot_stream_silver where device_id < 10;
select device_id ,count(*) from iot_stream_silver group by device_id

-- COMMAND ----------

create table iot_stream_silver_backfill using delta partitioned by (device_id,user_id)
as select device_id, user_id, avg(calories_burnt) as avg_cal,avg(miles_walked) as avg_walked, avg(num_steps) as avg_steps from iot_stream where device_id in (1,2,3,14,15) group by device_id, user_id 

-- COMMAND ----------

merge into iot_stream_silver as t 
using iot_stream_silver_backfill as s 
on t.device_id = s.device_id AND t.user_id = s.user_id 
WHEN MATCHED THEN 
  UPDATE SET *
WHEN NOT MATCHED 
  THEN INSERT * ;

-- COMMAND ----------

select device_id ,count(*) from iot_stream_silver group by device_id

-- COMMAND ----------


