-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Delta Lake 퀵 스타트(SQL)
-- MAGIC
-- MAGIC ## 목적
-- MAGIC
-- MAGIC 이 노트북에서는 Spark SQL을 사용하여 데이터 및 Delta Lake 형식과 상호 작용하는 다양한 방법을 논의하고 시연합니다. 다양한 데이터 소스를 쿼리하는 방법, 해당 데이터를 Delta에 저장하는 방법, 메타데이터를 관리 및 조회하는 방법, 일반적인 SQL DML 명령(MERGE, UPDATE, DELETE)을 사용하여 데이터를 정제하는 방법, Delta Lake를 관리하고 최적화하는 방법에 대해서 배우게 됩니다.
-- MAGIC
-- MAGIC 이 코스에서는 다음과 같은 작업을 수행하고 자신만의 Delta Lake를 만들게 됩니다.
-- MAGIC
-- MAGIC 1. Spark SQL로 다른 종류의 데이터 소스를 직접 쿼리  
-- MAGIC 2. 관리형 테이블 생성(델타 및 비델타 테이블 모두) 
-- MAGIC 3. 메타데이터 생성, 제어, 검색
-- MAGIC 4. MERGE, UPDATE, DELETE 구문을 활용한 데이터셋 정제  
-- MAGIC 5. 데이터의 과거 버전 탐색  
-- MAGIC 6. SQL로 스트리밍 데이터 직접 쿼리  
-- MAGIC 7. 병합, 집계 수행하여 최종 레이어 생성  
-- MAGIC 8. 테이블 최적화 및 데이터 레이크 관리  
-- MAGIC
-- MAGIC ⚠️ Please use DBR 8.* to run this notebook

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 설정
-- MAGIC
-- MAGIC 아래의 셀을 실행해서 임시 데이터베이스를 셋업 합니다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC databricks_user = spark.sql("SELECT current_user()").collect()[0][0].split('@')[0].replace(".", "_")
-- MAGIC print(databricks_user)
-- MAGIC
-- MAGIC spark.sql("DROP DATABASE IF EXISTS delta_{}_db CASCADE".format(str(databricks_user)))
-- MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS delta_{}_db".format(str(databricks_user)))
-- MAGIC spark.sql("USE delta_{}_db".format(str(databricks_user)))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 데이터 레이크 쿼리
-- MAGIC
-- MAGIC 데이터브릭스는 퍼블릭 클라우드의 오브젝트 스토리지(S3/ADLS/Google Storage)에 저정된 파일을 쿼리하는 것을 지원합니다.
-- MAGIC 이번 과정에서는 데이터브릭스 호스팅 저장소의 파일을 쿼리합니다.
-- MAGIC
-- MAGIC [These files are mounted to your Databricks File System under the /databricks-datasets/ path](https://docs.databricks.com/data/databricks-datasets.html), which makes them easier to reference from notebook code.
-- MAGIC
-- MAGIC 기존 Bucket을 마운트 하는 방법(Azure, AWS) [클릭](https://docs.databricks.com/data/databricks-file-system.html#mount-storage).
-- MAGIC
-- MAGIC Azure Gen2 [클릭](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 매직 커맨드
-- MAGIC
-- MAGIC [Magic commands](https://docs.databricks.com/notebooks/notebooks-use.html#mix-languages)는 노트북 내에서 언어를 변경할 수 있게 해주는 숏컷
-- MAGIC
-- MAGIC `%fs`는 [dbutils files sytem utilities]를 사용할 수 있게 해주는 숏컷(https://docs.databricks.com/dev-tools/databricks-utils.html#file-system-utilities). 이 코스에서는 IOT 스트리밍 데이터 디렉터리 아래에 파일 리스트를 확인해볼 것 입니다.
-- MAGIC
-- MAGIC 디바이스 데이터가 다수의 압축된 JSON 파일로 나누어져 있으며, 개별 파일을 지정하지 않고 디렉터리를 지정해서 해당 데이터를 읽을 것 입니다.

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/iot-stream/data-user

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/iot-stream/data-device

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CSV Data
-- MAGIC
-- MAGIC CSV 데이터는 아래와 같은 방식으로 직접 쿼리할 수 있지만, 데이터에 헤더가 있거나 옵션을 지정하고 싶으면 임시 뷰를 생성해야 합니다.

-- COMMAND ----------

SELECT * FROM csv.`/databricks-datasets/iot-stream/data-user/userData.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 보이는 것 처럼 이 데이터셋은 헤더가 있고, 해당 정보를 적절하게 쿼리하려면 임시 뷰를 만들어야 합니다. 특히 헤더와 스키마를 지정하려면 임시 뷰를 만들어야 합니다. 스키마를 지정하면 초기 추론 스캔을 방지하여 성능을 향상 시킬 수 있습니다. 하지만 이 데이터셋은 작기 때문에 스키마 유추를 하도록 합니다.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW userData
USING csv
OPTIONS(
  path '/databricks-datasets/iot-stream/data-user/userData.csv',
  header true,
  inferSchema true
);

SELECT * FROM userData;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## JSON Data
-- MAGIC
-- MAGIC JSON 파일은 CSV 처럼 직접 쿼리할 수도 있습니다. Spark는 JSON 파일 전체 디렉터리를 읽고 직접 쿼리할 수 있습니다. CSV 직접 읽기와 유사한 문장을 사용하겠지만 여기에서는 JSON을 지정하겠습니다.

-- COMMAND ----------

SELECT * FROM JSON.`/databricks-datasets/iot-stream/data-device` 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 테이블 생성
-- MAGIC
-- MAGIC 테이블을 메타스토어에 등록할 수 있고 다른 사용자가 해당 테이블을 사용할 수 있습니다. 메타스토어에 등록된 새 데이터베이스 및 테이블은 왼쪽의 데이터 탭에서 찾을 수 있습니다.
-- MAGIC
-- MAGIC 두 유형의 테이블을 생성할 수 있습니다. [managed and unmanaged](https://docs.microsoft.com/en-us/azure/databricks/data/tables#--managed-and-unmanaged-tables)
-- MAGIC - Managed 테이블은 메타스토어에 저장되고 모든 관리는 메타스토어에서 합니다. Managed 테이블을 삭제하면 관련 파일 및 데이터가 삭제됩니다.
-- MAGIC - Unmanaged 테이블은 다른 위치에 저장된 파일 및 데이터에 대한 포인터입니다. Unmanaged 테이블을 삭제하면 메타데이터는 삭제되고 해당 데이터셋을 참조하는 별칭도 제거합니다. 하지만 근본적인 데이터와 파일은 원 경로에 지워지지 않은 상태로 남아 있습니다.
-- MAGIC
-- MAGIC 이 과정에서는 Managed 테이블을 사용합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CSV User Table
-- MAGIC
-- MAGIC 실습 첫 번째 테이블을 만들어 봅시다! 운영 환경에서는 데이터를 오브젝트 스토리지 버켓에 저장할 수 있도록 `LOCATION`을 지정하여 unmanaged 테이블을 사용하는 것이 best practice 입니다. 이 실습에서는 간단하게 작업을 하기 위해서 생략을 하였습니다.
-- MAGIC
-- MAGIC 초기 테이블은 user_data_raw_csv 파일이며 이 테이블을 사용합니다.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_raw_csv(
  userid STRING,
  gender STRING,
  age INT,
  height INT,
  weight INT,
  smoker STRING,
  familyhistory STRING,
  chosestlevs STRING,
  bp STRING,
  risk INT)
USING csv
OPTIONS (path "/databricks-datasets/iot-stream/data-user/userData.csv", header "true");

DESCRIBE TABLE user_data_raw_csv

-- COMMAND ----------

describe formatted user_data_raw_csv

-- COMMAND ----------

SELECT * FROM user_data_raw_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### JSON Device Table

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/iot-stream/data-device

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC du -h /dbfs/databricks-datasets/iot-stream/

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS device_data_raw_json(
  calories_burnt DOUBLE,
  device_id INT,
  id STRING,
  miles_walked DOUBLE,
  num_steps INT,
  `timestamp` TIMESTAMP,
  user_id STRING,
  value STRING
)
USING JSON
OPTIONS (path "/databricks-datasets/iot-stream/data-device")

-- COMMAND ----------

select * from device_data_raw_json limit 10;

-- COMMAND ----------

select user_id, device_id, value:user_id, value:num_steps from device_data_raw_json limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### JSON에서 Delta Table 생성

-- COMMAND ----------

--drop table device_data_bronze_delta;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS device_data_bronze_delta
USING DELTA
PARTITIONED BY (device_id)
AS SELECT * FROM JSON.`/databricks-datasets/iot-stream/data-device`;

SELECT * FROM device_data_bronze_delta

-- COMMAND ----------

DESCRIBE formatted device_data_bronze_delta

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC ls /user/hive/warehouse/device_data_bronze_delta/device_id=3/

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC describe history device_data_bronze_delta

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC delete from device_data_bronze_delta where device_id =3

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC describe history device_data_bronze_delta

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC ls /user/hive/warehouse/device_data_bronze_delta/device_id=3

-- COMMAND ----------

describe device_data_bronze_delta

-- COMMAND ----------

describe extended device_data_bronze_delta

-- COMMAND ----------

describe formatted device_data_bronze_delta

-- COMMAND ----------

describe detail device_data_bronze_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CSV 테이블에서 Delta Table 생성

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_bronze_delta 
USING DELTA
PARTITIONED BY (gender)
COMMENT "User Data Raw Table - No Transformations"
AS SELECT * FROM user_data_raw_csv;

SELECT * FROM user_data_bronze_delta

-- COMMAND ----------

DESCRIBE TABLE user_data_bronze_delta 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 메타데이터 추가
-- MAGIC
-- MAGIC Comment는 컬럼, 테이블 수준에서 추가할 수 있습니다. 
-- MAGIC `ALTER TABLE` 구문을 사용해서 테이블 혹은 컬럼에 comment를 추가합니다.[생성 시에도 가능](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-hiveformat.html#create-table-with-hive-format) (상기 예 참조).
-- MAGIC
-- MAGIC Comment 내용도 UI에 표시됩니다.

-- COMMAND ----------

ALTER TABLE user_data_bronze_delta ALTER COLUMN bp COMMENT 'Can be High or Normal';
ALTER TABLE user_data_bronze_delta ALTER COLUMN smoker COMMENT 'Can be Y or N';
ALTER TABLE user_data_bronze_delta ALTER COLUMN familyhistory COMMENT 'Can be Y or N';
ALTER TABLE user_data_bronze_delta ALTER COLUMN chosestlevs COMMENT 'Can be High or Normal';
ALTER TABLE user_data_bronze_delta SET TBLPROPERTIES ('comment' = 'User Data Raw Table describing risk levels based on health factors');

-- COMMAND ----------

-- this shows the column schema and comments
DESCRIBE TABLE user_data_bronze_delta

-- COMMAND ----------

ALTER TABLE user_data_bronze_delta ALTER COLUMN age COMMENT '나이';
ALTER TABLE user_data_bronze_delta ALTER COLUMN userid COMMENT '사용자 아이디 정보이고 unique한 값으로 되어 있습니다';

-- COMMAND ----------

-- you can see the table comment in the detailed Table Information at the bottom - under the columns
DESCRIBE TABLE EXTENDED user_data_bronze_delta

-- COMMAND ----------

-- DBTITLE 1,여기서 잠깐 운영 할 때 이런 자료도 필요합니다.
-- MAGIC %python
-- MAGIC #테이블 리스트 및 empty 데이터프레임 생성
-- MAGIC #spark.sql("use delta_sangbae_lim_db")
-- MAGIC spark.sql("USE delta_{}_db".format(str(databricks_user)))
-- MAGIC spark.sql("drop table IF EXISTS  detailedTableInfoList")
-- MAGIC myTList=spark.sql("show tables").select('tableName').where("isTemporary == false")
-- MAGIC resultDF = spark.sql(f"desc detail {myTList.take(1)[0]['tableName']}")
-- MAGIC resultDF = resultDF.where("1==0")
-- MAGIC #최종 "desc detail 테이블명" 결과를 하나의 데이터프레임으로 생성
-- MAGIC for table in myTList.collect():  
-- MAGIC   myDF=spark.sql(f"desc detail {table[0]}")
-- MAGIC   resultDF = resultDF.union(myDF)
-- MAGIC #최종 수집된 내용을 델타 테이블(External)에 저장
-- MAGIC #resultDF.write.format("delta").mode("overwrite").option("path","/FileStore/flight/megazone/detailedTableInfoList").saveAsTable("detailedTableInfoList")
-- MAGIC resultDF.write.format("delta").mode("overwrite").saveAsTable("detailedTableInfoList")

-- COMMAND ----------

select * from detailedTableInfoList where format ='delta'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 데이터 탭에서 테이블 표시
-- MAGIC
-- MAGIC Click on the Data tab.
-- MAGIC <img src="https://docs.databricks.com/_images/data-icon.png" /></a>
-- MAGIC
-- MAGIC You can see your database.table in the data tab
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://docs.databricks.com/_images/default-database.png"/></a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DELETE, UPDATE, UPSERT
-- MAGIC
-- MAGIC 파케이(parquet)는 전통적으로 데이터 레이크의 주요 데이터 스토리지 계층입니다. 하지만 몇 가지 심각한 단점이 있습니다.
-- MAGIC 파케이는 완전한 SQL DML을 지원하지 않습니다. 파케이는 변경이 불가능하며, Append만을 지원합니다. 하나의 행을 삭제하기 위해서는 전체 파일을 다시 생성해야 합니다.
-- MAGIC
-- MAGIC Delta는 데이터 레이크 상에서 SQL DML 전체를 사용할 수 있습니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DELETE
-- MAGIC
-- MAGIC Delta는 SQL DML Delete를 지원하여 GDPR 및 CCPA 활용 사례에 사용할 수 있습니다.
-- MAGIC
-- MAGIC 현재 데이터에 2명의 25세 사용자가 등록되어 있고 방금 이중에서 사용자 아이디 21번을 삭제해야 한다는 요청을 받았습니다(right to be forgotten)

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta WHERE age = 25

-- COMMAND ----------

DELETE FROM user_data_bronze_delta where userid = 21;

SELECT * FROM user_data_bronze_delta WHERE age = 25

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### UPDATE
-- MAGIC
-- MAGIC Delta는 간단한 SQL DML로 행을 업데이트 하는 것을 지원합니다.

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta WHERE userid = 1

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC userid 1 사용자의 콜레스테롤 레벨이 정상 수주치로 떨어졌다는 갱신된 정보를 받았습니다.

-- COMMAND ----------

UPDATE user_data_bronze_delta SET chosestlevs = 'Normal' WHERE userid = 1;

SELECT * FROM user_data_bronze_delta where userid = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UPSERT / MERGE
-- MAGIC
-- MAGIC Delta는 Merge 구문을 사용해서 간단한 upsert를 지원합니다. 34세 사용자의 초기 데이터를 확인해봅시다.

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta WHERE age = 34

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 현재 데이터셋에는 80세 사용자는 없습니다.

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta WHERE age = 80

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 원 데이터셋에 merge 연산을 수행할 신규 데이터를 생성해보시죠. User 2는 기존 정보 중 일부를 업데이트(10 파운드, 약 4.5kg 체중 감소), 신규 사용자(userid=39)는 80세로 기존에는 없는 데이터

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_upsert_values
USING DELTA
AS
SELECT 2 AS userid,  "M" AS gender, 34 AS age, 69 AS height, 140 AS weight, "N" AS smoker, "Y" AS familyhistory, "Normal" AS chosestlevs, "Normal" AS bp, -10 AS risk
UNION
SELECT 39 AS userid,  "M" AS gender, 80 AS age, 72 AS height, 155 AS weight, "N" AS smoker, "Y" AS familyhistory, "Normal" AS chosestlevs, "Normal" AS bp, 10 AS risk;

SELECT * FROM user_data_upsert_values;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `user_data_upsert_values`의 정보를 `user_data_bronze_delta` 테이블에 Merge 구문을 실행합니다.
-- MAGIC 실행 후 신규 사용자(userid=39)가 추가되었고, userid 2 사용자는 기존 150에서 140으로 체중이 감소했습니다.

-- COMMAND ----------

MERGE INTO user_data_bronze_delta as target
USING user_data_upsert_values as source
ON target.userid = source.userid
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *;
  
SELECT * FROM user_data_bronze_delta WHERE age = 34 OR age = 80;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 테이블 시간 여행 : History
-- MAGIC
-- MAGIC Delta는 테이블의 이전 커밋을 트랙킹합니다. history를 볼 수 있고, 이전 상태를 쿼리하거나 롤백하는 것을 지원 합니다.

-- COMMAND ----------

DESCRIBE HISTORY user_data_bronze_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 쿼리 수행 시 가장 최신의 버전이 기본

-- COMMAND ----------

SELECT *
FROM user_data_bronze_delta
WHERE age = 25

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `VERSION AS OF` 명령어를 사용해서 쿼리를 해보겠습니다. 이전에 삭제된 데이터를 볼 수 있습니다. `TIMESTAMP AS OF` 명령어도 있습니다 :
-- MAGIC ```
-- MAGIC SELECT * FROM table_identifier TIMESTAMP AS OF timestamp_expression
-- MAGIC SELECT * FROM table_identifier VERSION AS OF version
-- MAGIC ```

-- COMMAND ----------

SELECT * FROM user_data_bronze_delta
VERSION AS OF 2
WHERE age = 25

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### 이전 버전으로 Restore
-- MAGIC `RESTORE` 명령어를 사용해서 이전 상태로 Delta 테이블을 복구할 수 있습니다.
-- MAGIC
-- MAGIC ⚠️ Databricks Runtime 7.4 and above

-- COMMAND ----------

describe history user_data_bronze_delta

-- COMMAND ----------

RESTORE TABLE user_data_bronze_delta TO VERSION AS OF 8;

SELECT *
FROM user_data_bronze_delta
WHERE age = 25;

-- COMMAND ----------

RESTORE TABLE user_data_bronze_delta TO VERSION AS OF 1;

SELECT *
FROM user_data_bronze_delta
WHERE age = 25;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Streaming 시각화
-- MAGIC
-- MAGIC 브론즈 테이블에 저장
-- MAGIC 원시 데이터는 대량 업로드 혹은 스트리밍 소스를 통해서 데이터 레이크에 수집되는 변경되지 않은 데이터 입니다.
-- MAGIC 다음의 함수는 카프카 서버에 덤프된 위키피디아 IRC 채널을 읽습니다. 카프카 서버는 일종의 "소방 호스" 역할을 하며 원시 데이터를 데이터 레이크에 덤프합니다.
-- MAGIC
-- MAGIC 다음의 첫 단계는 스키마를 설정하는 것으로 추가 설명은 노트북 커멘트를 참고해주세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def untilStreamIsReady(name):
-- MAGIC   queries = list(filter(lambda query: query.name == name, spark.streams.active))
-- MAGIC
-- MAGIC   if len(queries) == 0:
-- MAGIC     print("The stream is not active.")
-- MAGIC
-- MAGIC   else:
-- MAGIC     while (queries[0].isActive and len(queries[0].recentProgress) == 0):
-- MAGIC       pass # wait until there is any type of progress
-- MAGIC
-- MAGIC     if queries[0].isActive:
-- MAGIC       queries[0].awaitTermination(5)
-- MAGIC       print("The stream is active and ready.")
-- MAGIC     else:
-- MAGIC       print("The stream is not active.")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
-- MAGIC from pyspark.sql.functions import from_json, col
-- MAGIC
-- MAGIC schema = StructType([
-- MAGIC   StructField("channel", StringType(), True),
-- MAGIC   StructField("comment", StringType(), True),
-- MAGIC   StructField("delta", IntegerType(), True),
-- MAGIC   StructField("flag", StringType(), True),
-- MAGIC   StructField("geocoding", StructType([                 # (OBJECT): Added by the server, field contains IP address geocoding information for anonymous edit.
-- MAGIC     StructField("city", StringType(), True),
-- MAGIC     StructField("country", StringType(), True),
-- MAGIC     StructField("countryCode2", StringType(), True),
-- MAGIC     StructField("countryCode3", StringType(), True),
-- MAGIC     StructField("stateProvince", StringType(), True),
-- MAGIC     StructField("latitude", DoubleType(), True),
-- MAGIC     StructField("longitude", DoubleType(), True),
-- MAGIC   ]), True),
-- MAGIC   StructField("isAnonymous", BooleanType(), True),      # (BOOLEAN): Whether or not the change was made by an anonymous user
-- MAGIC   StructField("isNewPage", BooleanType(), True),
-- MAGIC   StructField("isRobot", BooleanType(), True),
-- MAGIC   StructField("isUnpatrolled", BooleanType(), True),
-- MAGIC   StructField("namespace", StringType(), True),         # (STRING): Page's namespace. See https://en.wikipedia.org/wiki/Wikipedia:Namespace 
-- MAGIC   StructField("page", StringType(), True),              # (STRING): Printable name of the page that was edited
-- MAGIC   StructField("pageURL", StringType(), True),           # (STRING): URL of the page that was edited
-- MAGIC   StructField("timestamp", StringType(), True),         # (STRING): Time the edit occurred, in ISO-8601 format
-- MAGIC   StructField("url", StringType(), True),
-- MAGIC   StructField("user", StringType(), True),              # (STRING): User who made the edit or the IP address associated with the anonymous editor
-- MAGIC   StructField("userURL", StringType(), True),
-- MAGIC   StructField("wikipediaURL", StringType(), True),
-- MAGIC   StructField("wikipedia", StringType(), True),         # (STRING): Short name of the Wikipedia that was edited (e.g., "en" for the English)
-- MAGIC ])
-- MAGIC
-- MAGIC # start our stream
-- MAGIC (spark.readStream
-- MAGIC   .format("kafka")  
-- MAGIC   .option("kafka.bootstrap.servers", "server1.databricks.training:9092")  # Oregon
-- MAGIC   #.option("kafka.bootstrap.servers", "server2.databricks.training:9092") # Singapore
-- MAGIC   .option("subscribe", "en")
-- MAGIC   .load()
-- MAGIC   .withColumn("json", from_json(col("value").cast("string"), schema))
-- MAGIC   .select(col("timestamp").alias("kafka_timestamp"), col("json.*"))
-- MAGIC   .writeStream
-- MAGIC   .format("delta")
-- MAGIC   .option("checkpointLocation", '/tmp/delta_rapid_start/{}/checkpoint/'.format(str(databricks_user)))
-- MAGIC   .outputMode("append")
-- MAGIC   .queryName("stream_1p")
-- MAGIC   .toTable('wikiIRC')
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC untilStreamIsReady('stream_1p')

-- COMMAND ----------

SELECT * FROM wikiIRC  

-- COMMAND ----------

describe detail wikiIRC

-- COMMAND ----------

describe detail wikiIRC

-- COMMAND ----------

select count(*) from wikiIRC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Medallion 아키텍처

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Silver 테이블
-- MAGIC 실버존은 데이터가 필터링, 정리 및 보강되는 곳입니다.
-- MAGIC
-- MAGIC **User Data Silver**
-- MAGIC * 회사 표준에 맞게 컬럼명 변경
-- MAGIC
-- MAGIC **Device Data Silver**
-- MAGIC * 타임스탬프에 파생된 날짜 열 추가
-- MAGIC * 더 이상 필요하지 않은 컬럼 삭제
-- MAGIC * 보폭을 기록할 컬럼 생성(miles walked / steps)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_data_silver
USING DELTA
  SELECT 
    userid AS user_id,
    gender,
    age,
    height,
    weight,
    smoker,
    familyhistory AS family_history,
    chosestlevs AS cholest_levs,
    bp AS blood_pressure,
    risk
  FROM user_data_bronze_delta;
  
SELECT * FROM user_data_silver

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS device_data_silver
USING DELTA
  SELECT
    id,
    device_id,
    user_id,
    calories_burnt,
    miles_walked,
    num_steps,
    miles_walked/num_steps as stride,
    timestamp,
    DATE(timestamp) as date
  FROM device_data_bronze_delta;
  
SELECT * FROM device_data_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold 테이블
-- MAGIC 골드존은 분석 및 보고에 사용되는 비즈니스 수준의 집계 테이블로 실버 테이블을 함께 조인하고 집계를 수행하는 것과 같은 변환 과정을 통해 생성됩니다.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS user_daily_averages_gold
USING DELTA
  SELECT
      u.user_id,
      u.gender,
      u.age,
      u.blood_pressure,
      u.cholest_levs,
      AVG(calories_burnt) as avg_calories_burnt,
      AVG(num_steps) as avg_num_steps,
      AVG(miles_walked) as avg_miles_walked
    FROM user_data_silver u
    LEFT JOIN
      (SELECT 
        user_id,
        date,
        MAX(calories_burnt) as calories_burnt,
        MAX(num_steps) as num_steps,
        MAX(miles_walked) as miles_walked
      FROM device_data_silver
      GROUP BY user_id, date) as daily
    ON daily.user_id = u.user_id
    GROUP BY u.user_id,
      u.gender,
      u.age,
      u.blood_pressure,
      u.cholest_levs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Schema Enforcement & Evolution
-- MAGIC **Schema enforcement**, 스키마 유효성 검사라고도 하는 데이터 품질을 보장하기 위한 Delta Lake의 보호 장치입니다. Delta Lake는 쓰기 시 스키마 유효성 검사를 합니다. 즉 , 테이블에 대한 모든 신규 쓰기 시 대상 테이블의 스키마와 호환성이 확인됩니다. 스키마가 호환되지 않는 경우 Delta Lake는 트랜잭션을 완전히 취소하고(데이터가 기록되지 않음) 예외를 발생시켜 사용자에게 불일치에 대해서 알려줍니다.
-- MAGIC
-- MAGIC **Schema evolution** 시간이 지남에 따라 변화하는 데이터를 수용하기 위해 사용자가 테이블의 현재 스키마를 쉽게 변경 할 수 있는 기능입니다. 가장 일반적인 사례는 하나 이상의 새 컬럼을 포함하도록 스키마를 자동 조정하기 위해 추가 및 덮어쓰기 작업을 수행할 때 사용합니다.
-- MAGIC
-- MAGIC ### Schema Enforcement
-- MAGIC 테이블에 쓰기에 적합한지 판단 시 Delta Lake는 다음의 규칙을 따름니다. 쓸 데이터프레임이 :
-- MAGIC
-- MAGIC * 대상 테이블의 스키마에 없는 추가 열을 포함할 수 없습니다. 
-- MAGIC * 대상 테이블의 컬럼 데이터 유형과 다른 컬럼 데이터 유형이 있을 수 없습니다.
-- MAGIC * 대소문자만 다른 열 이름은 포함할 수 없습니다.

-- COMMAND ----------

-- DBTITLE 1,에러 발생 여부 확인
-- You can uncomment the next line to see the error (remove the -- at the beginning of the line)
 INSERT INTO TABLE user_data_bronze_delta VALUES ('this is a test')

-- COMMAND ----------

-- You can uncomment the next line to see the error (remove the -- at the beginning of the line)
INSERT INTO user_data_bronze_delta VALUES (39, 'M', 44, 65, 150, 'N','N','Normal','High',20, 'John Doe')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schema Evolution

-- COMMAND ----------

-- Let's create a new table
CREATE OR REPLACE TABLE user_data_bronze_delta_new
USING DELTA
SELECT * FROM user_data_bronze_delta

-- COMMAND ----------

-- Set this configuration to allow for schema evolution
SET spark.databricks.delta.schema.autoMerge.enabled = true

-- COMMAND ----------

select * from user_data_bronze_delta

-- COMMAND ----------

-- Create new data to append
ALTER TABLE user_data_bronze_delta_new ADD COLUMN (Name string);

UPDATE user_data_bronze_delta_new
SET Name = 'J. Doe',
  userid = userid + 5;

SELECT * FROM user_data_bronze_delta_new

-- COMMAND ----------

-- Name is now in user_data_bronze_delta as well
INSERT INTO user_data_bronze_delta
SELECT * FROM user_data_bronze_delta_new;

SELECT * FROM user_data_bronze_delta

-- COMMAND ----------

-- shows schema history as of previous version
SELECT * FROM user_data_bronze_delta VERSION AS OF 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Optimize & Z-Ordering
-- MAGIC
-- MAGIC 쿼리 속도를 향상시키기 위해 데이터브릭스 상의 Delta Lake는 클라우드 저장소에 저장된 데이터의 레이아웃을 최적화하는 기능을 제공합니다.
-- MAGIC `OPTIMIZE` 명령은 작은 파일을 큰 파일로 합치는 데 사용할 수 있습니다.
-- MAGIC
-- MAGIC Z-Ordering은 관련 정보를 동일한 파일 집합에 배치해서 읽어야 하는 데이터의 양을 줄여 쿼리 성능을 향상 시키는 기술입니다. 쿼리 조건에 자주 사용되고 해당 열에 높은 카디널리티(distinct 값이 많은)가 있는 경우 `ZORDER BY`를 사용합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optimize

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/delta_{}_db.db/device_data_bronze_delta/device_id=1/'.format(str(databricks_user))))

-- COMMAND ----------

SELECT * FROM device_data_bronze_delta
WHERE num_steps > 7000 AND calories_burnt > 500

-- COMMAND ----------

-- DBTITLE 1,Optimize 수행 전에 몇 개의 파일이 있는지 확인해 봅시다(numFiles). 140개의 파일
desc detail device_data_bronze_delta

-- COMMAND ----------

-- DBTITLE 1,630k 수준의 작은 파일들
-- MAGIC %fs
-- MAGIC ls /user/hive/warehouse/delta_mj2727_db.db/device_data_bronze_delta/device_id=1

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC ls /user/hive/warehouse/delta_mj2727_db.db/device_data_bronze_delta/_delta_log/

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC head /user/hive/warehouse/delta_mj2727_db.db/device_data_bronze_delta/_delta_log/00000000000000000000.json

-- COMMAND ----------

-- DBTITLE 1,Optimize Zorder 수행
OPTIMIZE device_data_bronze_delta 
ZORDER BY num_steps, calories_burnt, id

/*
{"numFilesAdded": 20, "numFilesRemoved": 140, "filesAdded": {"min": 3982018, "max": 4095614, "avg": 4024556.3, "totalFiles": 20, "totalSize": 80491126}, "filesRemoved": {"min": 435006, "max": 676898, "avg": 612210.9857142858, "totalFiles": 140, "totalSize": 85709538}, "partitionsOptimized": 20, "zOrderStats": {"strategyName": "minCubeSize(107374182400)", "inputCubeFiles": {"num": 0, "size": 0}, "inputOtherFiles": {"num": 140, "size": 85709538}, "inputNumCubes": 0, "mergedFiles": {"num": 140, "size": 85709538}, "numOutputCubes": 20, "mergedNumCubes": null}, "numBatches": 1, "totalConsideredFiles": 140, "totalFilesSkipped": 0, "preserveInsertionOrder": false}
*/

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC
-- MAGIC ls -lt /dbfs/user/hive/warehouse/delta_mj2727_db.db/device_data_bronze_delta/device_id=1/

-- COMMAND ----------

INSERT INTO device_data_bronze_delta select * from device_data_bronze_delta where device_id = 1

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC
-- MAGIC ls -lt /dbfs/user/hive/warehouse/delta_mj2727_db.db/device_data_bronze_delta/device_id=1/

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC
-- MAGIC ls -lt /dbfs/user/hive/warehouse/delta_mj2727_db.db/device_data_bronze_delta/_delta_log/

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC head /user/hive/warehouse/delta_mj2727_db.db/device_data_bronze_delta/_delta_log/00000000000000000001.json

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC
-- MAGIC ls -lt /dbfs/user/hive/warehouse/delta_mj2727_db.db/device_data_bronze_delta/_delta_log/

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC head /user/hive/warehouse/delta_mj2727_db.db/device_data_bronze_delta/_delta_log/00000000000000000002.json

-- COMMAND ----------

OPTIMIZE device_data_bronze_delta 
ZORDER BY num_steps, calories_burnt, id

/*
{"numFilesAdded": 20, "numFilesRemoved": 140, "filesAdded": {"min": 3982018, "max": 4095614, "avg": 4024556.3, "totalFiles": 20, "totalSize": 80491126}, "filesRemoved": {"min": 435006, "max": 676898, "avg": 612210.9857142858, "totalFiles": 140, "totalSize": 85709538}, "partitionsOptimized": 20, "zOrderStats": {"strategyName": "minCubeSize(107374182400)", "inputCubeFiles": {"num": 0, "size": 0}, "inputOtherFiles": {"num": 140, "size": 85709538}, "inputNumCubes": 0, "mergedFiles": {"num": 140, "size": 85709538}, "numOutputCubes": 20, "mergedNumCubes": null}, "numBatches": 1, "totalConsideredFiles": 140, "totalFilesSkipped": 0, "preserveInsertionOrder": false}
*/

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC
-- MAGIC ls -lt /dbfs/user/hive/warehouse/delta_mj2727_db.db/device_data_bronze_delta/device_id=1/

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC
-- MAGIC ls -lt /dbfs/user/hive/warehouse/delta_mj2727_db.db/device_data_bronze_delta/_delta_log/

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC
-- MAGIC head /dbfs/user/hive/warehouse/delta_mj2727_db.db/device_data_bronze_delta/_delta_log/00000000000000000003.crc

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that the metrics show that we removed more files than we added 

-- COMMAND ----------

SELECT * FROM device_data_bronze_delta where num_steps > 7000 AND calories_burnt > 500

-- COMMAND ----------

OPTIMIZE device_data_bronze_delta 
ZORDER BY user_id, id

/*
{"numFilesAdded": 20, "numFilesRemoved": 140, "filesAdded": {"min": 3982018, "max": 4095614, "avg": 4024556.3, "totalFiles": 20, "totalSize": 80491126}, "filesRemoved": {"min": 435006, "max": 676898, "avg": 612210.9857142858, "totalFiles": 140, "totalSize": 85709538}, "partitionsOptimized": 20, "zOrderStats": {"strategyName": "minCubeSize(107374182400)", "inputCubeFiles": {"num": 0, "size": 0}, "inputOtherFiles": {"num": 140, "size": 85709538}, "inputNumCubes": 0, "mergedFiles": {"num": 140, "size": 85709538}, "numOutputCubes": 20, "mergedNumCubes": null}, "numBatches": 1, "totalConsideredFiles": 140, "totalFilesSkipped": 0, "preserveInsertionOrder": false}
*/

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC
-- MAGIC ls -lt /dbfs/user/hive/warehouse/delta_mj2727_db.db/device_data_bronze_delta/device_id=1/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Auto-Optimize
-- MAGIC
-- MAGIC Auto Optimize는 Delta table에 개별 쓰기가 발생하는 동안 작은 파일을 자동으로 압축하는 선택적 기능입니다. 쓰기 작업 중 약간의 비용을 들이면 자주 쿼리되는 테이블의 경우 상단한 효과를 볼 수 있습니다.
-- MAGIC
-- MAGIC 자동 최적화는 최적화된 쓰기 및 자동 압축이라는 두 가지 보완적 기능으로 구성되어 있습니다. Auto-optimize에 대한 자세한 내용은 여기에서 확인할 수 있습니다 [here](https://docs.microsoft.com/en-us/azure/databricks/delta/optimizations/auto-optimize).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 기존 테이블 업데이트

-- COMMAND ----------

ALTER TABLE device_data_bronze_delta
SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL Configuration
-- MAGIC
-- MAGIC 모든 신규 Delta table에 이 기능을 활성화 하려면 SQL 환경설정을 세팅합니다.

-- COMMAND ----------

SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

-- COMMAND ----------

GET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite

-- COMMAND ----------

GET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # CLONE
-- MAGIC
-- MAGIC Clone 명령을 사용해서 특정 버전의 Delta Table의 복사본을 생성할 수 있습니다. Clone에는 2가지 유형이 있습니다 :
-- MAGIC
-- MAGIC * **deep clone**은 기존 테이블의 메타데이터 외에 소스 테이블의 데이터를 클론 대상에 복사하는 Clone입니다.
-- MAGIC * **shallow clone**은 데이터 파일을 클론 대상에 복사하지 않는 클론입니다. 테이블의 메타 데이터는 소스와 동일합니다. 생성 비용을 절약할 수 있습니다.
-- MAGIC
-- MAGIC Deep 혹은 Shallow clone에 대한 모든 변경사항은 오직 클론 자체에만 영향을 주며 원본 테이블에는 영향을 주지 않습니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Clone 활용 사례
-- MAGIC
-- MAGIC * Data archiving : time travel 보다 장기 보유 혹은 DR 목적으로 deep clone 활용
-- MAGIC * Machine learning flow reproduction : 특정 버전의 테이블을 아카이빙 후 ML모델 훈련에 활용
-- MAGIC * Short-term experiments on a production table : 기존 테이블을 훼손하지 않으면서 운영계 테이블에 대한 워크 플로우 테스트(Shallow clone)
-- MAGIC * Data sharing : 동일 기관 내 다른 사업부에서 동일한 데이터에 대한 접근이 필요하나 최신의 데이터는 아니어도 될 경우 활용. 별개의 권한을 부여
-- MAGIC * Table property overrides : 기존 소스 테이블과 개별의 로그 유지 기간을 적용(더 장기간의 보관 주기 설정)
-- MAGIC
-- MAGIC [here](https://docs.microsoft.com/en-us/azure/databricks/delta/delta-utility#in-this-section).

-- COMMAND ----------

CREATE TABLE user_data_bronze_delta_clone
SHALLOW CLONE user_data_bronze_delta
VERSION AS OF 1;

SELECT * FROM user_data_bronze_delta_clone;

-- COMMAND ----------

CREATE TABLE user_data_bronze_delta_clone_deep
DEEP CLONE user_data_bronze_delta;

SELECT * FROM user_data_bronze_delta_clone_deep

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 작업 환경 정리
-- MAGIC
-- MAGIC 다음의 코드를 수행하여 이 과정에서 생성한 데이터베이스 및 테이블을 삭제합니다.
-- MAGIC
-- MAGIC Please uncomment the last cell to clean things up. You can remove the `#` and run the cell again

-- COMMAND ----------

-- DBTITLE 1,Auto Optimize 기본값으로 재설정(필수)
-- auto optimize를 원 상태로 돌려놓아야 함
SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = false;
SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = false;

-- COMMAND ----------

-- DBTITLE 1,스트리밍 중단 및 데이터 정리(필수)
-- MAGIC %python
-- MAGIC for s in spark.streams.active:
-- MAGIC    s.stop()
-- MAGIC spark.sql("DROP DATABASE IF EXISTS delta_{}_db CASCADE".format(str(databricks_user)))
-- MAGIC dbutils.fs.rm('/tmp/delta_rapid_start/{}/'.format(str(databricks_user)), True)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
