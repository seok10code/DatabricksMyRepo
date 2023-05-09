-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Implement CDC In DLT Pipeline: Change Data Capture
-- MAGIC
-- MAGIC -----------------
-- MAGIC ###### By Morgan Mazouchi
-- MAGIC -----------------
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/delta-live-tables-notebooks/main/change-data-capture-example/images/dlt_end_to_end_flow.png">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Importance of Change Data Capture (CDC)
-- MAGIC
-- MAGIC 변경 데이터 캡처(CDC)는 데이터베이스, 데이터 웨어하우스 등과 같은 데이터 저장소에 대한 레코드의 변경 사항을 캡처하는 프로세스입니다. 이러한 변경 사항은 일반적으로 데이터 삭제, 추가 및 업데이트와 같은 작업을 나타냅니다.
-- MAGIC
-- MAGIC 데이터 복제의 간단한 방법은 데이터베이스를 내보내고 LakeHouse/DataWarehouse/Lake로 가져올 데이터베이스 덤프를 가져오는 것이지만 이것은 확장 가능한 접근 방식이 아닙니다.
-- MAGIC
-- MAGIC 변경 데이터 캡처는 데이터베이스에 대한 변경 사항만 캡처하고 해당 변경 사항을 대상 데이터베이스에 적용합니다. CDC는 오버헤드를 줄이고 실시간 분석을 지원합니다. 대량 로드 업데이트가 필요하지 않으면서 증분 로드가 가능합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### CDC Approaches 
-- MAGIC
-- MAGIC **1- 사내 CDC 프로세스 개발:**
-- MAGIC
-- MAGIC ***복잡한 작업:*** CDC 데이터 복제는 일회성 쉬운 솔루션이 아닙니다. 데이터베이스 공급자 간의 차이, 다양한 레코드 형식 및 로그 레코드 액세스의 불편함으로 인해 CDC는 어려움을 겪고 있습니다.
-- MAGIC
-- MAGIC ***정기 유지 관리:*** CDC 프로세스 스크립트를 작성하는 것은 첫 번째 단계일 뿐입니다. 앞서 언급한 변경 사항에 정기적으로 매핑할 수 있는 맞춤형 솔루션을 유지 관리해야 합니다. 이것은 많은 시간과 자원이 필요합니다.
-- MAGIC
-- MAGIC ***과도한 부담:*** 회사의 개발자는 이미 공개 쿼리의 부담에 직면해 있습니다. 맞춤형 CDC 솔루션 구축을 위한 추가 작업은 기존 수익 창출 프로젝트에 영향을 미칩니다.
-- MAGIC
-- MAGIC **2- Debezium, Hevo Data, IBM Infosphere, Qlik Replicate, Talend, Oracle GoldenGate, StreamSets와 같은 CDC 도구 사용**.
-- MAGIC
-- MAGIC 이 데모 리포지토리에서 우리는 CDC 도구에서 오는 CDC 데이터를 사용하고 있습니다.
-- MAGIC CDC 도구가 데이터베이스 로그를 읽고 있기 때문에:
-- MAGIC 더 이상 개발자가 특정 열을 업데이트하는 데 의존하지 않습니다.
-- MAGIC
-- MAGIC — Debezium과 같은 CDC 도구는 변경된 모든 행을 캡처합니다. 애플리케이션이 사용하는 Kafka 로그의 데이터 변경 내역을 기록합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Setup/Requirements:
-- MAGIC
-- MAGIC 이 노트북을 파이프라인으로 실행하기 전에 DLT 파이프라인에 1-CDC_DataGenerator 노트북에 대한 경로를 포함하여 이 노트북이 생성된 CDC 데이터 위에서 실행되도록 해야 합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SQL 데이터베이스를 Lakehouse와 동기화하는 방법은 무엇입니까?
-- MAGIC CDC 도구, 오토로더 및 DLT 파이프라인이 포함된 CDC 흐름:
-- MAGIC
-- MAGIC - CDC 도구는 데이터베이스 로그를 읽고, 변경 사항이 포함된 json 메시지를 생성하고, 변경 사항 설명이 포함된 레코드를 Kafka에 스트리밍합니다.
-- MAGIC - Kafka는 INSERT, UPDATE 및 DELETE 작업을 포함하는 메시지를 스트리밍하고 이를 클라우드 객체 스토리지(S3 폴더, ADLS 등)에 저장합니다.
-- MAGIC - Autoloader를 사용하여 클라우드 객체 스토리지에서 메시지를 점진적으로 로드하고 원시 메시지를 저장할 때 Bronze 테이블에 저장합니다.
-- MAGIC - 다음으로 정리된 Bronze 레이어 테이블에서 APPLY CHANGES INTO를 수행하여 가장 업데이트된 데이터 다운스트림을 Silver 테이블로 전파할 수 있습니다.
-- MAGIC
-- MAGIC 다음은 외부 데이터베이스에서 CDC 데이터를 사용하여 구현할 흐름입니다. 수신은 Kafka와 같은 메시지 대기열을 포함하여 모든 형식일 수 있습니다.
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/delta-live-tables-notebooks/main/change-data-capture-example/images/cdc_flow_new.png" alt='Make all your data ready for BI and ML'/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###Debezium 출력과 같은 CDC 도구는 어떻게 생겼나요?
-- MAGIC
-- MAGIC 변경된 데이터를 설명하는 json 메시지에는 아래 목록과 유사한 흥미로운 필드가 있습니다.
-- MAGIC
-- MAGIC - 연산: 연산 코드(DELETE, APPEND, UPDATE, CREATE)
-- MAGIC - operation_date: 각 작업 작업에 대한 레코드의 날짜 및 타임스탬프
-- MAGIC
-- MAGIC Debezium 출력에서 ​​볼 수 있는 다른 필드(이 데모에는 포함되지 않음):
-- MAGIC - before: 변경 전 행
-- MAGIC - after: 변경 후 행
-- MAGIC
-- MAGIC To learn more about the expected fields check out [this reference](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-update-events)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Incremental data loading using Auto Loader (cloud_files)
-- MAGIC <div style="float:right">
-- MAGIC   <img width="700px" src="https://raw.githubusercontent.com/databricks/delta-live-tables-notebooks/main/change-data-capture-example/images/DLT_CDC.png"/>
-- MAGIC </div>
-- MAGIC
-- MAGIC 스키마 업데이트로 인해 외부 시스템과 작업하는 것이 어려울 수 있습니다. 외부 데이터베이스는 스키마 업데이트, 열 추가 또는 수정을 가질 수 있으며 시스템은 이러한 변경 사항에 대해 견고해야 합니다.
-- MAGIC Databricks Autoloader(`cloudFiles`)는 즉시 스키마 추론 및 진화를 처리합니다.
-- MAGIC
-- MAGIC 오토로더를 사용하면 클라우드 스토리지에서 수백만 개의 파일을 효율적으로 수집하고 대규모로 효율적인 스키마 추론 및 진화를 지원할 수 있습니다. 이 노트북에서는 Autoloader를 활용하여 스트리밍(및 배치) 데이터를 처리합니다.
-- MAGIC
-- MAGIC 이를 사용하여 파이프라인을 만들고 외부 공급자가 제공하는 원시 JSON 데이터를 수집해 보겠습니다.

-- COMMAND ----------

-- DBTITLE 1,Let's explore our incoming data - Bronze Table - Autoloader & DLT
SET
  spark.source;
CREATE
  OR REFRESH STREAMING LIVE TABLE customer_bronze (
    address string,
    email string,
    id string,
    firstname string,
    lastname string,
    operation string,
    operation_date string,
    _rescued_data string
  ) TBLPROPERTIES ("quality" = "bronze") COMMENT "New customer data incrementally ingested from cloud object storage landing zone" AS
SELECT
  *
FROM
  cloud_files(
    "${source}/customers",
    "json",
    map("cloudFiles.inferColumnTypes", "true")
  );

-- COMMAND ----------

-- DBTITLE 1,Silver Layer - Cleansed Table (Impose Constraints)
CREATE OR REFRESH STREAMING LIVE TABLE customer_bronze_clean_v(
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_address EXPECT (address IS NOT NULL),
  CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze customer view (i.e. what will become Silver)"
AS SELECT * 
FROM STREAM(LIVE.customer_bronze);

-- COMMAND ----------

-- DBTITLE 1,Delete unwanted clients records - Silver Table - DLT SQL 
CREATE OR REFRESH STREAMING LIVE TABLE customer_silver
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged customers";

-- COMMAND ----------

APPLY CHANGES INTO LIVE.customer_silver
FROM stream(LIVE.customer_bronze_clean_v)
  KEYS (id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY operation_date --auto-incremental ID to identity order of events
  COLUMNS * EXCEPT (operation, operation_date, _rescued_data);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Next step, create DLT pipeline, add a path to this notebook and **add configuration with enabling applychanges to true**. For more detail see notebook "PipelineSettingConfiguration.json". 
-- MAGIC
-- MAGIC After running the pipeline, check "3. Retail_DLT_CDC_Monitoring" to monitor log events and lineage data.

-- COMMAND ----------


