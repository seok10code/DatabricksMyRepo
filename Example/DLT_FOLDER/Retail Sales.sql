-- Databricks notebook source
-- CREATE DATABASE mj2727;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 1. Bronze Table - 고객 정보, 주문량

-- COMMAND ----------

CREATE STREAMING LIVE TABLE customers
COMMENT "The customers buying finished products, ingested from /databricks-datasets."
TBLPROPERTIES ("myCompanyPipeline.quality" = "mapping")
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv");

-- COMMAND ----------

CREATE STREAMING LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from /databricks-datasets."
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
AS
SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 2. Silver Table - 고객 및 주문 테이블 Join 테이블 (고객 ID 별 주문 정보)

-- COMMAND ----------

CREATE STREAMING LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW -- expect_drop
)
PARTITIONED BY (order_date)
COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS
SELECT f.customer_id, f.customer_name, f.number_of_line_items, 
  TIMESTAMP(from_unixtime((cast(f.order_datetime as long)))) as order_datetime, 
  DATE(from_unixtime((cast(f.order_datetime as long)))) as order_date, 
  f.order_number, f.ordered_products, c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
  FROM STREAM(LIVE.sales_orders_raw) f
  LEFT JOIN LIVE.customers c
      ON c.customer_id = f.customer_id
     AND c.customer_name = f.customer_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 3. Gold Table - LA 지역 내 판매 정보 테이블

-- COMMAND ----------

CREATE LIVE TABLE sales_order_in_la
-- Schema
(city STRING,
  order_date date,
  customer_id STRING,
  customer_name STRING,
  curr STRING,
  sales bigint,
  quantity bigint,
  product_count bigint)
COMMENT "Sales orders in LA."
TBLPROPERTIES ("myCompanyPipeline.quality" = "gold")
AS
SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, SUM(ordered_products_explode.price) as sales, SUM(ordered_products_explode.qty) as quantity, COUNT(ordered_products_explode.id) as product_count
FROM (
  SELECT city, order_date, customer_id, customer_name, EXPLODE(ordered_products) as ordered_products_explode
  FROM LIVE.sales_orders_cleaned
  WHERE city = 'Los Angeles'
  )
GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 4. Gold Table - Chicago 지역 내 판매 정보 테이블

-- COMMAND ----------

CREATE LIVE TABLE sales_order_in_chicago
-- Schema
(city STRING,
  order_date date,
  customer_id STRING,
  customer_name STRING,
  curr STRING,
  sales bigint,
  quantity bigint,
  product_count bigint)
COMMENT "Sales orders in Chicago."
TBLPROPERTIES ("myCompanyPipeline.quality" = "gold")
AS
SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, SUM(ordered_products_explode.price) as sales, SUM(ordered_products_explode.qty) as quantity, COUNT(ordered_products_explode.id) as product_count
FROM (
  SELECT city, order_date, customer_id, customer_name, EXPLODE(ordered_products) as ordered_products_explode
  FROM LIVE.sales_orders_cleaned 
  WHERE city = 'Chicago'
  )
GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------


