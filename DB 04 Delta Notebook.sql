-- Databricks notebook source
-- MAGIC %md ### Delta Lake Reading Databricks Datasets Already Provided

-- COMMAND ----------

-- MAGIC %md [Delta Lake Tutorial](https://docs.databricks.com/en/delta/tutorial.html#language-sql): 
-- MAGIC This tutorial introduces common Delta Lake operations on Databricks.
-- MAGIC Delta Lake is the optimized storage layer that provides the foundation for storing data and tables in the Databricks Lakehouse Platform. 
-- MAGIC Delta Lake is open source software that extends Parquet data files with a file-based transaction log for ACID transactions and scalable metadata handling.

-- COMMAND ----------

-- DBTITLE 1,Listing the Flights dataset
-- MAGIC %python
-- MAGIC dbutils.fs.ls("/databricks-datasets/flights/")

-- COMMAND ----------

-- DBTITLE 1,Creating a table called delayflights 
drop table if exists delayflights;
create table if not exists delayflights
as select * 
from read_files("/databricks-datasets/flights/departuredelays.csv")

-- COMMAND ----------

-- DBTITLE 1,All Tables created in Databricks will be Delta
DESCRIBE DETAIL delayflights;

-- COMMAND ----------

-- MAGIC %md ### Browse Databricks Datasets - https://docs.databricks.com/en/discover/databricks-datasets.html#databricks-datasets-databricks-datasets 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/databricks-datasets/learning-spark-v2/people/")

-- COMMAND ----------

-- MAGIC %md ### Lets read in a Delta table and save it in a new table called people_10m. 
-- MAGIC Notice the .delta below. 

-- COMMAND ----------

-- DBTITLE 1,Lets Read DELTA tables now.
DROP TABLE IF EXISTS people_10m;

CREATE TABLE IF NOT EXISTS people_10m
AS SELECT * FROM delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta`;

-- COMMAND ----------

DESCRIBE DETAIL people_10m;

-- COMMAND ----------

-- MAGIC %md ### Go to Catalog Now
-- MAGIC dbfs:/user/hive/warehouse/people_10m - the location is in the Delta Wearhouse.
-- MAGIC
-- MAGIC But also notice we created a new table too in Default
-- MAGIC

-- COMMAND ----------

select count(*) from people_10m

-- COMMAND ----------

-- MAGIC %md ## Upsert to a Table
-- MAGIC To merge a set of updates and insertions into an existing Delta table, you use the [MERGE INTO](https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html) statement

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW people_updates (
  id, firstName, middleName, lastName, gender, birthDate, ssn, salary
) AS VALUES
  (9999998, 'Hulk', 'Bruce', 'Banner', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452', 55250),
  (9999999, 'Batman', 'Bruce', 'Wayne', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500),
  (10000000, 'Thor', 'Chris', 'Hemsworth', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000),
  (20000001, 'Spiderman', '', 'Parker', 'M', '1978-01-14T04:00:00.000+000', '345-67-8901', 55500),
  (20000002, 'Captain', '', 'America', 'M', '1982-10-29T01:00:00.000+000', '456-78-9012', 98250),
  (20000003, 'Wonder', '', 'Woman', 'F', '1981-06-25T04:00:00.000+000', '567-89-0123', 89900);

MERGE INTO people_10m
USING people_updates
ON people_10m.id = people_updates.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- DBTITLE 1,We have a Date Error - Lets See the Data types in the Table
DESCRIBE people_10m;

-- COMMAND ----------

-- DBTITLE 1,Just see the columns
SHOW COLUMNS FROM people_10m;

-- COMMAND ----------

-- DBTITLE 1,Cast the Birthdate String as timestamp
CREATE OR REPLACE TEMP VIEW people_updates (
  id, firstName, middleName, lastName, gender, birthDate, ssn, salary
) AS VALUES
  (9999998, 'Hulk', 'Bruce', 'Banner', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452', 55250),
  (9999999, 'Batman', 'Bruce', 'Wayne', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500),
  (10000000, 'Thor', 'Chris', 'Hemsworth', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000),
  (20000001, 'Spiderman', '', 'Parker', 'M', '1978-01-14T04:00:00.000+000', '345-67-8901', 55500),
  (20000002, 'Captain', '', 'America', 'M', '1982-10-29T01:00:00.000+000', '456-78-9012', 98250),
  (20000003, 'Wonder', '', 'Woman', 'F', '1981-06-25T04:00:00.000+000', '567-89-0123', 89900);

MERGE INTO people_10m
USING (
  SELECT id, firstName, middleName, lastName, gender, ssn, salary, try_cast(birthDate AS timestamp) AS birthDate
  FROM people_updates
) AS updates
ON people_10m.id = updates.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md ## Read a table

-- COMMAND ----------

select * from people_10m limit 10;

-- COMMAND ----------

select * from delta.`dbfs:/user/hive/warehouse/people_10m` limit 10;

-- COMMAND ----------

-- MAGIC %md ## Update a table

-- COMMAND ----------

-- DBTITLE 1,Change Gender to Superhero & Eye Candy
UPDATE people_10m SET gender = 'Superhero' WHERE gender = 'Female';
UPDATE people_10m SET gender = 'Eye Candy' WHERE gender = 'Male';

-- COMMAND ----------

select * from people_10m limit 50;

-- COMMAND ----------

-- DBTITLE 1,View Eye Candy
select * from people_10m where gender = 'Eye Candy' limit 20;

-- COMMAND ----------

-- DBTITLE 1,View Superhero
select * from people_10m where gender = 'Superhero' limit 20;

-- COMMAND ----------

-- MAGIC %md ## Delete from a table

-- COMMAND ----------

-- DBTITLE 1,Show our new inserts
SELECT * FROM people_10m WHERE firstName IN ('Spiderman', 'Batman', 'Wonder', 'Thor', 'Hulk');

-- COMMAND ----------

-- DBTITLE 1,View Spiderman
SELECT * FROM people_10m WHERE firstName ='Spiderman'

-- COMMAND ----------

-- DBTITLE 1,Delete Spiderman
delete from people_10m where firstName = 'Spiderman'

-- COMMAND ----------

SELECT * FROM people_10m WHERE firstName ='Spiderman'

-- COMMAND ----------

-- MAGIC %md ## Display table history

-- COMMAND ----------

describe history people_10m

-- COMMAND ----------

-- MAGIC %md # ACID (time travel) With VERSION
-- MAGIC ## Query an earlier version of the table (time travel)

-- COMMAND ----------

-- DBTITLE 1,VERSION - As in the History - You can Select a Version
SELECT * FROM people_10m VERSION AS OF 0 limit 10

-- COMMAND ----------

-- MAGIC %md # Optimise a table
-- MAGIC ### Once you have performed multiple changes to a table, you might have a lot of small files. To improve the speed of read queries, you can use OPTIMISE to collapse small files into larger ones:

-- COMMAND ----------

optimize people_10m

-- COMMAND ----------

-- MAGIC  %md ## Z-ordering By Columns
-- MAGIC #### [Useful read](https://docs.databricks.com/en/delta/optimize.html#how-often-should-i-run-optimize)
-- MAGIC #### Z-Ordering is a technique used in Delta Lake to co-locate related information in the same set of files, which is automatically used by Delta Lake in data-skipping algorithms. 
-- MAGIC This behavior dramatically reduces the amount of data that Delta Lake needs to read, resulting in faster queries and improved query performance.
-- MAGIC By reorganising the data in storage, certain queries can read less data, so they run faster.
-- MAGIC

-- COMMAND ----------

OPTIMIZE people_10m
ZORDER BY (gender)
