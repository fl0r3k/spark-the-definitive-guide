from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("pyspark.sql.shuffle.partitions","5")


# How to run Spark SQL Queries

## Spark's Programmatic SQL Interface
spark.sql("SELECT 1+1").show()

spark.sql("""
  SELECT user_id, department, first_name
  FROM proffesors
  WHERE department IN (
    SELECT name from DEPARTMENT WHERE create_date >= '2016-01-01'
  )
""").show()

spark.read.json("data/flight-data/json/2015-summary.json")\
  .createOrReplaceTempView("some_sql_view")

spark.sql("""
    SELECT dest_country_name, sum(count)
    FROM some_sql_view
    GROUP BY dest_country_name
  """)\
  .where("dest_country_name like 'S%'")\
  .where("`sum(count)` > 10")\
  .count()

## SparkSQL Thrift JDBC/ODBC Server


# Tables
## Creating Tables

# spark.sql("DROP TABLE flights")
spark.sql("""
  CREATE TABLE flights(
    DEST_COUNTRY_NAME   STRING,
    ORIGIN_COUNTRY_NAME STRING,
    count               LONG
  )
  USING JSON OPTIONS(path 'data/flight-data/json/2015-summary.json')
""")
spark.sql("SELECT * FROM flights").show()

spark.sql("DROP TABLE flights_csv")
spark.sql("""
  CREATE TABLE flights_csv(
    DEST_COUNTRY_NAME   STRING,
    ORIGIN_COUNTRY_NAME STRING COMMENT "remember, the US will be most prevelent",
    count               LONG
  )
  USING JSON OPTIONS(header true, path 'data/flight-data/csv/2015-summary.csv')
""")
spark.sql("SELECT * FROM flights_csv").show()

spark.sql("""
  CREATE TABLE flights_from_select USING parquet AS
    SELECT * FROM flights
""")

spark.sql("""
  CREATE TABLE partitioned_flights
    USING parquet
    PARITIONED BY (DEST_COUNTRY_NAME)
  AS
    SELECT
      DEST_COUNTRY_NAME,
      ORIGIN_COUNTRY_NAME,
      count
    FROM flights
    LIMIT 5
""")

## Creating External Tables
spark.sql("""
  CREATE EXTERNAL TABLE hive_flights(
    DEST_COUNTRY_NAME   STRING,
    ORIGIN_COUNTRY_NAME STRING,
    count               LONG
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  LOCATION 'data/flight-data-hive/'
""")
# spark.sql("SELECT * FROM hive_flights").show(5)

spark.sql("""
  CREATE EXTERNAL TABLE hive_flights_2
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  LOCATION 'data/flight-data-hive/' AS
    SELECT * FROM flights
""")

## Inserting into Tables
spark.sql("""
  INSERT INTO fligths_from_select
    SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count
    FROM flights
    LIMIT 20
""")

spark.sql("""
  INSERT INTO partitioned_fligths
  PARTITION(DEST_COUNTRY_NAME="UNITED_STATES")
    SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count
    FROM flights
    WHERE DEST_COUNTRY_NAME = 'UNITED STATES'
    LIMIT 12
""")

## Describing Table Metadata
spark.sql("""
  DESCRIBE TABLE flights_csv
""")

spark.sql("""
  SHOW PARTITIONS patitioned_flights
""")


## Refresh Table Metadata
spark.sql("""
  REFRESH TABLE partitioned_flights
""")

spark.sql("""
  MSCK REPAIR TABLE partitioned_flights
""")

## Dropping Tables

spark.sql("""
  DROP TABLE flights_csv
""")


spark.sql("""
  DROP TABLE IF EXISTS flights_csv
""")

### Caching Tables
spark.sql("""
  CACHE TABLE flights
""")


# Views
## Creating Views
spark.sql("""
  CREATE VIEW just_usa_view AS
    SELECT *
    FROM flights
    WHERE dest_country_name = 'United States'
""")

spark.sql("""
  CREATE TEMP VIEW just_usa_view_temp AS
    SELECT *
    FROM flights
    WHERE dest_country_name = 'United States'
""")


spark.sql("""
  CREATE GLOBAL TEMP VIEW just_usa_global_view_temp AS
    SELECT *
    FROM flights
    WHERE dest_country_name = 'United States'
""")


spark.sql("""
  CREATE OR REPLACE TEMP VIEW just_usa_view_temp AS
    SELECT *
    FROM flights
    WHERE dest_country_name = 'United States'
""")

spark.sql("""
  EXPLAIN SELECT * FROM flights WHERE dest_country_name = 'United States'
""")

## Dropping Views
spark.sql("""
  DROP VIEW IF EXISTS just_usa_view
""")


# Databases
spark.sql("""
  SHOW DATABASES
""").show()

## Creating Databases
spark.sql("""
  CREATE DATABASE some_db
""")

## Setting the Database
spark.sql("""
  USE some_db
""")

## Dropping Databases
spark.sql("""
  DROP DATABASE IF EXISTS some_db
""")

# Advanced Topics
## Complex Types
### Structs
spark.sql("""
  CREATE VIEW IF NOT EXISTS nested_data AS
    SELECT (DEST_COUNTRY_NAME,ORIGIN_COUNTRY_NAME) AS country
    FROM flights
""")

spark.sql("""
  SELECT country.DEST_COUNTRY_NAME FROM nested_data
""")

### Lists
spark.sql("""
  SELECT
    DEST_COUNTRY_NAME AS new_name,
    collect_list(count) AS flight_counts,
    collect_set(count) AS origin_set
  FROM flights
  GROUP BY DEST_COUNTRY_NAME
""").show()

spark.sql("""
  SELECT DEST_COUNTRY_NAME, ARRAY(1,2,3) FROM flights
""").show()

spark.sql("""
  SELECT
    DEST_COUNTRY_NAME AS new_name,
    collect_list(count)[0]
  FROM flights
  GROUP BY DEST_COUNTRY_NAME
""").show()


spark.sql("""
  CREATE OR REPLACE TEMP VIEW flights_agg AS
    SELECT
      DEST_COUNTRY_NAME,
      collect_list(count) AS collected_counts
    FROM flights
    GROUP BY DEST_COUNTRY_NAME
""")

spark.sql("""
  SELECT
    explode(collected_counts),
    DEST_COUNTRY_NAME
  FROM flights_agg
""").show()


## Functions
spark.sql("""
  SHOW FUNCTIONS
""").show()

spark.sql("""
  SHOW SYSTEM FUNCTIONS
""").show()

spark.sql("""
  SHOW USER FUNCTIONS
""").show()


spark.sql("""
  SHOW FUNCTIONS "s*"
""").show()


spark.sql("""
  SHOW FUNCTIONS LIKE "collect*"
""").show()


spark.sql("""
  DESCRIBE FUNCTION colelct_list
""").show()

## Subqueries
### Uncorrelated predicate subqueries
spark.sql("""
  SELECT dest_country_name
  FROM flights
  GROUP BY dest_country_name
  ORDER BY sum(count)
  DESC LIMIT 5
""").show()

spark.sql("""
  SELECT dest_country_name
  FROM flights
  WHERE origin_country_name IN (
    SELECT dest_country_name
    FROM flights
    GROUP BY dest_country_name
    ORDER BY sum(count)
    DESC LIMIT 5
  )
""").show()

### Correlated predicate subqueries
spark.sql("""
  SELECT *
  FROM flights f1
  WHERE 1=1
    AND EXISTS (
      SELECT 1
      FROM flights f2
      WHERE f1.dest_country_name = f2.origin_country_name
    )
    AND EXISTS (
      SELECT 1
      FROM flights f2
      WHERE f2.dest_country_name = f1.origin_country_name
    )
""").show()


### Uncorrelated scalar queries
spark.sql("""
  SELECT
    *,
    ( SELECT max(count) FROM flights ) AS maximum
  FROM flights
""").show()

# Miscellaneous Features
## Configurations
### spark.sql.inMemoryColumnarStorage.compressed = true
### spark.sql.inMemoryColumnarStorage.batchSize = 10000
### spark.sql.files.maxPartitionBytes = 134217728 (128 MB)
### spark.sql.files.openCostInBytes = 4194304 (4 MB)
### spark.sql.broadcastTimeout = 300
### spark.sql.autoBroadcastJoinTreshold = 10485760 (10 MB)
### spark.sql.shhuffle.partitions = 200

## Setting Configuration Values in SQL
spark.sql("""
  SET spark.sql.shuffle.partitions=20
""")

