from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("pyspark.sql.shuffle.partitions","5")

df = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .load("data/retail-data/by-day/2010-12-01.csv")

df.printSchema()

df.createGlobalTempView("dfTable")


# Converting to Spark Types
from pyspark.sql.functions import lit
df.select(lit(5),lit("five"),lit(5.0))


# Working with Booleans
from pyspark.sql.functions import col
df.where(col("InvoiceNo") != 536365)\
  .select("InvoiceNo","Description")\
  .show(5,False)

df.where("InvoiceNo != 536365")\
  .show(5,False)

df.where("InvoiceNo <> 536365")\
  .show(5,False)


from pyspark.sql.functions import instr
priceFilter = col("UnitPrice") > 600
descriptionFilter = instr(df.Description,"POSTAGE") >= 1
df.where(df.StockCode.isin("DOT"))\
  .where(priceFilter | descriptionFilter)\
  .show()


from pyspark.sql.functions import instr
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descriptionFilter = instr(df.Description,"POSTAGE") >= 1
df.withColumn("isExpensive",DOTCodeFilter & (priceFilter | descriptionFilter))\
  .where("isExpensive")\
  .select("unitPrice","isExpensive")\
  .show(5)


from pyspark.sql.functions import expr
df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))\
  .where("isExpensive")\
  .select("Description","UnitPrice")\
  .show(5)


df.where(col("Description").eqNullSafe("hello")).show()


# Working with Numbers
from pyspark.sql.functions import expr, pow
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"),2) + 5
df.select(expr("CustomerId"),fabricatedQuantity.alias("realQuantity"))\
  .show(2)

df.selectExpr(
  "CustomerId",
  "power(Quantity*UnitPrice,2) + 5")\
  .show(2)


from pyspark.sql.functions import lit, round, bround
df.select(
    round(lit("2.5")),
    bround(lit("2.5")))\
  .show(2)


from pyspark.sql.functions import corr
df.stat.corr("Quantity","UnitPrice")
df.select(corr("Quantity","UnitPrice")).show()


df.describe().show()


from pyspark.sql.functions import count, mean, stddev_pop, min, max


colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
df.stat.approxQuantile("UnitPrice",quantileProbs,relError)


df.stat.freqItems(["StockCode","Quantity"]).show()


from pyspark.sql.functions import monotonically_increasing_id
df.select(monotonically_increasing_id()).show(2)


# Working with Strings
from pyspark.sql.functions import initcap
df.select(initcap(col("Description"))).show()


from pyspark.sql.functions import lower, upper
df.select(
    col("Description"),
    lower(col("Description")),
    upper(col("Description")))\
  .show(2)


from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
    lit("        HELLO      ").alias("no function"),
    ltrim(lit("        HELLO      ")).alias("ltrim"),
    rtrim(lit("        HELLO      ")).alias("rtrim"),
    trim(lit("        HELLO      ")).alias("trim"),
    lpad(lit("HELLO"),3," ").alias("lpad"),
    rpad(lit("HELLO"),10," ").alias("rpad"))\
  .show(2)


## Regular Expressions
from pyspark.sql.functions import regexp_replace
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
  regexp_replace(col("Description"),regex_string,"COLOR").alias("color_clean"),
  col("Description")).show()


from pyspark.sql.functions import translate
df.select(translate(col("Description"),"LEET","1337"),col("Description")).show(2)


from pyspark.sql.functions import regexp_extract
extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
  regexp_extract(col("Description"),extract_str,1).alias("color_clean"),
  col("Description")).show(2)


from pyspark.sql.functions import instr
containsBlack = instr(col("Description"),"BLACK") >= 1
containsWhite = instr(col("Description"),"WHITE") >= 1
df.withColumn("hasSimpleColor",containsBlack|containsWhite)\
  .where("hasSimpleColor")\
  .select("Description")\
  .show(3,False)


from pyspark.sql.functions import expr, locate
simpleColors = ["black","white","red","green","blue"]
def color_locator(column,color_string):
  return locate(color_string.upper(),column)\
    .cast("boolean")\
    .alias("is_" + color_string)
selectedColumns = [color_locator(df.Description,c) for c in simpleColors]
selectedColumns
selectedColumns.append(expr("*"))
df.select(*selectedColumns).where(expr("is_white or is_red"))\
  .select("Description").show(3,False)


# Working with Dates and Timestamp
df.printSchema()


from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
  .withColumn("today",current_date())\
  .withColumn("now",current_timestamp())
dateDF.createOrReplaceTempView("dateTable")
dateDF.printSchema()


from pyspark.sql.functions import date_add, date_sub
dateDF.select(
    date_sub(col("today"),5),
    date_add(col("today"),5))\
  .show(1)


from pyspark.sql.functions import datediff, months_between, to_date
dateDF.withColumn("week_ago",date_sub(col("today"),7))\
  .select(datediff(col("week_ago"),col("today")))\
  .show(1)
dateDF.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))\
  .select(months_between(col("start"),col("end")))\
  .show(1)


from pyspark.sql.functions import to_date, lit
spark.range(5).withColumn("date",lit("2017-01-01"))\
  .select(to_date(col("date")))\
  .show(1)

dateDF.select(
    to_date(lit("2016-20-12")),
    to_date(lit("2016-12-11")))\
  .show(1)

from pyspark.sql.functions import to_date
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
  to_date(lit("2017-12-11"),dateFormat).alias("date"),
  to_date(lit("2017-20-12"),dateFormat).alias("date2"))

cleanDateDF.createOrReplaceTempView("dateTable2")
cleanDateDF.show(1)


from pyspark.sql.functions import to_timestamp
cleanDateDF.select(
    to_timestamp(col("date"),dateFormat))\
  .show(1)


cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
cleanDateDF.filter(col("date2") > "'2017-12-12'").show()


# Working with Nulls in Data

## coalesce
from pyspark.sql.functions import coalesce
df.select(coalesce(
    col("Description"),
    col("CustomerId")
  ))\
  .show()

## ifnull, nullif, nvl and nvl2
df.selectExpr(
    "ifnull(null,'return_value') as a",
    "nullif('value','value') as b",
    "nvl(null,'return_value') as c",
    "nvl2('not_null','return_value','else_value') as d")\
  .show(1)
    
## drop
df.na.drop()
df.na.drop("any")
df.na.drop("all")
df.na.drop("all",subset=["StockCode","InvoiceNo"])

## fill
df.na.fill("All Null values become this string")
df.na.fill(5,subset=["StockCode","InvoiceNo"])

fill_cols_vals = {"StockCode":5,"Description":"No Value"}
df.na.fill(fill_cols_vals)

## replace
df.na.replace([""],["UNKNOWN"],"Description")

# Ordering
# asc_null_first, desc_null_first, asc_null_last, desc_null_last


# Working with Complex Types

## Structs
from pyspark.sql.functions import struct
complexDF = df.select(struct("Description","InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")

complexDF.select("complex.Description")
complexDF.select(col("complex").getField("Description"))
complexDF.select("complex.*")

## Arrays
### split
from pyspark.sql.functions import split
df.select(split(col("Description")," ")).show(2)
df.select(split(col("Description")," ").alias("array_col"))\
  .selectExpr("array_col[0]")\
  .show(2)

### Array Length
from pyspark.sql.functions import size
df.select(size(split(col("Description")," "))).show(2)

### array_contains
from pyspark.sql.functions import array_contains
df.select(array_contains(split(col("Description")," "),"WHITE")).show(2)

### explode
from pyspark.sql.functions import explode
df.withColumn("splitted",split(col("Description")," "))\
  .withColumn("exploded",explode(col("splitted")))\
  .select("Description","InvoiceNo","exploded")\
  .show(2)


## Maps
from pyspark.sql.functions import create_map
df.select(create_map(col("Description"),col("InvoiceNo")).alias("complex_map"))\
  .show(2)

df.select(create_map(col("Description"),col("InvoiceNo")).alias("complex_map"))\
  .selectExpr("complex_map['WHITE METAL LANTERN']")\
  .show(2)


df.select(create_map(col("Description"),col("InvoiceNo")).alias("complex_map"))\
  .selectExpr("explode(complex_map)")\
  .show(2)


# Working with JSON
jsonDF = spark.range(1).selectExpr("""
    '{"myJSONKey": {"myJSONValue" : [1,2,3]}}' as jsonString
  """)

from pyspark.sql.functions import get_json_object, json_tuple
jsonDF.select(
    get_json_object(col("jsonString"),"$.myJSONKey.myJSONValue[1]").alias("column"),
    json_tuple(col("jsonString"),"myJSONKey"))\
  .show(2)

from pyspark.sql.functions import to_json
df.selectExpr("(InvoiceNo,Description) as myStruct")\
  .select(to_json(col("myStruct")))

from pyspark.sql.functions import from_json
from pyspark.sql.types import *
parseSchema = StructType((
  StructField("InvoiceNo",StringType(),True),
  StructField("Description",StringType(),True)))
df.selectExpr("(InvoiceNo,Description) as myStruct")\
  .select(to_json(col("myStruct")).alias("newJSON"))\
  .select(from_json(col("newJSON"),parseSchema),col("newJSON"))\
  .show(2)


# User-Defined Functions
udfExampleDF = spark.range(5).toDF("num")
def power3(double_value):
  return double_value**3
power3(2.0)

from pyspark.sql.functions import udf
power3udf = udf(power3)

from pyspark.sql.functions import col
udfExampleDF.select(power3udf(col("num"))).show(2)


spark.udf.register("power3",power3)
udfExampleDF.selectExpr("power3(num)").show(2)

from pyspark.sql.types import IntegerType, DoubleType
spark.udf.register("power3py",power3,DoubleType())
udfExampleDF.selectExpr("power3py(num)").show(2)