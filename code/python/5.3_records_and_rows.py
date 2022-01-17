from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("pyspark.sql.shuffle.partitions","5")


from pyspark.sql import Row
myRow = Row("Hello",None,1,False)

myRow[0]
myRow[2]
