from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("pyspark.sql.shuffle.partitions","5")

person = spark.createDataFrame([
    (0,"Bill Chambers",0,[100]),
    (1,"Matei Zaharia",1,[500,250,100]),
    (2,"Michael Armbrust",1,[250,100])])\
  .toDF("id","name","graduate_program","spark_status")

graduateProgram = spark.createDataFrame([
    (0,"Masters","School of Information","UC Berkeley"),
    (2,"Masters","EECS","UC Berkley"),
    (1,"Ph.D.","EECS","UC Berkley")])\
  .toDF("id","degree","department","school")

sparkStatus = spark.createDataFrame([
    (500,"Vice President"),
    (250,"PMC Member"),
    (100,"Contributor")])\
  .toDF("id","status")

person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

# Inner Joins
joinExpression = person["graduate_program"] == graduateProgram["id"]

wrongJoinExpression = person["name"] == graduateProgram["school"]

person.join(graduateProgram,joinExpression).show()

joinType = "inner"
person.join(graduateProgram,joinExpression,joinType).show()

# Outer Joins
joinType = "outer"
person.join(graduateProgram,joinExpression,joinType).show()

# Left Outer Joins
joinType = "left_outer"
graduateProgram.join(person,joinExpression,joinType).show()

# Right Outer Joins
joinType = "right_outer"
person.join(graduateProgram,joinExpression,joinType).show()

# Left Semi Joins
joinType = "left_semi"
graduateProgram.join(person,joinExpression,joinType).show()

# Right Anti Joins
joinType = "left_anti"
graduateProgram.join(person,joinExpression,joinType).show()

# Natural Joins
## select * from graduateProgram natural join person

# Cross (Cartesian) Joins
spark.conf.set("spark.sql.crossJoin.enabled","true")
joinType = "cross"
graduateProgram.join(person,joinExpression,joinType).show()

person.crossJoin(graduateProgram).show()

# Challanges When Using Joins
## Joins on Complex Types
from pyspark.sql.functions import expr
person.withColumnRenamed("id","personId")\
  .join(sparkStatus,expr("array_contains(spark_status,id)"))\
  .show()

## Handling Duplacate Columns Names
gradProgramDupe = graduateProgram.withColumnRenamed("id","graduate_program")
joinExpr = gradProgramDupe["graduate_program"] == person["graduate_program"]
person.join(gradProgramDupe,joinExpr)\
  .select("graduate_program").show()

### Approach 1: Different join expression
person.join(gradProgramDupe,"graduate_program")\
  .select("graduate_program").show()

### Approach 2: Droppijng the column after the join
person.join(gradProgramDupe,joinExpr)\
  .drop(person["graduate_program"]).select("graduate_program").show()

### Approach 3: Renaming a column before join
gradProgram3 = graduateProgram.withColumnRenamed("id","grad_id")
joinExpr = person["graduate_program"] == gradProgram3["grad_id"]
person.join(gradProgram3,joinExpr)\
  .show()


# How Spark Performs Joins
## Big table-to-small table
joinExpr = person["graduate_program"] == graduateProgram["id"]
person.join(graduateProgram,joinExpr).explain()

from pyspark.sql.functions import broadcast
joinExpr = person["graduate_program"] == graduateProgram["id"]
person.join(broadcast(graduateProgram),joinExpr).explain()