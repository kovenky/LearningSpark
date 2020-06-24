"""
Author: VENKY VARMA
Date Created: 06-20-2020
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import ( StructField,
                                StringType, IntegerType, StructType
                                )

spark = SparkSession.builder.appName("Basics").getOrCreate()

# read json file now into a Data Frame
df = spark.read.json("./people.json")
print("People Count: ", df.count())
# df.show()

# print Schema
df.printSchema()

# get columns
print("DF Cols: ", df.columns)

# Describe Data Frame
df.describe().show()

# Create Schema to explicitly specify while reading data into DataFrame
"""
if any of the row value doesn't match with schema specified, 
it would make the entire row as null,null,null ... and so on
"""
data_schema = [StructField("name", StringType(),True),
               StructField("gender",StringType(),True),
               StructField("age", IntegerType(),True)]
final_schema = StructType(fields=data_schema)
df = spark.read.json("./people.json",schema=final_schema)
# print Schema
df.printSchema()
#print df
df.show()

"""
selecting cols from DF (table like data structure)
"""
# Select only "age" column
df.select('age').show()

#selecting multiple cols
df.select(['name','age']).show()

# Creating new Column for DataFrame, this operation is in-place only,
# meaning original DF will not be changed, if you want to take modified data - you gotta assign it to new df-VARIABLE

df.withColumn("double_age", df['age']).show()

# Renaming Columns
df.withColumnRenamed("name","Person Name").show()

# register DF as a Temp Table View in memory, so we can run SQL queries
df.createOrReplaceTempView("people")
results = spark.sql("SELECT * FROM people")
print("--- results from people table --- ")
results.show()
