from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("Popular Movies using DataFrames").getOrCreate()

schema = StructType(
    [
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

moviesDF = (
    spark.read.option("sep", "\t").schema(schema).csv("./DataFiles/ml-100k/u.data")
)

moviesDF.show()

moviesDF.groupBy("movieId").count().orderBy(funcs.desc("count")).show()
