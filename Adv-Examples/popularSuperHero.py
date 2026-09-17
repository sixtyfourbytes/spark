from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as funcs
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Get the Most Popular SuperHero").getOrCreate()

schema = StructType(
    [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
)

names = spark.read.option("sep", " ").schema(schema).csv("./DataFiles/MarvelNames")

lines = spark.read.text("./DataFiles/MarvelGraph")

connectionsDF = (
    lines.withColumn("id", funcs.split(funcs.col("value"), " ")[0])
    .withColumn("connections", funcs.size(funcs.split(funcs.col("value"), " ")) - 1)
    .groupby("id")
    .agg(funcs.sum("connections").alias("connections"))
)

mostPopular: Row | None = connectionsDF.sort(funcs.col("connections").desc()).first()

if mostPopular:
    names.filter(funcs.col("id") == mostPopular[0]).show()
