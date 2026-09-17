from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Most Obscure SuperHero").getOrCreate()

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ]
)

names = spark.read.schema(schema).option("sep", " ").csv("./DataFiles/MarvelNames")

connections = spark.read.text("./DataFiles/MarvelGraph")

connectionsDF = (
    connections.withColumn("id", funcs.split(funcs.col("value"), " ")[0])
    .withColumn("connections", funcs.size(funcs.split(funcs.col("value"), " ")) - 1)
    .groupBy("id")
    .agg(funcs.sum("connections").alias("connections"))
)

obscureSuperHero = connectionsDF.agg(
    funcs.min("connections").alias("connections")
).first()

minConnections = obscureSuperHero["connections"] if obscureSuperHero else None

obscureSuperHeroDF = connectionsDF.filter(connectionsDF.connections == minConnections)

obscureSuperHeroDF.join(names, "id", "left").show()
