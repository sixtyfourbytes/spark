from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
)

spark = SparkSession.builder.appName("Minimum Temperatures").getOrCreate()

schema = StructType(
    [
        StructField("station_id", StringType(), True),
        StructField("date", IntegerType(), True),
        StructField("measure_type", StringType(), True),
        StructField("temperature", FloatType(), True),
    ]
)

df = spark.read.schema(schema).csv("./DataFiles/1800.csv")

minTempDf = df.filter(df.measure_type == "TMIN")

minTempByStation = (
    minTempDf.select("station_id", "temperature")
    .groupBy("station_id")
    .min("temperature")
)

minTempByStation = minTempByStation.withColumn(
    "temperature",
    funcs.round(funcs.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2),
).sort("temperature")

minTempByStation.show()
