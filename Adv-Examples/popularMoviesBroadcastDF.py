from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs
from pyspark.sql.types import StructType, StructField, IntegerType, LongType


def loadMovieNames():
    movieNames = {}
    with open(
        "./DataFiles/ml-100k/u.item", "r", encoding="ISO-8859-1", errors="ignore"
    ) as file:
        for line in file:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.appName(
    "popular movies example using broadcast to exec nodes"
).getOrCreate()

# broadcast to all the exec nodes. all executor nodes has access to the dict. appropriate or only limited data
broadcastDict = spark.sparkContext.broadcast(loadMovieNames())

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

movieCountsDf = moviesDF.groupBy("movieId").count()


def getMovieName(movieId):
    return broadcastDict.value[movieId]


getMovieNameUDF = funcs.udf(getMovieName)

popularMoviesDF = movieCountsDf.withColumn(
    "title", getMovieNameUDF(funcs.col("movieId"))
)

popularMoviesDF.orderBy(funcs.desc("count")).show()
