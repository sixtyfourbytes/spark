from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL DataFrame").getOrCreate()

people = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("./DataFiles/fakefriends-header.csv")
)

people.printSchema()

people.select('name', people.age).show()

people.filter(people.age > 21).show()

people.groupBy('age').count().sort('age').show()

people.select(people.name, people.age + 10).show()

spark.stop()
