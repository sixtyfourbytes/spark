from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs

spark = SparkSession.builder.appName("Word Count using DataFrame").getOrCreate()

lines = spark.read.text("./DataFiles/book.txt")

wordsDF = lines.select(funcs.explode(funcs.split(lines.value, "\\W+")).alias("word"))

wordsDF = wordsDF.filter(wordsDF.word != "")

wordsLowerDF = wordsDF.select(funcs.lower(wordsDF.word).alias("word"))

wordCountDF = wordsLowerDF.groupBy("word").count().sort("count")

wordCountDF.show(wordCountDF.count())

spark.stop()
