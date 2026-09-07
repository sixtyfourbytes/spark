from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs

spark = SparkSession.builder.appName('Friends By Age Ex Using DataFrame').getOrCreate()

people = spark.read.option('header', 'true').option('inferSchema', 'true').csv('./DataFiles/fakefriends-header.csv')

friendsByAge = people.select('age', 'friends')

friendsByAge.groupBy('age').avg('friends').sort('age').show()

friendsByAge.groupBy('age').agg(funcs.round(funcs.avg('friends'), 2).alias('friendsAvg')).sort('age').show()

people.show(20)

spark.stop()
