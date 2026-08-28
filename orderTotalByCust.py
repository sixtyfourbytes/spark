from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('OrderTotalByCustomer')
sc = SparkContext(conf=conf)

lines = sc.textFile('./DataFiles/customer-orders.csv')
custSpent = lines.map(lambda x: (x.split(',')[0], float(x.split(',')[2])))
custSpent = (custSpent.reduceByKey(lambda x, y: x + y)
             .map(lambda x: (round(x[1], 2), x[0]))
             .sortByKey())

print(custSpent.collect())