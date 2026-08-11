from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('FriendsByAge')
sc = SparkContext(conf = conf)

def parseLines(line):
    fields = line.split(',')

    age = int(fields[2])
    numFriends = int(fields[3])

    return age, (numFriends, 1)

lines = sc.textFile('./DataFiles/fakefriends.csv')
rdd = lines.map(parseLines)
totalsByAge = rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averageByAge = totalsByAge.mapValues(lambda x: round(x[0] / x[1], 2))

print(averageByAge.collect())