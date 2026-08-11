from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('FriendsByAge')
sc = SparkContext(conf = conf)

def parse_line(line):
    fields = line.split(',')

    age = int(fields[2])
    num_friends = int(fields[3])

    return age, (num_friends, 1)

lines = sc.textFile('/home/red/Projects/SparkCourse/fakefriends.csv')
rdd = lines.map(parse_line)
totals_by_age = rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_by_age = totals_by_age.mapValues(lambda x: round(x[0] / x[1], 2))

print(average_by_age.collect())