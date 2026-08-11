import re

from pyspark import SparkConf, SparkContext

def normalizeWord(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster('local').setAppName('WordCount')
sc = SparkContext(conf = conf)

lines = sc.textFile('./DataFiles/book.txt')
words = lines.flatMap(normalizeWord)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(cleanWord, count)