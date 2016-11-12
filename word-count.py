from pyspark import SparkConf, SparkContext
from os import path

DATA_ROOT = "file:///home/nad2000/Dropbox/taming-big-data-with-apache-spark-hands-on"

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile(path.join(DATA_ROOT, "book.txt"))
# flatMap maps a collection value into a multiple entries each per an element
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
