from pyspark import SparkConf, SparkContext
import collections
from os import path

DATA_ROOT = "/home/nad2000/Dropbox/taming-big-data-with-apache-spark-hands-on/ml-100k"

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file://" + path.join(DATA_ROOT, "u.data"))
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
