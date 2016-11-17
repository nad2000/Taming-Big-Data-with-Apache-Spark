from pyspark import SparkConf, SparkContext
from os import path

DATA_ROOT = "file:///home/nad2000/Dropbox/taming-big-data-with-apache-spark-hands-on"

conf = SparkConf().setMaster("local").setAppName("CustomerSummary")
sc = SparkContext(conf = conf)

def extract_data(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

data = sc.textFile(path.join(DATA_ROOT, "customer-orders.csv"))
result = (data.map(lambda l: extract_data(l))
        .reduceByKey(lambda x, y: x + y)
        .map(lambda (a, b): (b, a))
        .sortByKey())

for (s, c) in result.collect():
    print "%d\t%.2f" % (c, s)
