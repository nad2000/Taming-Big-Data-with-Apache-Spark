from pyspark import SparkConf, SparkContext
from os import path
import re

normalize_re = re.compile(r'\W+', re.UNICODE)

DATA_ROOT = "file:///home/nad2000/Dropbox/taming-big-data-with-apache-spark-hands-on"

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalize_words(text):
    return normalize_re.split(text.lower())

input = sc.textFile(path.join(DATA_ROOT, "book.txt"))
# flatMap maps a collection value into a multiple entries each per an element
words = input.flatMap(normalize_words)
word_counts = words.map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y)
# swap and sort:
word_counts_sorted = word_counts.map(lambda (x, y): (y, x)).sortByKey()

for rec in word_counts_sorted.collect():
    count = rec[0]
    cleanWord = rec[1].encode('ascii', 'ignore')
    if (cleanWord):
        print cleanWord, count
