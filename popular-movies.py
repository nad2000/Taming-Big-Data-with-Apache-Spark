from pyspark import SparkConf, SparkContext
from os import path

DATA_ROOT = "file:///home/nad2000/Dropbox/taming-big-data-with-apache-spark-hands-on"


conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

lines = sc.textFile(path.join(DATA_ROOT, "ml-100k/u.data"))
movies = lines.map(lambda x: (int(x.split(None, 3)[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda xy: (xy[1],xy[0]) )
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for (c, m) in results:
    print(m, c)
