from pyspark import SparkConf, SparkContext
from configuration import data_url, data_filename

def loadMovieNames():
    movieNames = {}
    with open(data_filename("ml-100k/u.item")) as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

## stores in one single intance on the remote executer (broadcast variable)
nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile(data_url("ml-100k/u.data"))
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda x : (x[1], x[0]))
sortedMovies = flipped.sortByKey()

## joins  stored data:
sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print result
