from pyspark import SparkConf, SparkContext
from os import path

DATA_ROOT = "file:///home/nad2000/Dropbox/taming-big-data-with-apache-spark-hands-on"

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1  # C
    return (stationID, entryType, temperature)

lines = sc.textFile(path.join(DATA_ROOT, "1800.csv"))
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}C".format(result[1]))
