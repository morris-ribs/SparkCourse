from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 # to Farenheit if you want: * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
# filter() removes data from the RDD
# Here, we want to filter out entries that don't have "TMAX" in the first item of a list of data
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
# create (stationID, temperature) key/value pairs
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
# find minimum temperature by stationID
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = maxTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}C".format(result[1]))
