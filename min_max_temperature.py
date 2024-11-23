from pyspark import SparkConf, SparkContext
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conf = SparkConf().setMaster("local").setAppName("MinTemperature")
sc = SparkContext(conf=conf)

sc.setLogLevel(logLevel="ERROR")


def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)


lines = sc.textFile("data/1800.csv")

# Handle min temperatures
min_temps = lines.map(parseLine).filter(lambda x: "TMIN" in x[1])
station_temps = min_temps.map(lambda x: (x[0], x[2]))
min_temps = station_temps.reduceByKey(lambda x, y: min(x, y))

logger.info(f"Min temperatures: {min_temps.collect()}")

# Handle max temperatures
max_temps = lines.map(parseLine).filter(lambda x: "TMAX" in x[1])
station_temps = max_temps.map(lambda x: (x[0], x[2]))
max_temps = station_temps.reduceByKey(lambda x, y: max(x, y))
logger.info(f"Max temperatures: {max_temps.collect()}")