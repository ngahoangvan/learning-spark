import logging

from pyspark import SparkContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_line(line):
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

# Handle min temperatures
def find_min_temperature(spark_context: SparkContext, file_path: str):
    lines = spark_context.textFile(file_path)
    min_temps = lines.map(parse_line).filter(lambda x: "TMIN" in x[1])
    station_temps = min_temps.map(lambda x: (x[0], x[2]))
    min_temps = station_temps.reduceByKey(lambda x, y: min(x, y))
    # logger.info(f"Min temperatures: {min_temps.collect()}")
    return min_temps.collect()


# Handle max temperatures
def find_max_temperature(spark_context: SparkContext, file_path: str):
    lines = spark_context.textFile(file_path)
    max_temps = lines.map(parse_line).filter(lambda x: "TMAX" in x[1])
    station_temps = max_temps.map(lambda x: (x[0], x[2]))
    max_temps = station_temps.reduceByKey(lambda x, y: max(x, y))
    # logger.info(f"Max temperatures: {max_temps.collect()}")
    return max_temps.collect()
