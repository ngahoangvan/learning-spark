from pyspark import SparkContext, SparkConf
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

# Set Spark's log level (ERROR, WARN, INFO, DEBUG)
sc.setLogLevel("ERROR")

def parseLine(line):
    fields = line.split(",")
    try:
        age = int(fields[2])
        numFriends = int(fields[3])
        logger.debug(f"Successfully parsed line: age={age}, friends={numFriends}")
        return (age, numFriends)
    except Exception as e:
        logger.error(f"Error parsing line: {line}")
        logger.error(f"Exception: {str(e)}")
        raise

lines = sc.textFile("data/fake-social-nw/fake_friends.csv")
logger.info("Loading data from CSV file")

rdd = lines.map(parseLine)
logger.info("Mapped data to (age, friends) pairs")

totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
logger.info("Computed totals by age")

averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

logger.info(f"Average number of friends by age: {averagesByAge.collect()}")
