import logging

from pyspark import SparkContext

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_line(line):
    fields = line.split(",")
    try:
        age = int(fields[2])
        numFriends = int(fields[3])
        return (age, numFriends)
    except Exception as e:
        logger.error(f"Error parsing line: {line}")
        logger.error(f"Exception: {str(e)}")
        raise e


def find_average(spark_context: SparkContext, file_path: str):
    lines = spark_context.textFile(file_path)
    header = lines.first()
    data = lines.filter(lambda x: x != header)
    rdd = data.map(parse_line)
    totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    first_averages = totals_by_age.mapValues(lambda x: x[0] / x[1]).first()
    return first_averages


# if __name__ == "__main__":
#     conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
#     sc = SparkContext(conf=conf)
#     first_averages = find_average(
#         spark_context=sc,
#         file_path="data/fake-social-nw/fake_friends.csv"
#     )
#     logger.info(f"Average number of friends by age: {first_averages}")
