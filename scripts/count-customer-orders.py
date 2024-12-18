import logging

from pyspark import SparkContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_line(line):
    fields = line.split(",")
    customer_id = int(fields[0])
    order_amount = float(fields[2])
    return (customer_id, order_amount)


def count_customer_order(spark_context: SparkContext, file_path: str):
    lines = spark_context.textFile(file_path)
    rdd = lines.map(parse_line).reduceByKey(lambda x, y: x + y).sortByKey()
    return rdd.collect()
