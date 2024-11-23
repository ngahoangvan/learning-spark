from pyspark import SparkConf, SparkContext
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conf = SparkConf().setMaster("local").setAppName("CountCustomerOrders")
sc = SparkContext(conf=conf)

sc.setLogLevel(logLevel="ERROR")

def parseLine(line):
    fields = line.split(",")
    customer_id = int(fields[0])
    order_amount = float(fields[2])
    return (customer_id, order_amount)

lines = sc.textFile("data/customer-orders.csv")
rdd = lines.map(parseLine).reduceByKey(lambda x, y: x + y)
logger.info(f"Customer orders: {rdd.collect()}")