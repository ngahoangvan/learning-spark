import collections

from pyspark import SparkContext


def count_rating_histogram(spark_context: SparkContext, file_path: str):
    lines = spark_context.textFile(file_path)
    ratings = lines.map(lambda x: x.split(",")[2])
    result = ratings.countByValue()
    return collections.OrderedDict(sorted(result.items()))
