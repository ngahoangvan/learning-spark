import pytest
from pyspark.sql import DataFrame

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions as func

@pytest.fixture
def spark_context_fixture():
    conf = SparkConf().setMaster("local").setAppName("Test")
    sc = SparkContext(conf=conf)
    yield sc
    sc.stop()


@pytest.fixture
def spark_session_fixtrue():
    session: SparkSession = SparkSession.builder.appName("TestSession").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def mock_fake_friend(spark_session_fixtrue: SparkSession) -> DataFrame:
    data = [
        (1, "John", 20, 3),
        (2, "Jane", 25, 4),
        (3, "Doe", 30, 5),
    ]
    columns = ["id", "name", "age", "friends"]
    return spark_session_fixtrue.createDataFrame(data, columns)