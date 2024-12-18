import logging

import pytest

from scripts.friend_by_age import find_average, parse_line

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_parse_line_should_succeed():
    line = "0,Will,33,385"
    age, num_friends = parse_line(line)
    logger.warning(f"Age: {age}, Num Friends: {num_friends}")
    assert age == 33
    assert num_friends == 385


def test_parse_line_should_failed():
    line = "0,Will,33,385a"
    with pytest.raises(Exception) as e:
        parse_line(line)

    assert str(e.value) == "invalid literal for int() with base 10: '385a'"


def test_find_average_should_succeed(spark_context_fixture):
    average = find_average(
        spark_context=spark_context_fixture,
        file_path="tests/test_data/test_fake_friends.csv"
    )
    assert average == (33, 243.0)

def test_find_average_should_failed(spark_context_fixture):
    with pytest.raises(Exception) as e:
        find_average(
            spark_context=spark_context_fixture,
            file_path="tests/test_data/test_fake_friends_error.csv"
        )
        logger.error(f"Exception: {e}")