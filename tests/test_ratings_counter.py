from typing import OrderedDict
import pytest

from scripts.ratings_counter import count_rating_histogram


def test_count_rating_histogram_should_succeed(spark_context_fixture):
    histogram = count_rating_histogram(
        spark_context=spark_context_fixture,
        file_path="tests/test_data/test_fake_ratings.csv"
    )
    assert histogram == OrderedDict({
        '1.0': 3,
        '2.0': 2,
        '3.0': 2,
        '4.0': 3,
        '5.0': 9
    })