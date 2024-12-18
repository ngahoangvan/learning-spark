import pytest
from scripts.min_max_temperature import find_min_temperature, find_max_temperature, parse_line


def test_parse_line_should_succeed():
    line = "ITE00100554,18000101,TMAX,-75,,,E,"
    station_id, entry_type, temperature = parse_line(line)
    assert station_id == "ITE00100554"
    assert entry_type == "TMAX"
    assert temperature == 18.5


def test_parse_line_should_failed():
    line = "ITE00100554,18000101,TMAX,-75a,,,E,"
    with pytest.raises(Exception) as e:
        parse_line(line)

    assert str(e.value) == "could not convert string to float: '-75a'"


def test_find_min_temperature_should_succeed(spark_context_fixture):
    min_temps = find_min_temperature(
        spark_context=spark_context_fixture,
        file_path="tests/test_data/test_temperature.csv"
    )
    assert min_temps == [("ITE00100554", 9.5), ("EZE00100082", 18.68)]


def test_find_max_temperature_should_succeed(spark_context_fixture):
    max_temps = find_max_temperature(
        spark_context=spark_context_fixture,
        file_path="tests/test_data/test_temperature.csv"
    )
    assert max_temps == [("ITE00100554", 34.34), ("EZE00100082", 30.2)]
