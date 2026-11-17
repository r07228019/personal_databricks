import pytest

from giscovery_crawler.schedule import eventbridge_to_quartz


@pytest.mark.parametrize(
    "expression, expected",
    [
        ("0 16 1 * ? *", "0 0 16 1 * ? *"),
        ("0 16 5 3 ? *", "0 0 16 5 3 ? *"),
        ("  30 8 ? * MON *  ", "0 30 8 ? * MON *"),
    ],
)
def test_eventbridge_to_quartz(expression, expected):
    assert eventbridge_to_quartz(expression) == expected


@pytest.mark.parametrize("bad", ["cron(0 16 1 * ? *)", "rate(1 day)", "0 16 1 * ?", "0 16 1 * ? * *"])
def test_rejects_wrapper_and_wrong_field_count(bad):
    with pytest.raises(ValueError):
        eventbridge_to_quartz(bad)
