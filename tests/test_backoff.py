import pytest

from pulsar_dispatcher import default_backoff, exponential_backoff


def test_default_backoff_doubles_up_to_five_minutes() -> None:
    delays = [default_backoff(retry) for retry in range(1, 12)]

    assert delays == [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300]


def test_custom_backoff() -> None:
    backoff = exponential_backoff(initial=5, multiplier=3, maximum=100)

    assert [backoff(retry) for retry in range(1, 6)] == [5, 15, 45, 100, 100]


@pytest.mark.parametrize(
    ("initial", "multiplier", "expected"),
    [(0, 2, 0), (7, 1, 7), (1, 2, 300)],
)
def test_huge_retry_counts_are_cheap(
    initial: int, multiplier: int, expected: int
) -> None:
    backoff = exponential_backoff(initial=initial, multiplier=multiplier)

    assert backoff(10**12) == expected


@pytest.mark.parametrize("retry", [0, -3])
def test_retry_numbers_below_one_use_initial_delay(retry: int) -> None:
    assert default_backoff(retry) == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"initial": -1},
        {"multiplier": 0},
        {"initial": 10, "maximum": 5},
    ],
)
def test_rejects_invalid_parameters(kwargs: dict[str, int]) -> None:
    with pytest.raises(ValueError):
        exponential_backoff(**kwargs)
