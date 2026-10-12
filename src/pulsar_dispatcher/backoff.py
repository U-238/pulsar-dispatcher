from collections.abc import Callable

BackoffFunction = Callable[[int], int]
"""Map a retry number, starting at 1, to a delay in seconds."""


def exponential_backoff(
    initial: int = 1,
    multiplier: int = 2,
    maximum: int = 300,
) -> BackoffFunction:
    """Build a capped exponential backoff function.

    The returned function gives ``initial * multiplier ** (retry - 1)``
    seconds, capped at ``maximum``, for retry numbers starting at 1. With
    the defaults, successive retries wait 1, 2, 4, 8, ... seconds, up to
    5 minutes.
    """

    if initial < 0:
        raise ValueError("initial must not be negative")

    if multiplier < 1:
        raise ValueError("multiplier must be at least 1")

    if maximum < initial:
        raise ValueError("maximum must not be less than initial")

    def backoff(retry: int) -> int:
        delay = initial

        # Multiply step by step, stopping at the cap or once the delay
        # stops growing, so a huge retry number stays cheap.
        for _ in range(max(retry - 1, 0)):
            following = delay * multiplier
            if following == delay or following >= maximum:
                return min(following, maximum)
            delay = following

        return delay

    return backoff


default_backoff: BackoffFunction = exponential_backoff()
"""Wait 1, 2, 4, 8, ... seconds between retries, up to 5 minutes."""
