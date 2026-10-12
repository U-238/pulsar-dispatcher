import asyncio
from enum import IntEnum
from typing import Any

import pulsar
import pytest

from pulsar_dispatcher import PulsarDispatcher
from tests.conftest import FakeClient, FakeMessage, wait_until


class PriorityLevel(IntEnum):
    LIVE = 100
    BATCH = 200


class PrioritySubLevel(IntEnum):
    HIGH = 10
    LOW = 20
    RETRY = 50


class Transient(Exception):
    pass


async def handler(message: Any) -> None:
    pass


def client_of(dispatcher: PulsarDispatcher) -> FakeClient:
    assert isinstance(dispatcher.client, FakeClient)
    return dispatcher.client


def layout(dispatcher: PulsarDispatcher) -> dict[str, tuple[int, str | None]]:
    """Map each subscribed topic to its priority and retry topic."""

    return {
        source.topic: (source.priority, source.retry_topic)
        for group in dispatcher.sources.values()
        for source in group
    }


async def test_levels_only() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    await dispatcher.subscribe_multiple_priorities(
        "orders", handler, PriorityLevel, subscription_name="sub"
    )

    assert layout(dispatcher) == {
        "orders_live": (100, None),
        "orders_batch": (200, None),
    }
    assert all(
        consumer.kwargs["subscription_name"] == "sub"
        for consumer in client_of(dispatcher).consumers.values()
    )


async def test_levels_and_sublevels() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    await dispatcher.subscribe_multiple_priorities(
        "orders",
        handler,
        PriorityLevel,
        priority_sublevels=PrioritySubLevel,
    )

    assert layout(dispatcher) == {
        "orders_live_high": (110, None),
        "orders_live_low": (120, None),
        "orders_live_retry": (150, None),
        "orders_batch_high": (210, None),
        "orders_batch_low": (220, None),
        "orders_batch_retry": (250, None),
    }


async def test_retry_sublevel_is_retry_topic_within_each_level() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    await dispatcher.subscribe_multiple_priorities(
        "orders",
        handler,
        PriorityLevel,
        priority_sublevels=PrioritySubLevel,
        retry_exception_class=Transient,
        retry_priority_sublevel=PrioritySubLevel.RETRY,
        subscription_name="sub",
        consumer_type=pulsar.ConsumerType.Shared,
    )

    assert layout(dispatcher) == {
        "orders_live_high": (110, "orders_live_retry"),
        "orders_live_low": (120, "orders_live_retry"),
        "orders_live_retry": (150, "orders_live_retry"),
        "orders_batch_high": (210, "orders_batch_retry"),
        "orders_batch_low": (220, "orders_batch_retry"),
        "orders_batch_retry": (250, "orders_batch_retry"),
    }


async def test_retry_sublevel_accepts_plain_int() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    await dispatcher.subscribe_multiple_priorities(
        "orders",
        handler,
        PriorityLevel,
        priority_sublevels=PrioritySubLevel,
        retry_exception_class=Transient,
        retry_priority_sublevel=50,
        consumer_type=pulsar.ConsumerType.Shared,
    )

    assert layout(dispatcher)["orders_live_high"] == (110, "orders_live_retry")


@pytest.mark.parametrize("sublevels", [None, PrioritySubLevel])
async def test_retry_topic_is_shared_by_all_levels(sublevels: Any) -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    await dispatcher.subscribe_multiple_priorities(
        "orders",
        handler,
        PriorityLevel,
        priority_sublevels=sublevels,
        retry_exception_class=Transient,
        retry_topic="orders_retry",
    )

    assert {retry for _, retry in layout(dispatcher).values()} == {
        "orders_retry"
    }
    # One shared producer for the retry topic.
    assert list(client_of(dispatcher).producers) == ["orders_retry"]


async def test_options_are_passed_to_every_subscription() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    await dispatcher.subscribe_multiple_priorities(
        "persistent://tenant/ns/orders",
        handler,
        PriorityLevel,
        fatal_exception_class=Transient,
        reply_topic="replies",
        subscription_name="sub",
        receiver_queue_size=5,
    )

    client = client_of(dispatcher)
    assert sorted(client.consumers) == [
        "persistent://tenant/ns/orders_batch",
        "persistent://tenant/ns/orders_live",
    ]
    assert all(
        consumer.kwargs["receiver_queue_size"] == 5
        for consumer in client.consumers.values()
    )
    assert sorted(client.producers) == [
        "persistent://tenant/ns/orders_batch-sub-DLQ",
        "persistent://tenant/ns/orders_live-sub-DLQ",
        "replies",
    ]


async def test_single_level_has_no_gap_limit() -> None:
    class Only(IntEnum):
        MAIN = 100

    class Wide(IntEnum):
        A = 0
        B = 1000

    dispatcher = PulsarDispatcher("pulsar://test")

    await dispatcher.subscribe_multiple_priorities(
        "t", handler, Only, priority_sublevels=Wide
    )

    assert layout(dispatcher) == {
        "t_main_a": (100, None),
        "t_main_b": (1100, None),
    }


async def test_gap_is_the_smallest_between_any_levels() -> None:
    class Uneven(IntEnum):
        A = 100
        B = 130
        C = 500

    class Sub(IntEnum):
        X = 29

    dispatcher = PulsarDispatcher("pulsar://test")

    await dispatcher.subscribe_multiple_priorities(
        "t", handler, Uneven, priority_sublevels=Sub
    )

    assert sorted(p for p, _ in layout(dispatcher).values()) == [
        129,
        159,
        529,
    ]


async def test_retried_messages_land_in_level_retry_topic() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    attempts: list[str] = []

    async def flaky(message: Any) -> None:
        attempts.append(message.topic_name())
        if len(attempts) == 1:
            raise Transient()

    await dispatcher.subscribe_multiple_priorities(
        "orders",
        flaky,
        PriorityLevel,
        priority_sublevels=PrioritySubLevel,
        retry_exception_class=Transient,
        retry_priority_sublevel=PrioritySubLevel.RETRY,
        subscription_name="sub",
        consumer_type=pulsar.ConsumerType.Shared,
    )
    consumer = client_of(dispatcher).consumers["orders_batch_low"]
    consumer.push(FakeMessage(b"x", topic="orders_batch_low"))

    run_task = asyncio.create_task(dispatcher.run())
    await wait_until(lambda: len(consumer.acked) == 1)
    await dispatcher.close()
    await run_task

    [sent] = client_of(dispatcher).producers["orders_batch_retry"].sent
    assert sent.properties is not None
    assert sent.properties["DEAD_LETTER_TOPIC"] == "orders_batch_low-sub-DLQ"


# Validation; none of these may leave anything subscribed.


class Empty(IntEnum):
    pass


class TooWide(IntEnum):
    SMALL = 10
    HUGE = 100  # Equal to the gap between LIVE and BATCH.


class Negative(IntEnum):
    BELOW = -1
    ABOVE = 10


class Clashing(IntEnum):
    High = 10
    HIGH = 20


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        (
            {
                "retry_topic": "r",
                "retry_priority_sublevel": 50,
                "priority_sublevels": PrioritySubLevel,
                "retry_exception_class": Transient,
            },
            "Only one of",
        ),
        (
            {
                "retry_priority_sublevel": 50,
                "retry_exception_class": Transient,
            },
            "requires priority_sublevels",
        ),
        (
            {
                "retry_priority_sublevel": 50,
                "priority_sublevels": PrioritySubLevel,
            },
            "requires retry_exception_class",
        ),
        (
            {
                "retry_priority_sublevel": 30,
                "priority_sublevels": PrioritySubLevel,
                "retry_exception_class": Transient,
            },
            "not a value of PrioritySubLevel",
        ),
        ({"priority_sublevels": TooWide}, r"HUGE=100 .* \(100\)"),
        ({"priority_sublevels": Negative}, "negative"),
        ({"priority_sublevels": Empty}, "priority_sublevels"),
        ({"priority_sublevels": Clashing}, "distinct when lowercased"),
        ({"retry_exception_class": Transient}, "retry_topic"),
    ],
)
async def test_invalid_arguments_subscribe_nothing(
    kwargs: dict[str, Any], match: str
) -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    with pytest.raises(ValueError, match=match):
        await dispatcher.subscribe_multiple_priorities(
            "orders", handler, PriorityLevel, **kwargs
        )

    assert client_of(dispatcher).consumers == {}
    assert client_of(dispatcher).producers == {}


async def test_empty_priority_levels_are_rejected() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    with pytest.raises(ValueError, match="priority_levels"):
        await dispatcher.subscribe_multiple_priorities(
            "orders", handler, Empty
        )
