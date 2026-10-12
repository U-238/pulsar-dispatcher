import asyncio
import warnings
from datetime import timedelta
from typing import Any

import pulsar
import pytest

from pulsar_dispatcher import PulsarDispatcher, RetryTopicWarning
from tests.conftest import (
    FakeClient,
    FakeConsumer,
    FakeMessage,
    FakeProducer,
    wait_until,
)


class Fatal(Exception):
    pass


class Transient(Exception):
    pass


class FatalAndTransient(Fatal, Transient):
    pass


def client_of(dispatcher: PulsarDispatcher) -> FakeClient:
    assert isinstance(dispatcher.client, FakeClient)
    return dispatcher.client


def consumer_of(dispatcher: PulsarDispatcher, topic: str) -> FakeConsumer:
    return client_of(dispatcher).consumers[topic]


def producer_of(dispatcher: PulsarDispatcher, topic: str) -> FakeProducer:
    return client_of(dispatcher).producers[topic]


def raising(exc: BaseException) -> Any:
    async def handler(message: Any) -> None:
        raise exc

    return handler


def returning(value: bytes | None) -> Any:
    async def handler(message: Any) -> bytes | None:
        return value

    return handler


async def process(
    dispatcher: PulsarDispatcher,
    topic: str,
    *messages: Any,
) -> FakeConsumer:
    """Run ``dispatcher`` until every message is acked or nacked."""

    consumer = consumer_of(dispatcher, topic)
    consumer.push(*messages)

    run_task = asyncio.create_task(dispatcher.run())
    await wait_until(
        lambda: len(consumer.acked) + len(consumer.nacked) == len(messages)
    )
    await dispatcher.close()
    await run_task

    return consumer


async def subscribe(
    handler: Any,
    **kwargs: Any,
) -> PulsarDispatcher:
    """Subscribe ``handler`` to "orders", and to its retry topic if any."""

    dispatcher = PulsarDispatcher("pulsar://test")
    kwargs.setdefault("subscription_name", "orders-sub")
    kwargs.setdefault("consumer_type", pulsar.ConsumerType.Shared)
    await dispatcher.subscribe("orders", handler, priority=1, **kwargs)

    if "retry_topic" in kwargs:
        await dispatcher.subscribe(
            kwargs["retry_topic"], handler, priority=2, **kwargs
        )

    return dispatcher


async def start_and_close(dispatcher: PulsarDispatcher) -> None:
    """Await run() directly, closing it once it has started."""

    loop = asyncio.get_running_loop()
    loop.call_later(0.01, lambda: loop.create_task(dispatcher.close()))
    await dispatcher.run()


# Validation


@pytest.mark.parametrize(
    "kwargs",
    [
        {"retry_exception_class": Transient},
        {"retry_topic": "orders-retry"},
        {"retry_backoff_function": lambda retry: 1},
        {"max_retries": 3},
    ],
)
async def test_retry_options_must_be_used_together(
    kwargs: dict[str, Any],
) -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    with pytest.raises(ValueError, match="retry"):
        await dispatcher.subscribe(
            "orders", returning(None), priority=1, **kwargs
        )

    assert client_of(dispatcher).consumers == {}


@pytest.mark.parametrize(
    "name", ["fatal_exception_class", "retry_exception_class"]
)
@pytest.mark.parametrize("value", [ValueError("not a class"), int])
async def test_exception_classes_are_validated(name: str, value: Any) -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    with pytest.raises(TypeError, match=name):
        await dispatcher.subscribe(
            "orders",
            returning(None),
            priority=1,
            retry_topic="orders-retry",
            **{name: value},
        )


async def test_fatal_requires_a_dead_letter_topic() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    with pytest.raises(ValueError, match="subscription_name"):
        await dispatcher.subscribe(
            "orders", returning(None), priority=1, fatal_exception_class=Fatal
        )


@pytest.mark.parametrize("max_retries", [0, -1])
async def test_max_retries_must_be_positive(max_retries: int) -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    with pytest.raises(ValueError, match="positive"):
        await dispatcher.subscribe(
            "orders",
            returning(None),
            priority=1,
            retry_exception_class=Transient,
            retry_topic="orders-retry",
            max_retries=max_retries,
            subscription_name="orders-sub",
        )


async def test_max_retries_requires_a_dead_letter_topic() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    with pytest.raises(ValueError, match="max_retries"):
        await dispatcher.subscribe(
            "orders",
            returning(None),
            priority=1,
            retry_exception_class=Transient,
            retry_topic="orders-retry",
            max_retries=3,
        )


async def test_default_dead_letter_topic_matches_pulsar() -> None:
    dispatcher = await subscribe(returning(None), fatal_exception_class=Fatal)

    assert list(client_of(dispatcher).producers) == ["orders-orders-sub-DLQ"]


async def test_dead_letter_policy_topic_is_used() -> None:
    policy = pulsar.ConsumerDeadLetterPolicy(
        max_redeliver_count=3,
        dead_letter_topic="custom-dlq",
    )
    dispatcher = await subscribe(
        returning(None),
        fatal_exception_class=Fatal,
        dead_letter_policy=policy,
    )

    assert list(client_of(dispatcher).producers) == ["custom-dlq"]
    assert consumer_of(dispatcher, "orders").kwargs["dead_letter_policy"] is (
        policy
    )


async def test_producers_are_shared_and_unbatched() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    for topic, priority in (("orders", 1), ("orders-retry", 2)):
        await dispatcher.subscribe(
            topic,
            returning(None),
            priority=priority,
            retry_exception_class=Transient,
            retry_topic="orders-retry",
            reply_topic="replies",
            subscription_name="orders-sub",
            consumer_type=pulsar.ConsumerType.Shared,
        )

    producers = client_of(dispatcher).producers
    assert sorted(producers) == ["orders-retry", "replies"]
    assert all(
        producer.kwargs == {"batching_enabled": False}
        for producer in producers.values()
    )


# Fatal errors


@pytest.mark.parametrize("exc", [Fatal("bad"), FatalAndTransient("both")])
async def test_fatal_error_dead_letters_and_acks(exc: Exception) -> None:
    dispatcher = await subscribe(
        raising(exc),
        fatal_exception_class=Fatal,
        retry_exception_class=Transient,
        retry_topic="orders-retry",
    )
    message = FakeMessage(
        b"payload",
        props={"trace": "abc"},
        key="customer-1",
        order_key="k",
        timestamp=1234,
    )

    consumer = await process(dispatcher, "orders", message)

    assert consumer.acked == [message]
    assert consumer.nacked == []
    assert producer_of(dispatcher, "orders-retry").sent == []

    [sent] = producer_of(dispatcher, "orders-orders-sub-DLQ").sent
    assert sent.content == b"payload"
    assert sent.properties == {
        "trace": "abc",
        "REAL_TOPIC": "persistent://public/default/orders",
        "ORIGIN_MESSAGE_ID": "(1,2,-1,0)",
    }
    assert sent.partition_key == "customer-1"
    assert sent.ordering_key == "k"
    assert sent.event_timestamp == 1234
    assert sent.deliver_after is None


async def test_unknown_error_is_nacked() -> None:
    dispatcher = await subscribe(
        raising(KeyError("other")),
        fatal_exception_class=Fatal,
        retry_exception_class=Transient,
        retry_topic="orders-retry",
    )

    consumer = await process(dispatcher, "orders", FakeMessage(b"x"))

    assert consumer.acked == []
    assert len(consumer.nacked) == 1
    assert all(not p.sent for p in client_of(dispatcher).producers.values())


async def test_failed_republish_is_nacked() -> None:
    dispatcher = await subscribe(raising(Fatal()), fatal_exception_class=Fatal)
    dlq = producer_of(dispatcher, "orders-orders-sub-DLQ")
    dlq.send_error = ConnectionError("broker down")

    consumer = await process(dispatcher, "orders", FakeMessage(b"x"))

    assert consumer.acked == []
    assert len(consumer.nacked) == 1


# Retries


async def test_retry_republishes_with_delay_and_acks() -> None:
    calls: list[int] = []

    def backoff(retry: int) -> int:
        calls.append(retry)
        return 30

    dispatcher = await subscribe(
        raising(Transient("try later")),
        retry_exception_class=Transient,
        retry_topic="orders-retry",
        retry_backoff_function=backoff,
    )
    message = FakeMessage(b"payload")

    consumer = await process(dispatcher, "orders", message)

    assert consumer.acked == [message]
    assert calls == [1]

    [sent] = producer_of(dispatcher, "orders-retry").sent
    assert sent.content == b"payload"
    assert sent.deliver_after == timedelta(seconds=30)
    assert sent.properties is not None
    assert sent.properties["RECONSUMETIMES"] == "1"


async def test_retry_count_increments_and_keeps_origin() -> None:
    calls: list[int] = []

    def backoff(retry: int) -> int:
        calls.append(retry)
        return 0

    dispatcher = await subscribe(
        raising(Transient()),
        retry_exception_class=Transient,
        retry_topic="orders-retry",
        retry_backoff_function=backoff,
    )
    message = FakeMessage(
        b"payload",
        topic="persistent://public/default/orders-retry",
        props={
            "RECONSUMETIMES": "3",
            "REAL_TOPIC": "persistent://public/default/orders",
            "ORIGIN_MESSAGE_ID": "(9,9,-1,0)",
        },
    )

    await process(dispatcher, "orders", message)

    assert calls == [4]
    [sent] = producer_of(dispatcher, "orders-retry").sent
    assert sent.properties == {
        "RECONSUMETIMES": "4",
        "REAL_TOPIC": "persistent://public/default/orders",
        "ORIGIN_MESSAGE_ID": "(9,9,-1,0)",
        "DEAD_LETTER_TOPIC": "orders-orders-sub-DLQ",
    }
    assert sent.deliver_after is None


async def test_default_backoff_is_used() -> None:
    dispatcher = await subscribe(
        raising(Transient()),
        retry_exception_class=Transient,
        retry_topic="orders-retry",
    )
    message = FakeMessage(b"x", props={"RECONSUMETIMES": "2"})

    await process(dispatcher, "orders", message)

    [sent] = producer_of(dispatcher, "orders-retry").sent
    assert sent.deliver_after == timedelta(seconds=4)


@pytest.mark.parametrize(
    ("previous", "retried"),
    [(None, True), ("2", True), ("3", False), ("10", False)],
)
async def test_max_retries_then_dead_letters(
    previous: str | None, retried: bool
) -> None:
    dispatcher = await subscribe(
        raising(Transient()),
        retry_exception_class=Transient,
        retry_topic="orders-retry",
        max_retries=3,
    )
    props = {} if previous is None else {"RECONSUMETIMES": previous}
    message = FakeMessage(b"x", props=props)

    consumer = await process(dispatcher, "orders", message)

    assert consumer.acked == [message]
    retry_sent = producer_of(dispatcher, "orders-retry").sent
    dead_sent = producer_of(dispatcher, "orders-orders-sub-DLQ").sent
    assert (len(retry_sent), len(dead_sent)) == ((1, 0) if retried else (0, 1))

    if not retried:
        # Dead letters keep the retry count they arrived with.
        assert dead_sent[0].properties is not None
        assert dead_sent[0].properties["RECONSUMETIMES"] == previous


async def test_max_retries_dead_letters_with_policy_topic() -> None:
    policy = pulsar.ConsumerDeadLetterPolicy(
        max_redeliver_count=5, dead_letter_topic="custom-dlq"
    )
    dispatcher = await subscribe(
        raising(Transient()),
        retry_exception_class=Transient,
        retry_topic="orders-retry",
        max_retries=1,
        dead_letter_policy=policy,
    )
    message = FakeMessage(b"x", props={"RECONSUMETIMES": "1"})

    await process(dispatcher, "orders", message)

    assert len(producer_of(dispatcher, "custom-dlq").sent) == 1


async def test_malformed_retry_count_starts_from_zero() -> None:
    dispatcher = await subscribe(
        raising(Transient()),
        retry_exception_class=Transient,
        retry_topic="orders-retry",
    )
    message = FakeMessage(b"x", props={"RECONSUMETIMES": "lots"})

    await process(dispatcher, "orders", message)

    [sent] = producer_of(dispatcher, "orders-retry").sent
    assert sent.properties is not None
    assert sent.properties["RECONSUMETIMES"] == "1"


async def test_base_exception_subclasses_can_be_retried() -> None:
    class Interrupted(BaseException):
        pass

    dispatcher = await subscribe(
        raising(Interrupted()),
        retry_exception_class=Interrupted,
        retry_topic="orders-retry",
    )
    message = FakeMessage(b"x")

    consumer = await process(dispatcher, "orders", message)

    assert consumer.acked == [message]
    assert len(producer_of(dispatcher, "orders-retry").sent) == 1


# Replies


async def test_return_value_is_published_before_ack() -> None:
    dispatcher = await subscribe(returning(b"result"), reply_topic="replies")
    message = FakeMessage(b"request")

    consumer = await process(dispatcher, "orders", message)

    assert consumer.acked == [message]
    [sent] = producer_of(dispatcher, "replies").sent
    assert sent.content == b"result"


async def test_none_return_value_publishes_nothing() -> None:
    dispatcher = await subscribe(returning(None), reply_topic="replies")

    consumer = await process(dispatcher, "orders", FakeMessage(b"x"))

    assert len(consumer.acked) == 1
    assert producer_of(dispatcher, "replies").sent == []


async def test_failed_reply_is_nacked() -> None:
    dispatcher = await subscribe(returning(b"result"), reply_topic="replies")
    producer_of(dispatcher, "replies").send_error = ConnectionError("down")

    consumer = await process(dispatcher, "orders", FakeMessage(b"x"))

    assert consumer.acked == []
    assert len(consumer.nacked) == 1


async def test_return_value_ignored_without_reply_topic() -> None:
    dispatcher = await subscribe(returning(b"result"))

    consumer = await process(dispatcher, "orders", FakeMessage(b"x"))

    assert len(consumer.acked) == 1
    assert client_of(dispatcher).producers == {}


# Retry topic warnings


async def test_warns_when_retry_topic_is_not_consumed() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    await dispatcher.subscribe(
        "orders",
        returning(None),
        priority=1,
        retry_exception_class=Transient,
        retry_topic="orders-retry",
        subscription_name="orders-sub",
    )

    with pytest.warns(RetryTopicWarning, match="no subscription") as record:
        await start_and_close(dispatcher)

    assert len(record) == 1
    # The warning points at the caller of run(), not library internals.
    assert record[0].filename == __file__


@pytest.mark.parametrize(
    "retry_subscription_topic",
    [
        "orders-retry",
        "public/default/orders-retry",
        "persistent://public/default/orders-retry",
    ],
)
async def test_retry_topic_names_are_matched_in_any_form(
    retry_subscription_topic: str,
) -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    for topic, priority in (("orders", 1), (retry_subscription_topic, 2)):
        await dispatcher.subscribe(
            topic,
            returning(None),
            priority=priority,
            retry_exception_class=Transient,
            retry_topic="orders-retry",
            subscription_name="orders-sub",
            consumer_type=pulsar.ConsumerType.Shared,
        )

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        await start_and_close(dispatcher)


async def test_retry_topic_consumed_by_plain_subscription_is_enough() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    await dispatcher.subscribe(
        "orders",
        returning(None),
        priority=1,
        retry_exception_class=Transient,
        retry_topic="orders-retry",
        subscription_name="orders-sub",
    )
    # A different handler, with no retry options of its own.
    await dispatcher.subscribe(
        "orders-retry",
        returning(None),
        priority=2,
        subscription_name="retry-sub",
        consumer_type=pulsar.ConsumerType.KeyShared,
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        await start_and_close(dispatcher)


@pytest.mark.parametrize(
    "consumer_type",
    [None, pulsar.ConsumerType.Exclusive, pulsar.ConsumerType.Failover],
)
async def test_warns_when_retry_topic_ignores_delays(
    consumer_type: Any,
) -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    await dispatcher.subscribe(
        "orders",
        returning(None),
        priority=1,
        retry_exception_class=Transient,
        retry_topic="orders-retry",
        subscription_name="orders-sub",
    )
    options = {} if consumer_type is None else {"consumer_type": consumer_type}
    await dispatcher.subscribe(
        "orders-retry",
        returning(None),
        priority=2,
        subscription_name="s",
        **options,
    )

    with pytest.warns(RetryTopicWarning, match="Shared") as record:
        await start_and_close(dispatcher)

    assert len(record) == 1
    assert "'orders-retry'" in str(record[0].message)


# Dead-lettering retried messages to the original topic's DLQ


async def retry_pipeline(handler: Any, **kwargs: Any) -> PulsarDispatcher:
    """Subscribe "orders" and "orders-retry" with separate DLQs."""

    dispatcher = PulsarDispatcher("pulsar://test", concurrency=4)
    for topic, priority in (("orders", 1), ("orders-retry", 2)):
        await dispatcher.subscribe(
            topic,
            handler,
            priority=priority,
            retry_exception_class=Transient,
            retry_topic="orders-retry",
            subscription_name="orders-sub",
            consumer_type=pulsar.ConsumerType.Shared,
            **kwargs,
        )
    return dispatcher


async def test_first_retry_records_original_dead_letter_topic() -> None:
    dispatcher = await retry_pipeline(raising(Transient()))

    await process(dispatcher, "orders", FakeMessage(b"x"))

    [sent] = producer_of(dispatcher, "orders-retry").sent
    assert sent.properties is not None
    assert sent.properties["DEAD_LETTER_TOPIC"] == "orders-orders-sub-DLQ"


async def test_later_retries_keep_original_dead_letter_topic() -> None:
    dispatcher = await retry_pipeline(raising(Transient()))
    message = FakeMessage(
        b"x",
        topic="persistent://public/default/orders-retry",
        props={"RECONSUMETIMES": "1", "DEAD_LETTER_TOPIC": "first-DLQ"},
    )

    await process(dispatcher, "orders-retry", message)

    [sent] = producer_of(dispatcher, "orders-retry").sent
    assert sent.properties is not None
    assert sent.properties["DEAD_LETTER_TOPIC"] == "first-DLQ"


@pytest.mark.parametrize(
    "exc",
    [Fatal(), Transient()],
    ids=["fatal", "retries-exhausted"],
)
async def test_retried_message_goes_to_original_dead_letter_topic(
    exc: Exception,
) -> None:
    dispatcher = await retry_pipeline(
        raising(exc), fatal_exception_class=Fatal, max_retries=2
    )
    message = FakeMessage(
        b"x",
        topic="persistent://public/default/orders-retry",
        props={
            "RECONSUMETIMES": "2",
            "DEAD_LETTER_TOPIC": "orders-orders-sub-DLQ",
        },
    )

    await process(dispatcher, "orders-retry", message)

    assert len(producer_of(dispatcher, "orders-orders-sub-DLQ").sent) == 1
    assert producer_of(dispatcher, "orders-retry-orders-sub-DLQ").sent == []


async def test_unstamped_message_uses_own_dead_letter_topic() -> None:
    dispatcher = await retry_pipeline(
        raising(Transient()), fatal_exception_class=Fatal, max_retries=1
    )
    # E.g. retried by an older version, or published there directly.
    message = FakeMessage(b"x", props={"RECONSUMETIMES": "1"})

    await process(dispatcher, "orders-retry", message)

    own_dlq = producer_of(dispatcher, "orders-retry-orders-sub-DLQ")
    assert len(own_dlq.sent) == 1


async def test_unknown_dead_letter_topic_producer_is_created_once() -> None:
    dispatcher = await retry_pipeline(
        raising(Fatal()), fatal_exception_class=Fatal
    )
    messages = [
        FakeMessage(b"x", id=str(n), props={"DEAD_LETTER_TOPIC": "other-DLQ"})
        for n in range(4)
    ]

    consumer = await process(dispatcher, "orders", *messages)

    assert len(consumer.acked) == 4
    assert len(producer_of(dispatcher, "other-DLQ").sent) == 4


async def test_failed_producer_creation_is_retried() -> None:
    dispatcher = await retry_pipeline(
        raising(Fatal()), fatal_exception_class=Fatal
    )
    client = client_of(dispatcher)
    create_producer = client.create_producer
    failures = [ConnectionError("broker down")]

    async def flaky_create_producer(topic: str, **kwargs: Any) -> Any:
        if failures:
            raise failures.pop()
        return await create_producer(topic, **kwargs)

    client.create_producer = flaky_create_producer  # type: ignore[method-assign]
    stamped = {"DEAD_LETTER_TOPIC": "other-DLQ"}

    consumer = await process(
        dispatcher, "orders", FakeMessage(b"1", props=stamped)
    )

    assert len(consumer.nacked) == 1
    # The failure is not cached, so the next dead letter can try again.
    assert "other-DLQ" not in dispatcher.producers
    assert await dispatcher._producer("other-DLQ") is producer_of(
        dispatcher, "other-DLQ"
    )
