"""End-to-end tests against a real broker.

Deselected by default. Run with ``pdm run test-integration`` against the
broker at ``PULSAR_URL`` (default ``pulsar://localhost:6650``).
"""

import asyncio
import os
import time
import uuid
from collections.abc import AsyncIterator
from enum import IntEnum
from typing import Any

import pulsar
import pytest
from pulsar.asyncio import Client

from pulsar_dispatcher import PulsarDispatcher

pytestmark = pytest.mark.integration

PULSAR_URL = os.environ.get("PULSAR_URL", "pulsar://localhost:6650")
CLIENT_OPTIONS = {"operation_timeout_seconds": 10}


class Fatal(Exception):
    pass


class Transient(Exception):
    pass


@pytest.fixture
def topic() -> str:
    return f"persistent://public/default/dispatcher-{uuid.uuid4().hex}"


@pytest.fixture
async def client() -> AsyncIterator[Client]:
    client = Client(PULSAR_URL, **CLIENT_OPTIONS)
    yield client
    await client.close()


@pytest.fixture
async def dispatcher() -> AsyncIterator[PulsarDispatcher]:
    dispatcher = PulsarDispatcher(PULSAR_URL, **CLIENT_OPTIONS)
    yield dispatcher
    await dispatcher.close()


async def publish(client: Client, topic: str, *payloads: bytes) -> None:
    producer = await client.create_producer(topic)
    for payload in payloads:
        await producer.send(payload)
    await producer.close()


async def receive(client: Client, topic: str) -> Any:
    consumer = await client.subscribe(
        topic,
        "test-observer",
        initial_position=pulsar.InitialPosition.Earliest,
    )
    try:
        message = await asyncio.wait_for(consumer.receive(), 15)
        await consumer.acknowledge(message)
        return message
    finally:
        await consumer.close()


async def test_processes_messages_in_priority_order(
    client: Client, topic: str
) -> None:
    dispatcher = PulsarDispatcher(PULSAR_URL, concurrency=1, **CLIENT_OPTIONS)
    log: list[bytes] = []

    async def handler(message: Any) -> None:
        log.append(message.data())
        await asyncio.sleep(0.05)

    # Publish first, so both backlogs exist before dispatching starts.
    for name, priority in (("low", 10), ("high", 1)):
        await dispatcher.subscribe(
            f"{topic}-{name}",
            handler,
            priority=priority,
            subscription_name="sub",
            initial_position=pulsar.InitialPosition.Earliest,
        )
    await publish(client, f"{topic}-low", b"low1", b"low2")
    await publish(client, f"{topic}-high", b"high1", b"high2")

    run_task = asyncio.create_task(dispatcher.run())
    await asyncio.wait_for(_until(lambda: len(log) == 4), 15)
    await dispatcher.close()
    await run_task

    assert log == [b"high1", b"high2", b"low1", b"low2"]


async def test_reply_is_published(
    dispatcher: PulsarDispatcher, client: Client, topic: str
) -> None:
    async def handler(message: Any) -> bytes:
        return bytes(message.data()).upper()

    await dispatcher.subscribe(
        topic,
        handler,
        priority=1,
        subscription_name="sub",
        reply_topic=f"{topic}-replies",
    )
    run_task = asyncio.create_task(dispatcher.run())
    await publish(client, topic, b"hello")

    reply = await receive(client, f"{topic}-replies")

    assert reply.data() == b"HELLO"
    await dispatcher.close()
    await run_task


async def test_fatal_error_goes_to_default_dlq(
    dispatcher: PulsarDispatcher, client: Client, topic: str
) -> None:
    async def handler(message: Any) -> None:
        raise Fatal("unprocessable")

    await dispatcher.subscribe(
        topic,
        handler,
        priority=1,
        subscription_name="sub",
        fatal_exception_class=Fatal,
    )
    run_task = asyncio.create_task(dispatcher.run())
    producer = await client.create_producer(topic)
    await producer.send(b"poison", properties={"trace": "t1"})
    await producer.close()

    dead = await receive(client, f"{topic}-sub-DLQ")

    assert dead.data() == b"poison"
    assert dead.properties()["trace"] == "t1"
    assert dead.properties()["REAL_TOPIC"] == topic
    assert dead.properties()["ORIGIN_MESSAGE_ID"]
    await dispatcher.close()
    await run_task


async def test_retry_is_delayed_then_succeeds(
    dispatcher: PulsarDispatcher, client: Client, topic: str
) -> None:
    attempts: list[tuple[float, dict[str, str]]] = []

    async def handler(message: Any) -> None:
        attempts.append((time.monotonic(), message.properties()))
        if len(attempts) == 1:
            raise Transient("try again")

    retry_topic = f"{topic}-retry"
    for name, priority in ((topic, 1), (retry_topic, 2)):
        await dispatcher.subscribe(
            name,
            handler,
            priority=priority,
            subscription_name="sub",
            # Delayed delivery requires a shared subscription.
            consumer_type=pulsar.ConsumerType.Shared,
            retry_exception_class=Transient,
            retry_topic=retry_topic,
            retry_backoff_function=lambda retries: 2,
        )
    run_task = asyncio.create_task(dispatcher.run())
    await publish(client, topic, b"flaky")

    await asyncio.wait_for(_until(lambda: len(attempts) == 2), 20)
    await dispatcher.close()
    await run_task

    (first_at, first_props), (second_at, second_props) = attempts
    assert "RECONSUMETIMES" not in first_props
    assert second_props["RECONSUMETIMES"] == "1"
    assert second_props["REAL_TOPIC"] == topic
    assert second_at - first_at >= 1.5


async def _until(predicate: Any) -> None:
    while not predicate():  # noqa: ASYNC110
        await asyncio.sleep(0.01)


async def test_exhausted_retries_go_to_dlq(
    dispatcher: PulsarDispatcher, client: Client, topic: str
) -> None:
    attempts: list[dict[str, str]] = []

    async def handler(message: Any) -> None:
        attempts.append(message.properties())
        raise Transient("never works")

    retry_topic = f"{topic}-retry"
    for name, priority in ((topic, 1), (retry_topic, 2)):
        await dispatcher.subscribe(
            name,
            handler,
            priority=priority,
            subscription_name="sub",
            consumer_type=pulsar.ConsumerType.Shared,
            retry_exception_class=Transient,
            retry_topic=retry_topic,
            retry_backoff_function=lambda retry: 1,
            max_retries=2,
        )
    run_task = asyncio.create_task(dispatcher.run())
    await publish(client, topic, b"doomed")

    # The last attempt came from the retry topic, but the message is
    # dead-lettered to the original subscription's DLQ.
    dead = await receive(client, f"{topic}-sub-DLQ")

    assert dead.data() == b"doomed"
    assert dead.properties()["DEAD_LETTER_TOPIC"] == f"{topic}-sub-DLQ"
    assert dead.properties()["RECONSUMETIMES"] == "2"
    assert dead.properties()["REAL_TOPIC"] == topic
    assert [a.get("RECONSUMETIMES") for a in attempts] == [None, "1", "2"]
    await dispatcher.close()
    await run_task


async def test_multiple_priorities_retry_within_level(
    dispatcher: PulsarDispatcher, client: Client, topic: str
) -> None:
    class Level(IntEnum):
        LIVE = 100
        BATCH = 200

    class SubLevel(IntEnum):
        HIGH = 10
        RETRY = 50

    attempts: list[str] = []

    async def handler(message: Any) -> None:
        attempts.append(message.topic_name())
        if len(attempts) == 1:
            raise Transient("try again")

    await dispatcher.subscribe_multiple_priorities(
        topic,
        handler,
        Level,
        priority_sublevels=SubLevel,
        retry_exception_class=Transient,
        retry_priority_sublevel=SubLevel.RETRY,
        retry_backoff_function=lambda retry: 1,
        subscription_name="sub",
        consumer_type=pulsar.ConsumerType.Shared,
    )
    run_task = asyncio.create_task(dispatcher.run())
    await publish(client, f"{topic}_batch_high", b"job")

    await asyncio.wait_for(_until(lambda: len(attempts) == 2), 20)
    await dispatcher.close()
    await run_task

    assert attempts == [f"{topic}_batch_high", f"{topic}_batch_retry"]
