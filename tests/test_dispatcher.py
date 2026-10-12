import asyncio
from typing import Any

import pytest

from pulsar_dispatcher import PulsarDispatcher, __version__
from tests.conftest import FakeClient, FakeConsumer, wait_until


def client_of(dispatcher: PulsarDispatcher) -> FakeClient:
    assert isinstance(dispatcher.client, FakeClient)
    return dispatcher.client


def consumer_of(dispatcher: PulsarDispatcher, topic: str) -> FakeConsumer:
    return client_of(dispatcher).consumers[topic]


async def stop(run_task: "asyncio.Task[None]") -> None:
    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run_task


def recorder(
    log: list[Any],
    delay: float = 0.0,
) -> Any:
    async def handler(message: Any) -> None:
        log.append(message)
        await asyncio.sleep(delay)

    return handler


def test_version_is_exposed() -> None:
    assert isinstance(__version__, str)


@pytest.mark.parametrize("concurrency", [0, -1])
async def test_rejects_non_positive_concurrency(concurrency: int) -> None:
    with pytest.raises(ValueError, match="concurrency"):
        PulsarDispatcher("pulsar://test", concurrency=concurrency)


async def test_client_kwargs_are_forwarded() -> None:
    dispatcher = PulsarDispatcher("pulsar://test", operation_timeout=5)

    assert client_of(dispatcher).url == "pulsar://test"
    assert client_of(dispatcher).kwargs == {"operation_timeout": 5}


async def test_subscribe_defaults_receiver_queue_size() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    await dispatcher.subscribe(
        "a", recorder([]), priority=1, subscription_name="s"
    )
    await dispatcher.subscribe(
        "b", recorder([]), priority=1, receiver_queue_size=50
    )

    assert consumer_of(dispatcher, "a").kwargs == {
        "subscription_name": "s",
        "receiver_queue_size": 1,
    }
    assert consumer_of(dispatcher, "b").kwargs == {"receiver_queue_size": 50}

    await dispatcher.close()


async def test_processes_and_acknowledges_messages() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    log: list[Any] = []

    await dispatcher.subscribe("orders", recorder(log), priority=1)
    consumer = consumer_of(dispatcher, "orders")
    consumer.push("m1", "m2", "m3")

    run_task = asyncio.create_task(dispatcher.run())
    await wait_until(lambda: len(consumer.acked) == 3)
    await stop(run_task)

    assert sorted(log) == ["m1", "m2", "m3"]
    assert sorted(consumer.acked) == ["m1", "m2", "m3"]
    assert consumer.nacked == []
    assert client_of(dispatcher).closed


async def test_handler_failure_nacks_and_continues() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    async def handler(message: Any) -> None:
        if message == "bad":
            raise ValueError("boom")

    await dispatcher.subscribe("orders", handler, priority=1)
    consumer = consumer_of(dispatcher, "orders")
    consumer.push("ok1", "bad", "ok2")

    run_task = asyncio.create_task(dispatcher.run())
    await wait_until(lambda: len(consumer.acked) + len(consumer.nacked) == 3)
    await stop(run_task)

    assert sorted(consumer.acked) == ["ok1", "ok2"]
    assert consumer.nacked == ["bad"]


async def test_lower_number_is_higher_priority() -> None:
    dispatcher = PulsarDispatcher("pulsar://test", concurrency=1)
    log: list[Any] = []
    handler = recorder(log, delay=0.005)

    await dispatcher.subscribe("low", handler, priority=10)
    await dispatcher.subscribe("high", handler, priority=1)
    consumer_of(dispatcher, "low").push("low1", "low2", "low3")
    consumer_of(dispatcher, "high").push("high1", "high2", "high3")

    run_task = asyncio.create_task(dispatcher.run())
    await wait_until(lambda: len(log) == 6)
    await stop(run_task)

    assert log == ["high1", "high2", "high3", "low1", "low2", "low3"]


async def test_priority_defaults_to_100() -> None:
    dispatcher = PulsarDispatcher("pulsar://test", concurrency=1)
    log: list[Any] = []
    handler = recorder(log, delay=0.005)

    await dispatcher.subscribe("after", handler, priority=101)
    await dispatcher.subscribe("default", handler)
    await dispatcher.subscribe("before", handler, priority=99)
    for topic in ("after", "default", "before"):
        consumer_of(dispatcher, topic).push(topic)

    assert [s.topic for s in dispatcher.sources[100]] == ["default"]

    run_task = asyncio.create_task(dispatcher.run())
    await wait_until(lambda: len(log) == 3)
    await stop(run_task)

    assert log == ["before", "default", "after"]


async def test_priority_is_keyword_only() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    with pytest.raises(TypeError):
        await dispatcher.subscribe("orders", 1, recorder([]))  # type: ignore[call-arg, arg-type]


async def test_equal_priorities_are_round_robin() -> None:
    dispatcher = PulsarDispatcher("pulsar://test", concurrency=1)
    log: list[Any] = []
    handler = recorder(log, delay=0.005)

    await dispatcher.subscribe("a", handler, priority=5)
    await dispatcher.subscribe("b", handler, priority=5)
    consumer_of(dispatcher, "a").push("a1", "a2", "a3")
    consumer_of(dispatcher, "b").push("b1", "b2", "b3")

    run_task = asyncio.create_task(dispatcher.run())
    await wait_until(lambda: len(log) == 6)
    await stop(run_task)

    assert log == ["a1", "b1", "a2", "b2", "a3", "b3"]


async def test_concurrency_limit_is_respected() -> None:
    dispatcher = PulsarDispatcher("pulsar://test", concurrency=2)
    active = 0
    peak = 0
    done = 0

    async def handler(message: Any) -> None:
        nonlocal active, peak, done
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.005)
        active -= 1
        done += 1

    for topic in ("a", "b", "c"):
        await dispatcher.subscribe(topic, handler, priority=1)
        consumer_of(dispatcher, topic).push(*range(4))

    run_task = asyncio.create_task(dispatcher.run())
    await wait_until(lambda: done == 12)
    await stop(run_task)

    assert peak == 2


async def test_close_drains_queue_and_nacks_unqueued_message() -> None:
    dispatcher = PulsarDispatcher("pulsar://test", concurrency=1)
    release = asyncio.Event()
    started: list[Any] = []

    async def handler(message: Any) -> None:
        started.append(message)
        await release.wait()

    await dispatcher.subscribe("orders", handler, priority=1)
    consumer = consumer_of(dispatcher, "orders")
    consumer.push("m1", "m2", "m3")

    run_task = asyncio.create_task(dispatcher.run())

    # m1 is processing, m2 waits in the queue, m3 is held by the reader.
    await wait_until(lambda: started == ["m1"] and consumer.inbox.empty())
    await asyncio.sleep(0.01)

    run_task.cancel()
    await wait_until(lambda: consumer.nacked == ["m3"])
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await run_task

    assert started == ["m1", "m2"]
    assert consumer.acked == ["m1", "m2"]
    assert client_of(dispatcher).closed


async def test_reader_failure_stops_run() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    await dispatcher.subscribe("orders", recorder([]), priority=1)
    consumer = consumer_of(dispatcher, "orders")
    consumer.receive_error = ConnectionError("lost broker")

    with pytest.raises(ConnectionError, match="lost broker"):
        await asyncio.wait_for(dispatcher.run(), 1)

    assert client_of(dispatcher).closed


async def test_subscribe_after_run_is_rejected() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    await dispatcher.subscribe("orders", recorder([]), priority=1)

    run_task = asyncio.create_task(dispatcher.run())
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="after run"):
        await dispatcher.subscribe("late", recorder([]), priority=1)

    with pytest.raises(RuntimeError, match="already running"):
        await dispatcher.run()

    await stop(run_task)


async def test_subscribe_after_close_is_rejected() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    await dispatcher.close()

    with pytest.raises(RuntimeError, match="closed"):
        await dispatcher.subscribe("orders", recorder([]), priority=1)


async def test_close_is_idempotent() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    await dispatcher.subscribe("orders", recorder([]), priority=1)

    await dispatcher.close()
    await dispatcher.close()

    assert client_of(dispatcher).closed


async def test_external_close_stops_run_cleanly() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    log: list[Any] = []

    await dispatcher.subscribe("orders", recorder(log), priority=1)
    consumer = consumer_of(dispatcher, "orders")
    consumer.push("m1", "m2")

    run_task = asyncio.create_task(dispatcher.run())
    await wait_until(lambda: len(consumer.acked) == 2)

    await dispatcher.close()

    # run() returns normally instead of raising CancelledError.
    await asyncio.wait_for(run_task, 1)
    assert client_of(dispatcher).close_count == 1


async def test_concurrent_close_shuts_down_once() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    release = asyncio.Event()

    async def handler(message: Any) -> None:
        await release.wait()

    await dispatcher.subscribe("orders", handler, priority=1)
    consumer = consumer_of(dispatcher, "orders")
    consumer.push("m1")

    run_task = asyncio.create_task(dispatcher.run())
    await wait_until(lambda: consumer.inbox.empty())
    await asyncio.sleep(0.01)

    closers = [asyncio.create_task(dispatcher.close()) for _ in range(3)]
    await asyncio.sleep(0.01)

    # Every caller waits for in-flight work, not just the first.
    assert not any(closer.done() for closer in closers)

    release.set()
    await asyncio.gather(*closers, run_task)

    assert consumer.acked == ["m1"]
    assert client_of(dispatcher).close_count == 1


async def test_cancelled_close_caller_does_not_abort_shutdown() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    release = asyncio.Event()

    async def handler(message: Any) -> None:
        await release.wait()

    await dispatcher.subscribe("orders", handler, priority=1)
    consumer = consumer_of(dispatcher, "orders")
    consumer.push("m1")

    run_task = asyncio.create_task(dispatcher.run())
    await wait_until(lambda: consumer.inbox.empty())
    await asyncio.sleep(0.01)

    closer = asyncio.create_task(dispatcher.close())
    await asyncio.sleep(0.01)
    closer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closer

    release.set()
    await asyncio.wait_for(run_task, 1)

    assert consumer.acked == ["m1"]
    assert client_of(dispatcher).closed


async def test_nothing_is_received_before_run() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")

    await dispatcher.subscribe("orders", recorder([]), priority=1)
    consumer = consumer_of(dispatcher, "orders")
    consumer.push("m1", "m2")
    await asyncio.sleep(0.01)

    assert consumer.inbox.qsize() == 2

    await dispatcher.close()

    # Messages stay with Pulsar rather than stranded in local queues.
    assert consumer.inbox.qsize() == 2
    assert consumer.acked == []
    assert consumer.nacked == []
    assert client_of(dispatcher).closed


async def test_run_after_close_is_rejected() -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    await dispatcher.close()

    with pytest.raises(RuntimeError, match="closed"):
        await dispatcher.run()


async def test_dispatcher_failure_still_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatcher = PulsarDispatcher("pulsar://test")
    await dispatcher.subscribe("orders", recorder([]), priority=1)

    async def broken_next() -> Any:
        raise RuntimeError("dispatcher broke")

    monkeypatch.setattr(dispatcher, "_next", broken_next)

    with pytest.raises(RuntimeError, match="dispatcher broke"):
        await asyncio.wait_for(dispatcher.run(), 1)

    assert client_of(dispatcher).closed


async def test_cancelled_processing_task_releases_its_slot() -> None:
    dispatcher = PulsarDispatcher("pulsar://test", concurrency=1)
    log: list[Any] = []
    cancelled = False

    await dispatcher.subscribe("orders", recorder(log))
    consumer = consumer_of(dispatcher, "orders")
    consumer.push("m1", "m2")

    original_create_task = asyncio.create_task

    def create_task(coro: Any, **kwargs: Any) -> "asyncio.Task[Any]":
        nonlocal cancelled
        task = original_create_task(coro, **kwargs)

        # Cancel the first processing task before it ever runs.
        if not cancelled and task.get_name().startswith("process-"):
            cancelled = True
            task.cancel()

        return task

    run_task = asyncio.create_task(dispatcher.run())
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(asyncio, "create_task", create_task)
        # With concurrency=1, m2 is only processed if m1's slot came back.
        await wait_until(lambda: log == ["m2"])

    await stop(run_task)

    assert consumer.acked == ["m2"]
