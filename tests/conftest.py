import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pytest

from pulsar_dispatcher import dispatcher as dispatcher_module


@dataclass
class FakeMessage:
    """In-memory stand-in for ``pulsar.Message``."""

    payload: bytes
    props: dict[str, str] = field(default_factory=dict)
    topic: str = "persistent://public/default/orders"
    id: str = "(1,2,-1,0)"
    key: str = ""
    order_key: str = ""
    timestamp: int = 0

    def data(self) -> bytes:
        return self.payload

    def properties(self) -> dict[str, str]:
        return dict(self.props)

    def topic_name(self) -> str:
        return self.topic

    def message_id(self) -> str:
        return self.id

    def partition_key(self) -> str:
        return self.key

    def ordering_key(self) -> str:
        return self.order_key

    def event_timestamp(self) -> int:
        return self.timestamp


@dataclass
class Sent:
    content: Any
    properties: dict[str, str] | None = None
    partition_key: str | None = None
    ordering_key: str | None = None
    event_timestamp: int | None = None
    deliver_after: timedelta | None = None


class FakeProducer:
    """In-memory stand-in for ``pulsar.asyncio.Producer``."""

    def __init__(self, topic: str, **kwargs: Any) -> None:
        self.topic = topic
        self.kwargs = kwargs
        self.sent: list[Sent] = []
        self.send_error: BaseException | None = None

    async def send(self, content: Any, **kwargs: Any) -> None:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(Sent(content, **kwargs))


class FakeConsumer:
    """In-memory stand-in for ``pulsar.asyncio.Consumer``."""

    def __init__(self, topic: str, **kwargs: Any) -> None:
        self.topic = topic
        self.kwargs = kwargs
        self.inbox: asyncio.Queue[Any] = asyncio.Queue()
        self.acked: list[Any] = []
        self.nacked: list[Any] = []
        self.receive_error: BaseException | None = None

    def push(self, *messages: Any) -> None:
        for message in messages:
            self.inbox.put_nowait(message)

    async def receive(self) -> Any:
        if self.receive_error is not None:
            raise self.receive_error
        return await self.inbox.get()

    async def acknowledge(self, message: Any) -> None:
        self.acked.append(message)

    async def negative_acknowledge(self, message: Any) -> None:
        self.nacked.append(message)


class FakeClient:
    """In-memory stand-in for ``pulsar.asyncio.Client``."""

    def __init__(self, url: str, **kwargs: Any) -> None:
        self.url = url
        self.kwargs = kwargs
        self.consumers: dict[str, FakeConsumer] = {}
        self.producers: dict[str, FakeProducer] = {}
        self.close_count = 0

    async def subscribe(self, topic: str, **kwargs: Any) -> FakeConsumer:
        consumer = FakeConsumer(topic, **kwargs)
        self.consumers[topic] = consumer
        return consumer

    async def create_producer(self, topic: str, **kwargs: Any) -> FakeProducer:
        assert topic not in self.producers, "producer created twice"
        # Yield like a real broker round trip, so races can show up.
        await asyncio.sleep(0)
        assert topic not in self.producers, "producer created twice"
        producer = FakeProducer(topic, **kwargs)
        self.producers[topic] = producer
        return producer

    @property
    def closed(self) -> bool:
        return self.close_count > 0

    async def close(self) -> None:
        self.close_count += 1


@pytest.fixture(autouse=True)
def fake_client(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Integration tests talk to a real broker.
    if request.node.get_closest_marker("integration") is None:
        monkeypatch.setattr(dispatcher_module, "Client", FakeClient)


async def wait_until(
    predicate: Callable[[], bool],
    timeout: float = 1.0,
) -> None:
    """Yield to the event loop until ``predicate`` holds."""

    async def poll() -> None:
        while not predicate():  # noqa: ASYNC110
            await asyncio.sleep(0.001)

    await asyncio.wait_for(poll(), timeout)
