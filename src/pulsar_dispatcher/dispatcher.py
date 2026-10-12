import asyncio
import functools
import logging
import warnings
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from enum import IntEnum
from itertools import pairwise
from typing import Any

from pulsar import ConsumerType, Message
from pulsar.asyncio import Client, Consumer, Producer

from pulsar_dispatcher.backoff import BackoffFunction, default_backoff

logger = logging.getLogger(__name__)

MessageHandler = Callable[[Message], Awaitable[bytes | None]]
"""Process a message, optionally returning a reply payload."""

# Property names shared with the Java client's retry and DLQ support.
RECONSUME_TIMES = "RECONSUMETIMES"
REAL_TOPIC = "REAL_TOPIC"
ORIGIN_MESSAGE_ID = "ORIGIN_MESSAGE_ID"

# Dead letter topic of the subscription that first retried a message, so
# later failures are dead-lettered there rather than per retry topic.
DEAD_LETTER_TOPIC = "DEAD_LETTER_TOPIC"

# Subscription types on which Pulsar honours delivery delays.
DELAYED_DELIVERY_TYPES = (ConsumerType.Shared, ConsumerType.KeyShared)


class RetryTopicWarning(UserWarning):
    """A retry topic is set up in a way that will not work as expected."""


@dataclass(eq=False)
class Source:
    topic: str
    priority: int
    consumer: Consumer
    process_message: MessageHandler
    consumer_type: Any = ConsumerType.Exclusive
    fatal_exception_class: type[BaseException] | None = None
    dead_letter_topic: str | None = None
    retry_exception_class: type[BaseException] | None = None
    retry_topic: str | None = None
    retry_producer: Producer | None = None
    retry_backoff_function: BackoffFunction = default_backoff
    max_retries: int | None = None
    reply_producer: Producer | None = None
    queue: asyncio.Queue[Message] = field(
        default_factory=lambda: asyncio.Queue(maxsize=1)
    )


class PulsarDispatcher:
    def __init__(
        self,
        url: str,
        concurrency: int = 20,
        **kwargs: Any,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be positive")

        self.client = Client(url, **kwargs)
        self.semaphore = asyncio.Semaphore(concurrency)
        self.condition = asyncio.Condition()

        self.sources: dict[int, deque[Source]] = defaultdict(deque)
        self.producers: dict[str, asyncio.Future[Producer]] = {}
        self.readers: list[asyncio.Task[None]] = []
        self.processing: set[asyncio.Task[None]] = set()
        self.dispatcher: asyncio.Task[None] | None = None
        self.stopped: asyncio.Future[None] | None = None
        self.shutdown: asyncio.Task[None] | None = None

        # Set once readers have stopped; no more messages may be queued.
        self.closed = False

    async def subscribe(
        self,
        topic: str,
        process_message: MessageHandler,
        *,
        priority: int = 100,
        fatal_exception_class: type[BaseException] | None = None,
        retry_exception_class: type[BaseException] | None = None,
        retry_topic: str | None = None,
        retry_backoff_function: BackoffFunction | None = None,
        max_retries: int | None = None,
        reply_topic: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Subscribe to a topic with its own processing callback.

        Messages from subscriptions with a lower ``priority`` number are
        dispatched first; the default is 100.

        If the handler raises ``fatal_exception_class`` (or a subclass),
        the message is republished to the subscription's dead letter
        topic and acknowledged. If it raises ``retry_exception_class``,
        the message is republished to ``retry_topic``, delayed by
        ``retry_backoff_function(retry)`` seconds, where ``retry`` counts
        from 1, and acknowledged. Once ``max_retries`` retries have been
        used, the message is dead-lettered instead. Fatal takes
        precedence when an exception matches both. Any other exception
        negatively acknowledges the message.

        ``retry_topic`` should be consumed by another subscription on this
        dispatcher, using a Shared or Key_Shared subscription so that
        delays apply; run() warns with ``RetryTopicWarning`` otherwise.

        If ``reply_topic`` is set, a non-None value returned by the
        handler is published to it before the message is acknowledged.

        Remaining keyword arguments are passed to Pulsar's subscribe().
        """

        if self.shutdown is not None:
            raise RuntimeError("Dispatcher is closed")

        if self.dispatcher is not None:
            raise RuntimeError("Cannot subscribe after run() starts")

        for name, value in (
            ("fatal_exception_class", fatal_exception_class),
            ("retry_exception_class", retry_exception_class),
        ):
            if value is not None and not (
                isinstance(value, type) and issubclass(value, BaseException)
            ):
                raise TypeError(f"{name} must be an exception class")

        if (retry_exception_class is None) != (retry_topic is None):
            raise ValueError(
                "retry_exception_class and retry_topic must be used together"
            )

        if retry_backoff_function is not None and retry_topic is None:
            raise ValueError("retry_backoff_function requires retry_topic")

        if max_retries is not None:
            if retry_topic is None:
                raise ValueError("max_retries requires retry_topic")

            if max_retries < 1:
                raise ValueError("max_retries must be positive")

        kwargs.setdefault("receiver_queue_size", 1)

        dead_letter_topic = _dead_letter_topic(topic, kwargs)

        # Create producers first so a failure leaves no orphaned consumer.
        if fatal_exception_class is not None or max_retries is not None:
            if dead_letter_topic is None:
                raise ValueError(
                    "fatal_exception_class and max_retries require "
                    "subscription_name or a dead_letter_policy with "
                    "dead_letter_topic"
                )

            await self._producer(dead_letter_topic)

        retry_producer = None
        if retry_topic is not None:
            retry_producer = await self._producer(retry_topic)

        reply_producer = None
        if reply_topic is not None:
            reply_producer = await self._producer(reply_topic)

        consumer = await self.client.subscribe(
            topic=topic,
            **kwargs,
        )

        source = Source(
            topic=topic,
            priority=priority,
            consumer=consumer,
            process_message=process_message,
            consumer_type=kwargs.get("consumer_type", ConsumerType.Exclusive),
            fatal_exception_class=fatal_exception_class,
            dead_letter_topic=dead_letter_topic,
            retry_exception_class=retry_exception_class,
            retry_topic=retry_topic,
            retry_producer=retry_producer,
            retry_backoff_function=(retry_backoff_function or default_backoff),
            max_retries=max_retries,
            reply_producer=reply_producer,
        )

        self.sources[priority].append(source)

    async def subscribe_multiple_priorities(
        self,
        topic_prefix: str,
        process_message: MessageHandler,
        priority_levels: type[IntEnum],
        *,
        priority_sublevels: type[IntEnum] | None = None,
        fatal_exception_class: type[BaseException] | None = None,
        retry_exception_class: type[BaseException] | None = None,
        retry_topic: str | None = None,
        retry_priority_sublevel: int | None = None,
        retry_backoff_function: BackoffFunction | None = None,
        max_retries: int | None = None,
        reply_topic: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Subscribe one handler to a family of topics, one per priority.

        Without ``priority_sublevels``, each member of ``priority_levels``
        gets the topic ``<topic_prefix>_<level>`` at that level's
        priority. With them, each level and sublevel pair gets the topic
        ``<topic_prefix>_<level>_<sublevel>`` at priority level +
        sublevel. Member names are lowercased in topic names.

        Sublevels must be non-negative and smaller than the gap between
        any two levels, so each level's topics stay in their own band.

        Retries go to ``retry_topic`` for every level, or, with
        ``retry_priority_sublevel``, to the topic with that sublevel in
        the same level. Other arguments are as for subscribe().
        """

        if retry_topic is not None and retry_priority_sublevel is not None:
            raise ValueError(
                "Only one of retry_topic or retry_priority_sublevel can be "
                "specified"
            )

        levels = list(priority_levels)
        if not levels:
            raise ValueError("priority_levels must have at least one member")

        # (topic, priority, retry topic) for each subscription.
        plan: list[tuple[str, int, str | None]] = []

        if priority_sublevels is None:
            if retry_priority_sublevel is not None:
                raise ValueError(
                    "retry_priority_sublevel requires priority_sublevels"
                )

            for level in levels:
                plan.append(
                    (
                        _priority_topic(topic_prefix, level),
                        level.value,
                        retry_topic,
                    )
                )

        else:
            sublevels = list(priority_sublevels)
            _check_sublevels(levels, sublevels)

            retry_sublevel = None
            if retry_priority_sublevel is not None:
                if retry_exception_class is None:
                    raise ValueError(
                        "retry_priority_sublevel requires "
                        "retry_exception_class"
                    )

                if retry_priority_sublevel not in {s.value for s in sublevels}:
                    raise ValueError(
                        f"retry_priority_sublevel {retry_priority_sublevel} "
                        f"is not a value of {priority_sublevels.__name__}"
                    )

                retry_sublevel = priority_sublevels(retry_priority_sublevel)

            for level in levels:
                level_retry_topic = retry_topic
                if retry_sublevel is not None:
                    level_retry_topic = _priority_topic(
                        topic_prefix, level, retry_sublevel
                    )

                for sublevel in sublevels:
                    plan.append(
                        (
                            _priority_topic(topic_prefix, level, sublevel),
                            level.value + sublevel.value,
                            level_retry_topic,
                        )
                    )

        topics = [topic for topic, _, _ in plan]
        if len(set(topics)) != len(topics):
            raise ValueError(
                "Priority member names must stay distinct when lowercased"
            )

        for topic, priority, topic_retry_topic in plan:
            await self.subscribe(
                topic,
                process_message,
                priority=priority,
                fatal_exception_class=fatal_exception_class,
                retry_exception_class=retry_exception_class,
                retry_topic=topic_retry_topic,
                retry_backoff_function=retry_backoff_function,
                max_retries=max_retries,
                reply_topic=reply_topic,
                **kwargs,
            )

    async def _producer(self, topic: str) -> Producer:
        """Return a producer for ``topic``, shared between sources.

        Dead letter topics named by messages are only known while
        processing, so concurrent callers share one pending creation.
        """

        pending = self.producers.get(topic)

        if pending is None:
            # Every send is awaited individually, so batching would only
            # add latency. It also keeps delayed delivery per message.
            pending = asyncio.ensure_future(
                self.client.create_producer(
                    topic,
                    batching_enabled=False,
                )
            )
            self.producers[topic] = pending

        try:
            return await asyncio.shield(pending)
        except Exception:
            # Let a later caller try again.
            if self.producers.get(topic) is pending:
                del self.producers[topic]
            raise

    async def _dead_letter(self, source: Source, message: Message) -> None:
        """Republish to the DLQ of the subscription that first saw it."""

        topic = message.properties().get(DEAD_LETTER_TOPIC)
        topic = topic or source.dead_letter_topic
        assert topic is not None

        await _republish(await self._producer(topic), message)

    async def _receive(self, source: Source) -> None:
        """Receive messages with per-consumer backpressure."""

        while True:
            message = await source.consumer.receive()

            try:
                async with self.condition:
                    while source.queue.full() and not self.closed:
                        await self.condition.wait()

                    if self.closed:
                        raise asyncio.CancelledError()

                    source.queue.put_nowait(message)

                    # Wake the dispatcher.
                    self.condition.notify()

            except BaseException:
                # The message was received but not queued.
                try:
                    await source.consumer.negative_acknowledge(message)
                except Exception:
                    logger.exception(
                        "Failed to nack unqueued message from %s",
                        source.topic,
                    )
                raise

    async def _next(self) -> tuple[Source, Message]:
        """Retrieve the highest-priority available message."""

        async with self.condition:
            while True:
                for priority in sorted(self.sources):
                    group = self.sources[priority]

                    for _ in range(len(group)):
                        source = group[0]
                        group.rotate(-1)

                        try:
                            message = source.queue.get_nowait()
                        except asyncio.QueueEmpty:
                            continue

                        # Wake readers waiting for queue capacity.
                        self.condition.notify_all()

                        return source, message

                if self.closed:
                    raise StopAsyncIteration()

                await self.condition.wait()

    async def _process(self, source: Source, message: Message) -> None:
        """Invoke the source-specific callback and acknowledge."""

        try:
            await self._handle(source, message)

        except Exception:
            logger.exception(
                "Processing failed: %s",
                source.topic,
            )

            try:
                await source.consumer.negative_acknowledge(message)
            except Exception:
                logger.exception(
                    "Failed to nack message from %s",
                    source.topic,
                )

    def _finish(self, source: Source, task: "asyncio.Task[None]") -> None:
        """Release a processing task's slot, however the task ended."""

        source.queue.task_done()
        self.semaphore.release()

    async def _handle(self, source: Source, message: Message) -> None:
        """Run the handler, then reply, retry or dead-letter, and ack.

        Raises if the message should be negatively acknowledged.
        """

        try:
            result = await source.process_message(message)

        except BaseException as exc:
            if source.fatal_exception_class is not None and isinstance(
                exc, source.fatal_exception_class
            ):
                logger.info(
                    "Fatal error, dead-lettering message from %s",
                    source.topic,
                    exc_info=exc,
                )

                await self._dead_letter(source, message)

            elif source.retry_exception_class is not None and isinstance(
                exc, source.retry_exception_class
            ):
                await self._retry(source, message, exc)

            else:
                raise

        else:
            if source.reply_producer is not None and result is not None:
                await source.reply_producer.send(result)

        await source.consumer.acknowledge(message)

    async def _retry(
        self,
        source: Source,
        message: Message,
        exc: BaseException,
    ) -> None:
        """Republish to the retry topic, or dead-letter if out of retries."""

        retry = _retry_count(message) + 1

        if source.max_retries is not None and retry > source.max_retries:
            logger.info(
                "Retries exhausted, dead-lettering message from %s "
                "after %d retries",
                source.topic,
                retry - 1,
                exc_info=exc,
            )

            await self._dead_letter(source, message)
            return

        assert source.retry_producer is not None

        delay = max(source.retry_backoff_function(retry), 0)

        logger.info(
            "Retrying message from %s in %ds (retry %d): %r",
            source.topic,
            delay,
            retry,
            exc,
        )

        properties = {RECONSUME_TIMES: str(retry)}

        # Only the original subscription sets this; later retries keep it.
        if (
            DEAD_LETTER_TOPIC not in message.properties()
            and source.dead_letter_topic is not None
        ):
            properties[DEAD_LETTER_TOPIC] = source.dead_letter_topic

        await _republish(
            source.retry_producer,
            message,
            properties=properties,
            delay=delay,
        )

    async def _dispatch(self) -> None:
        """Schedule messages in priority order with bounded concurrency."""

        while True:
            # Reserve capacity before selecting a message.
            await self.semaphore.acquire()

            try:
                source, message = await self._next()
            except StopAsyncIteration:
                self.semaphore.release()
                return
            except BaseException:
                self.semaphore.release()
                raise

            task = asyncio.create_task(
                self._process(source, message),
                name=f"process-{source.topic}",
            )

            # The task now owns the slot. A done callback runs however the
            # task ends, even if it is cancelled before it starts, which a
            # finally block inside _process() would not.
            task.add_done_callback(functools.partial(self._finish, source))

            self.processing.add(task)
            task.add_done_callback(self.processing.discard)

    async def run(self) -> None:
        """Run until close() is called, cancelled, or a task exits."""

        if self.shutdown is not None:
            raise RuntimeError("Dispatcher is closed")

        if self.dispatcher is not None:
            raise RuntimeError("Dispatcher is already running")

        self._check_retry_topics()

        self.stopped = asyncio.get_running_loop().create_future()

        # Readers start here, not in subscribe(), so no messages are
        # received until something is ready to dispatch them.
        for group in self.sources.values():
            for source in group:
                self.readers.append(
                    asyncio.create_task(
                        self._receive(source),
                        name=f"receive-{source.topic}",
                    )
                )

        self.dispatcher = asyncio.create_task(
            self._dispatch(),
            name="pulsar-dispatcher",
        )

        try:
            done, _ = await asyncio.wait(
                [self.stopped, self.dispatcher, *self.readers],
                return_when=asyncio.FIRST_COMPLETED,
            )

            # close() cancels the readers, so check for a requested
            # stop before treating finished tasks as failures.
            if self.stopped.done():
                return

            for task in done:
                assert isinstance(task, asyncio.Task)
                task.result()
                raise RuntimeError(
                    f"Task exited unexpectedly: {task.get_name()}"
                )

        finally:
            await self.close()

    def _check_retry_topics(self) -> None:
        """Warn about retry topics that will not work as expected."""

        sources = [s for group in self.sources.values() for s in group]
        consumed = {_full_topic_name(source.topic) for source in sources}
        retry_topics = {
            _full_topic_name(source.retry_topic)
            for source in sources
            if source.retry_topic is not None
        }

        for source in sources:
            if source.retry_topic is not None and (
                _full_topic_name(source.retry_topic) not in consumed
            ):
                warnings.warn(
                    f"Subscription to {source.topic!r} retries to "
                    f"{source.retry_topic!r}, but no subscription on this "
                    "dispatcher consumes that topic",
                    RetryTopicWarning,
                    stacklevel=3,
                )

            if _full_topic_name(source.topic) in retry_topics and (
                source.consumer_type not in DELAYED_DELIVERY_TYPES
            ):
                warnings.warn(
                    f"Retry topic {source.topic!r} is consumed with "
                    f"{source.consumer_type}, so retry delays are ignored; "
                    "use ConsumerType.Shared or ConsumerType.KeyShared",
                    RetryTopicWarning,
                    stacklevel=3,
                )

    async def close(self) -> None:
        """Stop receiving, drain queued work, and close Pulsar.

        Safe to call more than once, and from another task while run()
        is active; every caller waits for the same shutdown to finish.
        """

        if self.shutdown is None:
            if self.stopped is not None and not self.stopped.done():
                self.stopped.set_result(None)

            self.shutdown = asyncio.create_task(
                self._shutdown(),
                name="pulsar-dispatcher-shutdown",
            )

        # Shield so a cancelled caller cannot abandon a half-done shutdown.
        await asyncio.shield(self.shutdown)

    async def _shutdown(self) -> None:
        try:
            # Stop receiving new messages.
            for task in self.readers:
                task.cancel()

            await asyncio.gather(
                *self.readers,
                return_exceptions=True,
            )

            # Prevent additional insertions.
            async with self.condition:
                self.closed = True
                self.condition.notify_all()

            # Drain queued messages through the dispatcher.
            if self.dispatcher is not None:
                await self.dispatcher

        finally:
            # Always wait for in-flight work and release the client,
            # even if the dispatcher failed.
            await asyncio.gather(
                *self.processing,
                return_exceptions=True,
            )

            await self.client.close()


def _priority_topic(prefix: str, *members: IntEnum) -> str:
    """Name a topic from a prefix and lowercased enum member names."""

    return "_".join([prefix, *(member.name.lower() for member in members)])


def _check_sublevels(levels: list[IntEnum], sublevels: list[IntEnum]) -> None:
    """Check that sublevels keep each priority level's topics separate."""

    if not sublevels:
        raise ValueError("priority_sublevels must have at least one member")

    if min(sublevels) < 0:
        raise ValueError("priority_sublevels must not be negative")

    values = sorted({level.value for level in levels})

    if len(values) > 1:
        gap = min(b - a for a, b in pairwise(values))
        largest = max(sublevels)

        if largest >= gap:
            raise ValueError(
                f"Largest priority sublevel {largest.name}={largest.value} "
                f"must be less than the smallest gap between priority "
                f"levels ({gap})"
            )


def _dead_letter_topic(topic: str, kwargs: dict[str, Any]) -> str | None:
    """Resolve the DLQ topic the way the Pulsar client does, if possible."""

    policy = kwargs.get("dead_letter_policy")

    if policy is not None and policy.dead_letter_topic:
        return str(policy.dead_letter_topic)

    subscription_name = kwargs.get("subscription_name")

    if not subscription_name:
        return None

    return f"{topic}-{subscription_name}-DLQ"


def _full_topic_name(topic: str) -> str:
    """Expand a short topic name the way the Pulsar client does."""

    if "://" in topic:
        return topic

    if topic.count("/") == 2:
        return f"persistent://{topic}"

    return f"persistent://public/default/{topic}"


def _retry_count(message: Message) -> int:
    """Return how many times ``message`` has been retried so far."""

    try:
        return max(int(message.properties().get(RECONSUME_TIMES, 0)), 0)
    except (TypeError, ValueError):
        return 0


async def _republish(
    producer: Producer,
    message: Message,
    properties: dict[str, str] | None = None,
    delay: int = 0,
) -> None:
    """Copy ``message`` to another topic, keeping keys and properties."""

    merged = dict(message.properties())

    # Keep the first topic and ID when a message is republished again.
    merged.setdefault(REAL_TOPIC, message.topic_name())
    merged.setdefault(ORIGIN_MESSAGE_ID, str(message.message_id()))
    merged.update(properties or {})

    await producer.send(
        message.data(),
        properties=merged,
        partition_key=message.partition_key() or None,
        ordering_key=message.ordering_key() or None,
        event_timestamp=message.event_timestamp() or None,
        deliver_after=timedelta(seconds=delay) if delay else None,
    )
