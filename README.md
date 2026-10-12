# Pulsar Dispatcher

An asyncio-based message processor for Apache Pulsar that supports priority-based scheduling, per-consumer backpressure, configurable concurrency, and asynchronous message handling.

The processor allows multiple Pulsar consumers to share a single asyncio event loop while maintaining independent message queues and processing priorities.

Messages are dispatched in priority order, with round-robin fairness between consumers of equal priority. A configurable concurrency limit prevents excessive simultaneous processing, while bounded queues propagate backpressure to individual Pulsar consumers.

## Features

- **Asynchronous processing:** Uses Python's `asyncio` and the asynchronous Apache Pulsar client.
- **Priority-based scheduling:** Messages from higher-priority consumers are dispatched before those from lower-priority consumers.
- **Per-consumer backpressure:** Each consumer has an independent `asyncio.Queue(maxsize=1)`, preventing unbounded accumulation of messages.
- **Configurable concurrency:** An `asyncio.Semaphore` limits the number of messages being processed simultaneously.
- **Independent message handlers:** Each subscription has its own asynchronous processing callback.
- **Round-robin fairness:** Consumers sharing the same priority are serviced fairly.
- **Automatic acknowledgement:** Messages are acknowledged after successful processing and negatively acknowledged when processing fails.
- **Delayed retries:** Chosen exceptions republish the message to a retry topic after an exponential (or custom) delay.
- **Dead-lettering:** Chosen exceptions send the message straight to the dead letter topic, without waiting for redeliveries.
- **Replies:** A handler's return value can be published to a reply topic.
- **Graceful shutdown:** Stops receiving messages and attempts to finish processing messages already accepted into application queues.
- **Native Pulsar configuration:** Subscription-specific options are passed directly to the Pulsar client.
- **No polling or worker threads:** Uses event-driven scheduling and asyncio tasks throughout.

## Requirements

- Python 3.10 or later
- Apache Pulsar broker
- Apache Pulsar Python client (`pulsar-client`) 3.9.0 or later, installed automatically as a dependency

Install the package:

```bash
pip install pulsar-dispatcher
```

Launch Pulsar broker:

```
docker run -d \
  --name pulsar \
  --mount source=pulsardata,target=/pulsar/data \
  --mount source=pulsarconf,target=/pulsar/conf \
  -p 6650:6650 -p 8080:8080 \
  apachepulsar/pulsar:5.0.0 \
  bin/pulsar standalone
```

The dispatcher uses the client's asynchronous API (`pulsar.asyncio`).

## Quick start

Create a processor, register subscriptions, and start processing:

```python
import asyncio

import pulsar

from pulsar_dispatcher import PulsarDispatcher


async def process_orders(message):
    order = message.data().decode()
    print(f"Processing order: {order}")

    await asyncio.sleep(0.5)


async def process_payments(message):
    payment = message.data().decode()
    print(f"Processing payment: {payment}")

    await asyncio.sleep(0.2)


async def main():
    processor = PulsarDispatcher(
        url="pulsar://localhost:6650",
        concurrency=20,
    )

    await processor.subscribe(
        topic="orders",
        priority=5,
        process_message=process_orders,
        subscription_name="orders-sub",
        consumer_type=pulsar.ConsumerType.Shared,
    )

    await processor.subscribe(
        topic="payments",
        priority=10,
        process_message=process_payments,
        subscription_name="payments-sub",
        consumer_type=pulsar.ConsumerType.Shared,
    )

    await processor.run()


if __name__ == "__main__":
    asyncio.run(main())
```

The processor creates one Pulsar consumer and one bounded application queue for each subscription.

Messages are processed using the callback associated with their originating subscription.

## Examples

Two runnable examples are in [`examples/`](examples/). Both connect to a broker at `pulsar://localhost:6650`.

- [`simple_dispatcher.py`](examples/simple_dispatcher.py) processes orders, payments and notifications, each with its own handler, at equal priority.
- [`priority_dispatcher.py`](examples/priority_dispatcher.py) processes orders across six live and batch topics with `subscribe_multiple_priorities()`, with delayed retries, dead-lettering and order confirmations.

## Architecture

```text
                        Apache Pulsar
                              │
             ┌────────────────┼────────────────┐
             │                │                │
             ▼                ▼                ▼
         Consumer A       Consumer B       Consumer C
         Priority 5       Priority 10      Priority 10
             │                │                │
             ▼                ▼                ▼
         Queue(1)         Queue(1)         Queue(1)
             │                │                │
             └────────────────┼────────────────┘
                              │
                              ▼
                     Priority Dispatcher
                              │
                              ▼
                     Semaphore (N)
                              │
                              ▼
                     asyncio.create_task()
                              │
                 ┌────────────┼────────────┐
                 │            │            │
                 ▼            ▼            ▼
              Handler A    Handler B    Handler C
                 │            │            │
                 └────────────┼────────────┘
                              │
                              ▼
                       Pulsar ACK/NACK
```

The application has three main components, all managed by `PulsarDispatcher`.

### Pulsar readers

Each subscription has a dedicated asynchronous reader coroutine.

The reader receives a message from Pulsar and attempts to insert it into its associated bounded queue.

```python
message = await consumer.receive()
```

If the queue is full, the reader suspends until capacity becomes available.

This prevents the reader from requesting additional messages while its queue remains full, although messages may still be buffered internally by the Pulsar client.

### Priority dispatcher

A single dispatcher retrieves messages from the application queues in priority order.

Before selecting a message, it acquires a processing slot from the concurrency semaphore:

```python
await semaphore.acquire()
source, message = await self._next()
```

This ordering is important.

If all processing slots are occupied, messages remain in their respective priority queues rather than being removed and held by the dispatcher.

When capacity becomes available, the dispatcher selects the highest-priority message currently queued.

### Message processing

Each dispatched message is scheduled as an independent asyncio task:

```python
task = asyncio.create_task(
    self._process(source, message)
)
```

The task invokes the subscription's callback, acknowledges or negatively acknowledges the message, and releases its concurrency slot.

There is no fixed pool of worker tasks. Processing tasks are created as needed and run cooperatively on the application's event loop.

## Priority scheduling

Every subscription is assigned an integer priority, set with the `priority` keyword argument. Subscriptions without one get priority 100.

**Lower numbers represent higher priorities.**

For example:

| Subscription | Priority |
|---|---:|
| Critical events | 5 |
| Orders | 10 |
| Payments | 10 |
| Notifications | 20 |
| Background processing | 40 |

Priorities do not need to be contiguous, and multiple subscriptions may share the same priority.

When messages are available from several subscriptions, the dispatcher selects them in ascending priority order.

### Equal-priority subscriptions

Subscriptions with identical priorities are serviced using round-robin scheduling.

For example, suppose two subscriptions share priority 10:

```text
orders       Priority 10
payments     Priority 10
```

If both queues continually contain messages, the dispatcher alternates between them:

```text
orders
payments
orders
payments
...
```

This prevents one busy subscription from monopolising processing capacity within its priority level.

### Strict priority scheduling

The scheduler uses strict priority ordering.

A message at priority 5 is selected before an available message at priority 10, regardless of how long the priority-10 message has been waiting.

Consequently, **lower-priority subscriptions may experience starvation** if higher-priority subscriptions continuously supply enough messages to occupy all processing capacity.

This behaviour is intentional.

If starvation is undesirable, a future implementation could introduce weighted scheduling, priority ageing, or reserved processing capacity for lower-priority subscriptions.

### Priority does not imply preemption

Priority is evaluated when a message is dispatched.

Once processing begins, the message is not interrupted when a higher-priority message arrives.

For example, if all 20 processing slots are occupied by low-priority messages, a newly arriving high-priority message must wait until a slot becomes available.

It will then be preferred over any lower-priority messages still waiting in the application queues.

## Backpressure

Each Pulsar subscription has its own application-level queue:

```python
asyncio.Queue(maxsize=1)
```

This limits each queue to one waiting message.

Consider a subscription receiving messages A, B, and C:

```text
Message A → Currently being processed
Message B → Waiting in application queue
Message C → Reader waiting for queue capacity
```

The reader cannot enqueue C until B is removed from the queue.

Because the reader awaits queue capacity, it does not continue calling `consumer.receive()` while blocked.

This provides backpressure independently for every Pulsar subscription.

### Backpressure is independent of acknowledgement

Messages are removed from application queues when they are dispatched, not when they are acknowledged.

Therefore, processing a message releases space in its originating queue even if that message has not finished processing.

A single subscription may consequently have multiple unacknowledged messages:

- Messages currently being processed.
- One message waiting in the application queue.
- One message already received but waiting to enter the application queue.
- Messages buffered internally by the Pulsar client.

The concurrency semaphore controls the total number of active processing tasks across all subscriptions.

### Pulsar receiver queue

By default, the processor configures:

```python
receiver_queue_size=1
```

This reduces the Pulsar client's internal prefetch buffer, helping backpressure propagate toward the broker.

The value can be overridden for individual subscriptions:

```python
await processor.subscribe(
    topic="high-throughput",
    priority=10,
    process_message=process_messages,
    subscription_name="high-throughput-sub",
    receiver_queue_size=100,
)
```

Increasing `receiver_queue_size` may improve throughput at the expense of additional client-side buffering.

Note that the receive queue size is not a strict limit on the number of unacknowledged messages.

## Concurrency

The maximum number of simultaneously active processing tasks is configured when creating the processor:

```python
processor = PulsarDispatcher(
    url="pulsar://localhost:6650",
    concurrency=20,
)
```

Internally, concurrency is controlled by:

```python
asyncio.Semaphore(20)
```

A semaphore slot is acquired before dispatching a message and released after its processing task finishes.

This provides a global concurrency limit across every subscription.

### Concurrency versus parallelism

All processing tasks execute on the same asyncio event loop.

Asyncio enables concurrency by switching between tasks when they suspend at an `await` expression.

For example:

```python
async def process_message(message):
    data = await fetch_external_data(message)
    await store_result(data)
    await publish_notification(data)
```

While one task waits for an external operation, other tasks can make progress.

However, asyncio does not automatically execute Python code in parallel across multiple CPU cores.

CPU-intensive processing, blocking libraries, or synchronous I/O can block the event loop and delay unrelated messages.

Such operations should be delegated to an executor or implemented using asynchronous alternatives.

### Choosing a concurrency limit

The appropriate concurrency value depends on the workload.

For I/O-bound tasks, relatively high concurrency may improve throughput.

For CPU-intensive tasks, increasing concurrency may provide little benefit without parallel execution.

Other considerations include:

- Database connection pool capacity.
- External API rate limits.
- Memory consumption per message.
- Pulsar acknowledgement timeouts.
- Acceptable processing latency.

Start with a conservative limit and adjust it based on observed performance.

## Subscription configuration

The `subscribe()` method takes two required arguments:

| Argument | Type | Description |
|---|---|---|
| `topic` | `str` | Pulsar topic to consume |
| `process_message` | Async callable | Callback invoked for each received message |

It also accepts these optional keyword-only arguments; the error-handling and reply options are described under [Error handling](#error-handling) and [Replies](#replies):

| Argument | Type | Description |
|---|---|---|
| `priority` | `int` | Scheduling priority; lower numbers are preferred. Defaults to 100 |
| `fatal_exception_class` | `type[BaseException]` | Send messages whose handler raises this to the dead letter topic |
| `retry_exception_class` | `type[BaseException]` | Republish messages whose handler raises this to `retry_topic` |
| `retry_topic` | `str` | Topic that delayed retries are published to |
| `retry_backoff_function` | `Callable[[int], int]` | Delay in seconds for a given retry number, starting at 1; defaults to `default_backoff` |
| `max_retries` | `int` | Dead-letter messages that are still failing after this many retries |
| `reply_topic` | `str` | Topic that the handler's return value is published to |

All other keyword arguments are passed directly to Pulsar's `Client.subscribe()`.

For example:

```python
await processor.subscribe(
    topic="persistent://public/default/orders",
    priority=10,
    process_message=process_orders,

    # Pulsar subscription options
    subscription_name="order-processing",
    consumer_type=pulsar.ConsumerType.Shared,
    receiver_queue_size=1,
    negative_ack_redelivery_delay_ms=5000,
)
```

Supported options depend on the installed Pulsar client version.

Refer to the [Apache Pulsar Python client API](https://pulsar.apache.org/api/python/) for available parameters.

### Subscribing to multiple priority topics

A common pattern is to split one stream of work across several topics by priority, for example live versus batch traffic, each with high- and low-priority lanes. `subscribe_multiple_priorities()` subscribes one handler to the whole family, naming the topics and assigning priorities from two `IntEnum` classes:

```python
from enum import IntEnum


class PriorityLevel(IntEnum):
    LIVE = 100
    BATCH = 200


class PrioritySubLevel(IntEnum):
    HIGH = 10
    LOW = 20
    RETRY = 50


await processor.subscribe_multiple_priorities(
    "orders",
    process_orders,
    PriorityLevel,
    priority_sublevels=PrioritySubLevel,
    retry_exception_class=ServiceUnavailable,
    retry_priority_sublevel=PrioritySubLevel.RETRY,
    subscription_name="orders-sub",
    consumer_type=pulsar.ConsumerType.Shared,
)
```

This subscribes to six topics. Each name is the prefix followed by the lowercased level and sublevel member names, joined by underscores, and each priority is the level value plus the sublevel value:

| Topic | Priority | Retry topic |
|---|---:|---|
| `orders_live_high` | 110 | `orders_live_retry` |
| `orders_live_low` | 120 | `orders_live_retry` |
| `orders_live_retry` | 150 | `orders_live_retry` |
| `orders_batch_high` | 210 | `orders_batch_retry` |
| `orders_batch_low` | 220 | `orders_batch_retry` |
| `orders_batch_retry` | 250 | `orders_batch_retry` |

Without `priority_sublevels`, there is one topic per level, such as `orders_live` at priority 100 and `orders_batch` at priority 200.

Sublevel values must not be negative, and the largest must be less than the smallest gap between any two level values (100 here), so that every topic in a level outranks every topic in the next level down.

Retries can go to one of two places:

- `retry_priority_sublevel`: within each level, the topic with this sublevel value is the retry topic for every topic in that level, as in the table above. A failed live order is retried on `orders_live_retry`, still ahead of all batch work. Requires `priority_sublevels`.
- `retry_topic`: one retry topic for every topic, at every level. This topic is not subscribed automatically.

Only one of the two can be given. All other arguments, including the error-handling options and Pulsar subscription options, apply to every topic, as for `subscribe()`. Every argument is checked before anything is subscribed, so an invalid call leaves the dispatcher unchanged.

### Client configuration

Additional arguments supplied to the `PulsarDispatcher` constructor are passed to Pulsar's client constructor.

For example:

```python
processor = PulsarDispatcher(
    url="pulsar+ssl://pulsar.example.com:6651",
    concurrency=20,
    authentication=authentication,
    tls_trust_certs_file_path="/etc/ssl/certs/pulsar-ca.pem",
)
```

This allows the processor to use Pulsar authentication, TLS, and other supported client settings without exposing those options individually.

## Message handlers

Each subscription must provide an asynchronous callback accepting a Pulsar message.

```python
async def process_message(message):
    data = message.data()
    await perform_business_logic(data)
```

Handlers should not acknowledge messages themselves.

The processor automatically handles acknowledgements based on whether the callback succeeds.

### Successful processing

If the callback returns normally:

```python
await source.consumer.acknowledge(message)
```

Pulsar is informed that processing has completed successfully.

### Failed processing

If the callback raises an exception, the processor logs the error and attempts to negatively acknowledge the message:

```python
await source.consumer.negative_acknowledge(message)
```

This requests redelivery according to the consumer's configured redelivery behaviour.

For example:

```python
async def process_message(message):
    data = message.data()

    if not await validate(data):
        raise ValueError("Invalid message")

    await save_to_database(data)
```

An exception propagating from `validate()` or `save_to_database()` causes the processor to attempt negative acknowledgement.

## Error handling

By default every handler exception negatively acknowledges the message, so Pulsar redelivers it after `negative_ack_redelivery_delay_ms`, and moves it to the dead letter topic once a configured `dead_letter_policy` runs out of redeliveries.

Not every failure deserves that treatment. A malformed message will never succeed, and a temporarily unavailable dependency may need longer than the redelivery delay. Two optional exception classes change what happens:

```python
class InvalidOrder(Exception):
    """The message can never be processed."""


class ServiceUnavailable(Exception):
    """Processing may succeed later."""


await processor.subscribe(
    topic="orders",
    priority=10,
    process_message=process_orders,
    fatal_exception_class=InvalidOrder,
    retry_exception_class=ServiceUnavailable,
    retry_topic="orders-retry",
    subscription_name="orders-sub",
    consumer_type=pulsar.ConsumerType.Shared,
)
```

Each class also matches its subclasses. If an exception matches both, the fatal class wins. Any other exception is still negatively acknowledged.

In both cases the message is republished with its payload, properties, partition key, ordering key and event time, and is acknowledged on the original topic only once the republish succeeds. If republishing fails, the message is negatively acknowledged instead. Two properties record the message's origin, using the same names as the Java client; they are set only on the first republish, so they always point at the original message:

| Property | Value |
|---|---|
| `REAL_TOPIC` | Topic the message was first consumed from |
| `ORIGIN_MESSAGE_ID` | ID of the original message |

Retried messages also carry `RECONSUMETIMES` and `DEAD_LETTER_TOPIC`, described below.

### Dead-lettering fatal errors

When the handler raises `fatal_exception_class`, the message goes straight to the dead letter topic, however many redeliveries `dead_letter_policy` would otherwise allow.

The dead letter topic is the `dead_letter_topic` of the subscription's `dead_letter_policy`, if set. Otherwise it is Pulsar's default, `<topic>-<subscription_name>-DLQ`.

A message that has been retried is dead-lettered to the original subscription's dead letter topic, not the one belonging to the retry topic's subscription. On its first retry the message is stamped with a `DEAD_LETTER_TOPIC` property naming the original subscription's dead letter topic, and later dead-lettering uses that property wherever the message has got to. So with the setup under [Delayed retries](#delayed-retries), every failed order ends up in `orders-orders-sub-DLQ`, whether it failed on first delivery or after its last retry. A message without the property, such as one published straight to the retry topic, goes to the dead letter topic of the subscription that received it.

The dispatcher publishes to whatever topic `DEAD_LETTER_TOPIC` names, creating a producer for it if needed. Anyone who can publish to your retry topic can therefore choose where its dead letters go, within the topics your client is permitted to write to.

### Delayed retries

When the handler raises `retry_exception_class`, the message is republished to `retry_topic` with a delivery delay, and its `RECONSUMETIMES` property is incremented.

The delay in seconds is `retry_backoff_function(retry)`, where `retry` is the number of this retry: 1 the first time a message is retried, 2 the second, and so on. The default, `default_backoff`, waits 1, 2, 4, 8, ... seconds, up to 5 minutes. Use `exponential_backoff()` to build a different curve, or pass any function:

```python
from pulsar_dispatcher import exponential_backoff

retry_backoff_function=exponential_backoff(initial=5, multiplier=3, maximum=600)
retry_backoff_function=lambda retry: 30
```

The dispatcher does not consume `retry_topic` for you. Subscribe to it like any other topic, usually with the same handler and retry options so that a message can be retried more than once. Giving it a lower priority than the main topic keeps retries from crowding out new work:

```python
for topic, priority in (("orders", 10), ("orders-retry", 20)):
    await processor.subscribe(
        topic=topic,
        priority=priority,
        process_message=process_orders,
        retry_exception_class=ServiceUnavailable,
        retry_topic="orders-retry",
        max_retries=5,
        subscription_name="orders-sub",
        consumer_type=pulsar.ConsumerType.Shared,
    )
```

Pulsar only honours delivery delays on `Shared` and `Key_Shared` subscriptions. On other subscription types, retries are delivered immediately.

When `run()` starts, it emits a `RetryTopicWarning` if:

- a subscription's `retry_topic` is not consumed by any subscription on the same dispatcher, so retried messages would never be processed, or
- a subscription that consumes some subscription's retry topic is not `Shared` or `Key_Shared`, so retry delays would be ignored.

Topic names are compared in full, so `orders-retry` and `persistent://public/default/orders-retry` count as the same topic. These are standard Python warnings: silence them with `warnings.filterwarnings("ignore", category=RetryTopicWarning)` if, for example, another service consumes the retry topic, or turn them into errors with `"error"`.

### Limiting retries

Without `max_retries`, a message is retried for as long as the handler keeps raising `retry_exception_class`. With `max_retries=5`, a message that fails again after its fifth retry is sent to the dead letter topic instead, keeping its `RECONSUMETIMES` property.

`max_retries` needs a dead letter topic, so it requires `subscription_name` or a `dead_letter_policy` with `dead_letter_topic`, just like `fatal_exception_class`.

Like fatal errors, exhausted messages go to the original subscription's dead letter topic (see [Dead-lettering fatal errors](#dead-lettering-fatal-errors)).

## Replies

If `reply_topic` is set, the handler's return value is published to that topic before the message is acknowledged:

```python
async def resize_image(message):
    return make_thumbnail(message.data())


await processor.subscribe(
    topic="images",
    priority=10,
    process_message=resize_image,
    reply_topic="thumbnails",
    subscription_name="resizer",
)
```

The return value must be `bytes`. A handler that returns `None` publishes nothing, and without `reply_topic` the return value is ignored. If publishing the reply fails, the message is negatively acknowledged, so the handler runs again on redelivery.

## Message ordering

The processor does **not** guarantee that messages from the same subscription finish processing in their original delivery order.

For example:

```text
Message A ── Processing starts ─────────────── Completes
Message B ───── Processing starts ── Completes
Message C ─────── Processing starts ───────────────── Completes
```

Although messages may be dispatched in receive order, asynchronous processing allows them to complete out of order.

This is particularly relevant when using multiple concurrent processing tasks.

Applications requiring strict per-subscription or per-message-key ordering must introduce additional coordination or limit concurrency appropriately.

Also consider the ordering semantics of the selected Pulsar subscription type.

## Shutdown behaviour

Shutdown starts when `close()` is called (for example from a signal handler) or when the task running `run()` is cancelled. When `close()` is called, `run()` returns normally once shutdown has finished.

The processor implements a graceful shutdown sequence:

1. Cancel Pulsar reader tasks to stop receiving new messages.
2. Close the application scheduler to further insertions.
3. Continue dispatching messages already accepted into application queues.
4. Allow active processing tasks to complete.
5. Close the Pulsar client. This step runs even if an earlier step fails.

`close()` can be called more than once and from several tasks at the same time; every caller waits for the same shutdown to finish. Cancelling a caller of `close()` does not abort the shutdown in progress.

Do not await `close()` from inside a message handler: shutdown waits for in-flight handlers, so the handler would wait for itself.

Reader tasks start when `run()` starts, not during `subscribe()`. Calling `close()` without calling `run()` therefore leaves every message with Pulsar.

The intention is to finish accepted work rather than discard it immediately.

Messages received but not successfully acknowledged remain subject to Pulsar's redelivery mechanisms.

### Shutdown limitations

The current implementation does not impose a maximum shutdown duration.

If message processing hangs indefinitely, shutdown may also remain incomplete.

Production deployments should consider an explicit shutdown deadline and policies for processing tasks that cannot finish before the application terminates.

Unexpected broker failures still require additional lifecycle handling.

## Delivery guarantees and idempotency

The processor is intended for **at-least-once message processing**, subject to Pulsar's delivery and subscription behaviour.

A message may be delivered more than once.

For example:

1. The handler processes a message successfully.
2. The handler writes its result to a database.
3. The processor attempts to acknowledge the message.
4. The acknowledgement fails because the connection is interrupted.
5. Pulsar subsequently redelivers the message.

The handler may therefore execute again, even though its earlier processing succeeded.

Applications should use idempotent operations wherever practical.

Common approaches include unique message identifiers, database uniqueness constraints, idempotency keys, or transactions that prevent duplicate side effects.

**Successful processing and successful acknowledgement are not inherently atomic operations.**

The same applies to replies, retries and dead-lettering: the message is published to the other topic before the original is acknowledged, so a failed acknowledgement can produce a duplicate reply, retry or dead letter.

## Limitations

The current implementation deliberately favours simplicity.

It does not currently provide:

- Dynamic subscription registration after `run()` starts.
- Dynamic priority changes.
- Per-subscription concurrency limits.
- Strict per-subscription processing order.
- Weighted priority scheduling or starvation prevention.
- Processing deadlines or message timeouts.
- Built-in metrics, tracing, or health checks.
- A configurable maximum graceful-shutdown duration.

These features can be introduced if required without fundamentally changing the architecture.

## Design principles

The processor follows several principles intended to keep the implementation small and predictable.

**One event loop:** Message reception, scheduling, and business logic execute asynchronously without requiring application-managed worker threads.

**Bounded buffering:** Each subscription has a queue capacity of one, making backpressure explicit.

**Priority at dispatch:** The concurrency semaphore is acquired before retrieving the next message, preserving priority selection when processing capacity becomes available.

**Independent consumers:** A slow or backpressured subscription does not directly prevent other readers from receiving messages.

**Application-owned business logic:** The processor manages transport and scheduling while subscription-specific callbacks implement business logic.

**Pulsar-native configuration:** Client and subscription options are forwarded to Pulsar rather than duplicated in the processor's API.

## Further reading

- [Apache Pulsar documentation](https://pulsar.apache.org/docs/)
- [Apache Pulsar Python client API](https://pulsar.apache.org/api/python/)
- [Python asyncio documentation](https://docs.python.org/3/library/asyncio.html)
- [Python asyncio synchronisation primitives](https://docs.python.org/3/library/asyncio-sync.html)
- [Python asyncio queues](https://docs.python.org/3/library/asyncio-queue.html)

---

**Implementation note:** This README describes the current `PulsarDispatcher` implementation and its intended behaviour. Before production deployment, its shutdown, cancellation, and acknowledgement paths should be tested against the exact installed `pulsar-client` version, particularly because async API support and negative-acknowledgement semantics may vary between releases.


## Development

Launch Pulsar development instance:

```
docker run -d \
  --name pulsar \
  --mount source=pulsardata,target=/pulsar/data \
  --mount source=pulsarconf,target=/pulsar/conf \
  -p 6650:6650 -p 8080:8080 \
  apachepulsar/pulsar:5.0.0 \
  bin/pulsar standalone
```

Install the project with its development dependencies:

```bash
pdm install -G test -G lint
```

Run the checks (the unit tests use an in-memory fake client and do not need a broker):

```bash
pdm run test               # unit tests with coverage
pdm run test-integration   # end-to-end tests against the broker above
pdm run lint               # ruff check, ruff format --check, mypy --strict
pdm run fmt                # apply ruff fixes and formatting
```

Integration tests connect to `pulsar://localhost:6650`; set `PULSAR_URL` to use another broker.

To run the linters on every commit:

```bash
pdm run pre-commit install
```
