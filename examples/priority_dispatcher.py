"""Process orders across six topics, from live/high down to batch/retry.

Topics and priorities come from the two enums below:

    orders_live_high     110    orders_batch_high    210
    orders_live_low      120    orders_batch_low     220
    orders_live_retry    150    orders_batch_retry   250

Each level retries into its own retry topic, so a retried live order
still runs ahead of all batch work. Invalid orders, and orders still
failing after 5 retries, go to the dead letter topic of the topic they
were first published to. Each accepted order is confirmed on the
order-confirmations topic.
"""

import asyncio
import contextlib
import logging
from enum import IntEnum

import pulsar

from pulsar_dispatcher import PulsarDispatcher, exponential_backoff


class PriorityLevel(IntEnum):
    LIVE = 100
    BATCH = 200


class PrioritySubLevel(IntEnum):
    HIGH = 10
    LOW = 20
    RETRY = 50


class InvalidOrder(Exception):
    """The order can never be processed; dead-letter it."""


class InventoryUnavailable(Exception):
    """The order may succeed later; retry it with a delay."""


async def process_orders(message):
    order = message.data().decode()
    print(f"Processing order from {message.topic_name()}: {order}")

    if not order:
        raise InvalidOrder("empty order")

    await asyncio.sleep(0.5)

    # Published to the reply topic.
    return f"accepted: {order}".encode()


async def main():
    processor = PulsarDispatcher(
        url="pulsar://localhost:6650",
        concurrency=20,
    )

    await processor.subscribe_multiple_priorities(
        "orders",
        process_orders,
        PriorityLevel,
        priority_sublevels=PrioritySubLevel,
        fatal_exception_class=InvalidOrder,
        retry_exception_class=InventoryUnavailable,
        retry_priority_sublevel=PrioritySubLevel.RETRY,
        retry_backoff_function=exponential_backoff(maximum=60),
        max_retries=5,
        reply_topic="order-confirmations",
        subscription_name="orders-sub",
        # Retry delays need a Shared or Key_Shared subscription.
        consumer_type=pulsar.ConsumerType.Shared,
    )

    await processor.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
