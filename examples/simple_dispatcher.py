"""Process three topics, each with its own handler, at equal priority.

Messages are acknowledged when a handler returns and negatively
acknowledged when it raises, so Pulsar redelivers them.
"""

import asyncio
import contextlib
import logging

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


async def process_notifications(message):
    notification = message.data().decode()
    print(f"Processing notification: {notification}")

    await asyncio.sleep(0.1)


async def main():
    processor = PulsarDispatcher(
        url="pulsar://localhost:6650",
        concurrency=20,
    )

    await processor.subscribe(
        topic="orders",
        process_message=process_orders,
        subscription_name="orders-sub",
        consumer_type=pulsar.ConsumerType.Shared,
    )

    await processor.subscribe(
        topic="payments",
        process_message=process_payments,
        subscription_name="payments-sub",
        consumer_type=pulsar.ConsumerType.Shared,
    )

    await processor.subscribe(
        topic="notifications",
        process_message=process_notifications,
        subscription_name="notifications-sub",
        consumer_type=pulsar.ConsumerType.Exclusive,
        negative_ack_redelivery_delay_ms=5000,
    )

    await processor.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
