from pulsar_dispatcher.backoff import (
    BackoffFunction,
    default_backoff,
    exponential_backoff,
)
from pulsar_dispatcher.dispatcher import (
    MessageHandler,
    PulsarDispatcher,
    RetryTopicWarning,
)

try:
    from pulsar_dispatcher.version import __version__
except ImportError:
    __version__ = "0.0.0"
version = __version__

__all__ = [
    "BackoffFunction",
    "MessageHandler",
    "PulsarDispatcher",
    "RetryTopicWarning",
    "__version__",
    "default_backoff",
    "exponential_backoff",
    "version",
]
