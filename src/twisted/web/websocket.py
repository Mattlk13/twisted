# -*- test-case-name: twisted.web.test.test_websocket -*-
"""
Websocket (rfc6455) client and server support.

For websocket servers, place a L{WebSocketResource} into your Twisted Web
resource hierarchy.

For websocket clients, call L{WebSocketClientEndpoint.connect}.
"""

from ._websocket_impl import (
    ConnectionRejected,
    WebSocketClientEndpoint,
    WebSocketClientFactory,
    WebSocketProtocol,
    WebSocketResource,
    WebSocketServerFactory,
    WebSocketTransport,
)

__all__ = [
    "ConnectionRejected",
    "WebSocketClientEndpoint",
    "WebSocketClientFactory",
    "WebSocketProtocol",
    "WebSocketResource",
    "WebSocketServerFactory",
    "WebSocketTransport",
]
