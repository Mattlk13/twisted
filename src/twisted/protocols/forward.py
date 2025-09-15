"""
Generic forwarder from a stream server listener to a stream client.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from zope.interface import implementer

from twisted.internet.defer import Deferred
from twisted.internet.interfaces import (
    IProtocol,
    IProtocolFactory,
    IStreamClientEndpoint,
)
from twisted.internet.protocol import Factory
from twisted.python.failure import Failure


@implementer(IProtocol)
@dataclass
class _ForwardingListener:
    _forwardTo: IStreamClientEndpoint
    _transport: Any = field(init=False)
    _otherTransport: Any = field(init=False)
    factory: Factory[_ForwardingListener] = field(init=False)

    def makeConnection(self, transport: Any) -> None:
        self._transport = transport

        async def _() -> None:
            transport.pauseProducing()
            proto: Any = await self._forwardTo.connect(
                Factory.forProtocol(lambda: _ForwardingConnection(transport))
            )
            self._otherTransport = proto.transport
            proto.transport.registerProducer(transport)
            transport.registerProducer(proto.transport)
            transport.resumeProducing()

        Deferred.fromCoroutine(_())

    def connectionMade(self) -> None:
        ...

    def dataReceived(self, data: bytes) -> None:
        self._transport.write(data)

    def connectionLost(self, reason: Failure) -> None:
        if self._otherTransport is None:
            return
        transport, self._otherTransport = self._otherTransport, None

        transport.loseConnection()
        transport.unregisterProducer()


@implementer(IProtocol)
@dataclass
class _ForwardingConnection:
    _otherTransport: Any
    factory: Factory[_ForwardingConnection] = field(init=False)

    def makeConnection(self, transport: Any) -> None:
        ...

    def connectionMade(self) -> None:
        ...

    def dataReceived(self, data: bytes) -> None:
        self._otherTransport.write(data)

    def connectionLost(self, reason: Failure) -> None:
        if self._otherTransport is None:
            return
        transport, self._otherTransport = self._otherTransport, None

        transport.loseConnection()
        transport.unregisterProducer()


def forwarder(to: IStreamClientEndpoint) -> IProtocolFactory:
    """
    Create a listening protocol factory that will forward its incoming
    connections to the given client endpoint.
    """
    return Factory.forProtocol(lambda: _ForwardingListener(to))
