# -*- test-case-name: twisted.web.test.test_websocket -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol as TypingProtocol, TypeVar

from zope.interface import implementer

from wsproto import Connection, ConnectionType, WSConnection
from wsproto.events import (
    AcceptConnection,
    BytesMessage,
    CloseConnection,
    Ping,
    Request as WSRequest,
    TextMessage,
)
from wsproto.handshake import H11Handshake

from twisted.internet.interfaces import (
    IAddress,
    IProtocol,
    IProtocolFactory,
    IReactorTCP,
    ITransport,
)
from twisted.internet.protocol import Protocol
from twisted.python.failure import Failure
from twisted.web.client import URI, BrowserLikePolicyForHTTPS, _StandardEndpointFactory
from twisted.web.iweb import IAgentEndpointFactory, IPolicyForHTTPS
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET, Request, Request as TRequest


class WebSocketTransport(TypingProtocol):
    """
    The transport that can send websocket messages.
    """

    def sendTextMessage(self, text: str) -> None:
        """
        Send a text message.
        """

    def sendBytesMessage(self, data: bytes) -> None:
        """
        Send a bytes message.
        """

    def loseConnection(self, code: int = 1000) -> None:
        """
        Drop the websocket connection.
        """


class WebSocketProtocol(TypingProtocol):
    """
    The receiver of websocket messages.
    """

    transport: WebSocketTransport

    def textMessageReceived(self, message: str) -> None:
        """
        A text message was received from the peer.
        """

    def bytesMessageReceived(self, data: bytes) -> None:
        """
        A bytes message was received.
        """

    def connectionLost(self, reason: Failure) -> None:
        """
        The websocket connection was lost.
        """


_WSP = TypeVar("_WSP", covariant=True, bound=WebSocketProtocol)


class WebSocketServerProtocolFactory(TypingProtocol[_WSP]):
    def buildProtocol(self, request: Request) -> _WSP:
        ...


class WebSocketClientProtocolFactory(TypingProtocol[_WSP]):
    def buildProtocol(self, uri: str) -> _WSP:
        ...


@dataclass
class _WebSocketTransport:
    _sktprot: _WebSocketServerProtocol | _WebSocketClientProtocol[WebSocketProtocol]

    def sendTextMessage(self, text: str) -> None:
        t = self._sktprot.transport
        assert t is not None
        t.write(self._sktprot._wsconn.send(TextMessage(text)))

    def sendBytesMessage(self, data: bytes) -> None:
        t = self._sktprot.transport
        assert t is not None
        t.write(self._sktprot._wsconn.send(BytesMessage(data)))

    def loseConnection(self, code: int = 1000, reason: str = "") -> None:
        t = self._sktprot.transport
        assert t is not None
        t.write(self._sktprot._wsconn.send(CloseConnection(code, reason)))
        t.loseConnection()


@dataclass
class WebSocketClientEndpoint:
    endpointFactory: IAgentEndpointFactory
    uri: str

    @classmethod
    def new(
        cls,
        reactor: IReactorTCP,
        uri: str,
        contextFactory: IPolicyForHTTPS = BrowserLikePolicyForHTTPS(),
        connectTimeout: int | None = None,
        bindAddress: bytes | None = None,
    ) -> WebSocketClientEndpoint:
        sef = _StandardEndpointFactory(
            reactor, contextFactory, connectTimeout, bindAddress
        )
        return WebSocketClientEndpoint(sef, uri)

    async def connect(
        self, protocolFactory: WebSocketClientProtocolFactory[_WSP]
    ) -> _WSP:
        print("connecting")
        endpoint = self.endpointFactory.endpointForURI(
            URI.fromBytes(self.uri.encode("utf-8"))
        )
        print("endpoint", endpoint)
        d = endpoint.connect(_WebSocketClientProtocolFactory(self, protocolFactory))
        print("started")
        connected: _WebSocketClientProtocol[_WSP] = await d
        print("connected", connected)
        return connected.wscp


@implementer(IProtocol)
@dataclass
class _WebSocketClientProtocol(Generic[_WSP]):
    uri: str
    wscp: _WSP

    def makeConnection(self, transport: ITransport) -> None:
        self.transport = transport
        self.connectionMade()

    def connectionMade(self) -> None:
        self._wsconn = WSConnection(ConnectionType.CLIENT)
        from hyperlink import parse as parseURL

        h = parseURL(self.uri)
        target = str(h.replace(scheme="", host="", port=None))
        print("target", target)
        self.transport.write(self._wsconn.send(WSRequest(h.host, target)))
        self.wscp.transport = _WebSocketTransport(self)

    def dataReceived(self, data: bytes) -> None:
        _dr(self.transport, self._wsconn, self.wscp, data)

    def connectionLost(self, reason: Failure) -> None:
        self.wscp.connectionLost(reason)


def _dr(
    transport: ITransport,
    wsconn: WSConnection | Connection,
    proto: WebSocketProtocol,
    data: bytes,
) -> None:
    wsconn.receive_data(data)
    for event in wsconn.events():
        if isinstance(event, CloseConnection):
            # TODO: close the connection
            assert transport is not None
            transport.write(wsconn.send(event.response()))
            transport.loseConnection()
        elif isinstance(event, AcceptConnection):
            pass
        elif isinstance(event, TextMessage):
            proto.textMessageReceived(event.data)
        elif isinstance(event, BytesMessage):
            proto.bytesMessageReceived(event.data)
        elif isinstance(event, Ping):
            transport.write(wsconn.send(event.response()))
        else:
            assert False, f"unhandled message type: {event}"


@implementer(IProtocolFactory)
@dataclass
class _WebSocketClientProtocolFactory(Generic[_WSP]):
    endpoint: WebSocketClientEndpoint
    webSocketProtocolFactory: WebSocketClientProtocolFactory[_WSP]

    def doStart(self) -> None:
        pass

    def doStop(self) -> None:
        pass

    def buildProtocol(self, addr: IAddress) -> _WebSocketClientProtocol[_WSP]:
        return _WebSocketClientProtocol(
            self.endpoint.uri,
            self.webSocketProtocolFactory.buildProtocol(self.endpoint.uri),
        )


class _WebSocketServerProtocol(Protocol):
    def __init__(self, connection: Connection, protocol: WebSocketProtocol) -> None:
        self._wsconn = Connection(ConnectionType.SERVER)
        self._wsproto = protocol

    def connectionMade(self) -> None:
        print("websocket connection made")
        t = self.transport
        assert t is not None
        self._wsproto.transport = _WebSocketTransport(self)

    def dataReceived(self, data: bytes) -> None:
        assert self.transport is not None
        _dr(self.transport, self._wsconn, self._wsproto, data)

    def connectionLost(self, reason: Failure | None = None) -> None:
        print("websocket connection lost")


class WebSocketResource(Resource):
    def __init__(
        self, factory: WebSocketServerProtocolFactory[WebSocketProtocol]
    ) -> None:
        super().__init__()
        self.factory = factory

    def render_GET(self, request: TRequest) -> int:
        handshake = H11Handshake(ConnectionType.SERVER)
        headers = []
        for hkey, hvals in request.requestHeaders.getAllRawHeaders():
            for val in hvals:
                headers.append((hkey, val))
        path = request.path
        print("headers", headers)
        handshake.initiate_upgrade_connection(headers, path)
        wsreq = None
        for evt in handshake.events():
            if isinstance(evt, WSRequest):
                wsreq = evt
            else:
                print("not request?", evt)
        assert wsreq is not None, "no request received"
        print("wsreq?", wsreq)
        # for k, v in wsreq.extra_headers:
        #     request.responseHeaders.setRawHeaders(k, [v])
        toSend = handshake.send(AcceptConnection())
        print("toSend?", toSend)
        wscon = handshake.connection
        wsprot = self.factory.buildProtocol(request)
        assert wscon is not None, "connection not accepted"
        t = request.channel.transport
        assert t is not None
        request.channel.upgradeToProtocol(_WebSocketServerProtocol(wscon, wsprot))
        t.write(toSend)
        return NOT_DONE_YET
