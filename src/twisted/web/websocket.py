# -*- test-case-name: twisted.web.test.test_websocket -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Generic, Protocol as TypingProtocol, TypeVar, Union

from zope.interface import implementer

from hyperlink import URL
from wsproto import Connection, ConnectionType, WSConnection
from wsproto.connection import ConnectionState
from wsproto.events import (
    AcceptConnection,
    BytesMessage,
    CloseConnection,
    Ping,
    Pong,
    Request as WSRequest,
    TextMessage,
)
from wsproto.handshake import H11Handshake
from wsproto.utilities import RemoteProtocolError

from twisted.internet.defer import Deferred
from twisted.internet.interfaces import (
    IAddress,
    IProtocol,
    IProtocolFactory,
    IReactorTCP,
    ITransport,
)
from twisted.logger import Logger
from twisted.python.failure import Failure
from twisted.web._responses import BAD_REQUEST
from twisted.web.client import URI, BrowserLikePolicyForHTTPS, _StandardEndpointFactory
from twisted.web.iweb import IAgentEndpointFactory, IPolicyForHTTPS
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET, Request


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

    def ping(self, payload: bytes = b"") -> Deferred[bytes]:
        """
        Send a 'ping' request to measure latency.
        """


class WebSocketProtocol(TypingProtocol):
    """
    The receiver of websocket messages.
    """

    def makeConnection(self, transport: WebSocketTransport) -> None:
        """
        A connection was established.
        """

    def pongReceived(self, payload: bytes) -> None:
        """
        A 'pong' message was received.
        """

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
class _WebSocketTransportImpl:
    _sktprot: _ByteProtocol[WebSocketProtocol]

    def sendTextMessage(self, text: str) -> None:
        t = self._sktprot.transport
        assert t is not None
        t.write(self._sktprot._wsconn.send(TextMessage(text)))

    def sendBytesMessage(self, data: bytes) -> None:
        t = self._sktprot.transport
        assert t is not None
        t.write(self._sktprot._wsconn.send(BytesMessage(data)))

    def ping(self, payload: bytes = b"") -> None:
        t = self._sktprot.transport
        assert t is not None
        t.write(self._sktprot._wsconn.send(Ping(payload)))

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
        endpoint = self.endpointFactory.endpointForURI(
            URI.fromBytes(self.uri.encode("utf-8"))
        )
        d = endpoint.connect(_WebSocketClientProtocolFactory(self, protocolFactory))
        connected: _ByteProtocol[_WSP] = await d
        return connected._wsp


@implementer(IProtocolFactory)
@dataclass
class _WebSocketClientProtocolFactory(Generic[_WSP]):
    endpoint: WebSocketClientEndpoint
    webSocketProtocolFactory: WebSocketClientProtocolFactory[_WSP]

    def doStart(self) -> None:
        ...

    def doStop(self) -> None:
        ...

    def buildProtocol(self, addr: IAddress) -> _ByteProtocol[_WSP]:
        return _ByteProtocol(
            WSConnection(ConnectionType.CLIENT),
            _clientBoot(self.endpoint.uri),
            self.webSocketProtocolFactory.buildProtocol(self.endpoint.uri),
        )


_Bootstrap = Callable[[Union[WSConnection, Connection], ITransport], None]


def _clientBoot(uri: str) -> _Bootstrap:
    def _(wsc: WSConnection | Connection, t: ITransport) -> None:
        h = URL.fromText(uri)
        target = str(h.replace(scheme="", host="", port=None))
        t.write(wsc.send(WSRequest(h.host, target)))

    return _


@implementer(IProtocol)
@dataclass
class _ByteProtocol(Generic[_WSP]):
    _wsconn: WSConnection | Connection
    _bootstrap: _Bootstrap
    _wsp: _WSP

    transport: ITransport = field(init=False)

    def makeConnection(self, transport: ITransport) -> None:
        self.transport = transport
        self.connectionMade()

    def connectionMade(self) -> None:
        self._bootstrap(self._wsconn, self.transport)
        self._wsp.makeConnection(_WebSocketTransportImpl(self))

    def dataReceived(self, data: bytes) -> None:
        self._wsconn.receive_data(data)
        for event in self._wsconn.events():
            if isinstance(event, CloseConnection):
                # TODO: close the connection
                assert self.transport is not None
                if self._wsconn.state != ConnectionState.CLOSED:
                    self.transport.write(self._wsconn.send(event.response()))
                self.transport.loseConnection()
            elif isinstance(event, AcceptConnection):
                pass
            elif isinstance(event, TextMessage):
                self._wsp.textMessageReceived(event.data)
            elif isinstance(event, BytesMessage):
                self._wsp.bytesMessageReceived(event.data)
            elif isinstance(event, Ping):
                self.transport.write(self._wsconn.send(event.response()))
            elif isinstance(event, Pong):
                self._wsp.pongReceived(event.payload)
            else:
                assert False, f"unhandled message type: {event}"

    def connectionLost(self, reason: Failure) -> None:
        self._wsp.connectionLost(reason)


_log = Logger()


def _negotiationError(request: Request) -> bytes:
    request.setResponseCode(BAD_REQUEST)
    request.setHeader("content-type", "text/plain")
    return b"websocket protocol negotiation error"


class WebSocketResource(Resource):
    def __init__(
        self, factory: WebSocketServerProtocolFactory[WebSocketProtocol]
    ) -> None:
        super().__init__()
        self.factory = factory

    def render_GET(self, request: Request) -> bytes | int:
        handshake = H11Handshake(ConnectionType.SERVER)
        raw = request.requestHeaders.getAllRawHeaders()
        simpleHeaders = [(hkey, val) for hkey, hvals in raw for val in hvals]
        try:
            handshake.initiate_upgrade_connection(simpleHeaders, request.path)
        except RemoteProtocolError as rpe:
            _log.error("{request} failed with {rpe}", request=request, rpe=rpe)
            return _negotiationError(request)
        wsprot = self.factory.buildProtocol(request)
        assert wsprot is not None, "connection not accepted by twisted"
        toSend = handshake.send(AcceptConnection())
        wscon = handshake.connection
        t = request.channel.transport
        assert t is not None, "channel transport not connected"
        assert wscon is not None, "connection not accepted by wsproto"
        request.channel.upgradeToProtocol(
            _ByteProtocol(wscon, lambda ign, ign2: None, wsprot)
        )
        t.write(toSend)
        return NOT_DONE_YET
