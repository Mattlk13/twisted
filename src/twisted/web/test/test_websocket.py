from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar
from unittest import skipIf

from twisted.internet.defer import Deferred
from twisted.internet.testing import MemoryReactorClock
from twisted.python.failure import Failure
from twisted.test.iosim import ConnectionCompleter, IOPump
from twisted.trial.unittest import SynchronousTestCase
from twisted.web._responses import BAD_REQUEST
from twisted.web.client import (
    Agent,
    BrowserLikePolicyForHTTPS,
    _StandardEndpointFactory,
    readBody,
)
from twisted.web.iweb import IRequest
from twisted.web.resource import Resource
from twisted.web.server import Site

WSP = TypeVar("WSP", bound="WebSocketProtocol")
shouldSkip = False
try:
    __import__("wsproto")
except ImportError:
    shouldSkip = True
else:
    from twisted.web.websocket import (
        WebSocketClientEndpoint,
        WebSocketClientProtocolFactory,
        WebSocketProtocol,
        WebSocketResource,
        WebSocketServerProtocolFactory,
        WebSocketTransport,
    )

    class MyWSP(WebSocketProtocol):
        wasLost: Failure | None = None

        def makeConnection(self, transport: WebSocketTransport) -> None:
            self.transport = transport

        def connectionLost(self, reason: Failure) -> None:
            self.wasLost = reason

        def bytesMessageReceived(self, data: bytes) -> None:
            if data == b"request":
                self.transport.sendBytesMessage(b"\x00resp\x01onse\xff")
            else:
                self.bDeferred.callback(data)

        def textMessageReceived(self, data: str) -> None:
            if data == "request":
                self.transport.sendTextMessage("response")
            else:
                self.deferred.callback(data)

        def sendRequest(self) -> Deferred[str]:
            """
            Send a text message to the server and expect a response.
            """
            self.deferred: Deferred[str] = Deferred()
            self.transport.sendTextMessage("request")
            return self.deferred

        def bytesRequest(self) -> Deferred[bytes]:
            """
            Send a bytes message to the server and expect a response.
            """
            self.bDeferred: Deferred[bytes] = Deferred()
            self.transport.sendBytesMessage(b"request")
            return self.bDeferred

        def goodbye(self) -> None:
            self.transport.loseConnection()

    class MyFactory(WebSocketServerProtocolFactory[MyWSP]):
        fixture: WebSocketFixture[Any]

        def buildProtocol(self, request: IRequest) -> MyWSP:
            new = MyWSP()
            self.fixture.servers.append(new)
            return new

    class MyClientFactory(WebSocketClientProtocolFactory[MyWSP]):
        def buildProtocol(self, uri: str) -> MyWSP:
            return MyWSP()


@dataclass
class WebSocketFixture(Generic[WSP]):
    clientFactory: WebSocketClientProtocolFactory[WSP] = field()
    reactor: MemoryReactorClock = field(default_factory=MemoryReactorClock)
    resource: Resource = field(default_factory=Resource)
    portNumber: int = 80
    servers: list[WSP] = field(default_factory=list)

    @classmethod
    def new(
        cls, clientFactory: WebSocketClientProtocolFactory[WSP]
    ) -> WebSocketFixture[WSP]:
        self = cls(clientFactory)
        serverFactory = MyFactory()
        serverFactory.fixture = self
        self.resource.putChild(b"connect", WebSocketResource(serverFactory))
        self.reactor.listenTCP(self.portNumber, Site(self.resource))
        return self

    async def connect(self) -> WSP:
        client = WebSocketClientEndpoint(
            # TODO: Oops, _StandardEndpointFactory is not public API
            _StandardEndpointFactory(
                self.reactor, BrowserLikePolicyForHTTPS(), None, None
            ),
            "http://localhost:80/connect",
        )
        return await client.connect(self.clientFactory)

    def complete(self) -> IOPump:
        """
        There should be a single connection in progress; complete it.
        """
        completer = ConnectionCompleter(self.reactor)
        succeeded = completer.succeedOnce()
        assert succeeded is not None, "Connection not in progress."
        return succeeded


@skipIf(shouldSkip, "wsproto library required for websockets")
class WebSocketTests(SynchronousTestCase):
    def test_websocket(self) -> None:
        """
        Connecting to a websocket server (installed with L{WebSocketResource})
        from a websocket client (connected with L{WebSocketClientEndpoint})
        results in a websocket connection.
        """
        fixture = WebSocketFixture.new(MyClientFactory())
        connected = Deferred.fromCoroutine(fixture.connect())
        self.assertNoResult(connected)
        self.assertEqual(len(fixture.reactor.tcpServers), 1)
        self.assertEqual(len(fixture.reactor.tcpClients), 1)
        pump = fixture.complete()
        wsp = self.successResultOf(connected)
        requested = wsp.sendRequest()
        self.assertNoResult(requested)
        pump.flush()
        self.assertEqual(self.successResultOf(requested), "response")

    def test_bytesMessage(self) -> None:
        fixture = WebSocketFixture.new(MyClientFactory())
        connected = Deferred.fromCoroutine(fixture.connect())
        pump = fixture.complete()
        wsp = self.successResultOf(connected)
        bRequested = wsp.bytesRequest()
        self.assertNoResult(bRequested)
        pump.flush()
        self.assertEqual(self.successResultOf(bRequested), b"\x00resp\x01onse\xff")

    def test_serverConnectionLost(self) -> None:
        fixture = WebSocketFixture.new(MyClientFactory())
        connected = Deferred.fromCoroutine(fixture.connect())
        pump = fixture.complete()
        wsp = self.successResultOf(connected)
        self.assertIs(fixture.servers[0].wasLost, None)
        self.assertIs(wsp.wasLost, None)
        wsp.goodbye()
        self.assertIs(fixture.servers[0].wasLost, None)
        self.assertIs(wsp.wasLost, None)
        pump.flush()
        self.assertIsNot(fixture.servers[0].wasLost, None)
        self.assertIsNot(wsp.wasLost, None)

    def test_bad(self) -> None:
        """
        Attempting to issue an C{HTTP GET} against a websocket server
        (installed with L{WebSocketResource}) results in a C{BAD_REQUEST}
        response.
        """
        fixture = WebSocketFixture.new(MyClientFactory())
        agent = Agent(fixture.reactor)
        response = agent.request(b"GET", b"http://localhost/connect")
        self.assertNoResult(response)
        fixture.complete()
        r = self.successResultOf(response)
        self.assertEqual(r.code, BAD_REQUEST)
        body = readBody(r)
        self.assertEqual(
            self.successResultOf(body),
            b"websocket protocol negotiation error",
        )
