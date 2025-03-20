from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar
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
        def makeConnection(self, transport: WebSocketTransport) -> None:
            self.transport = transport

        def connectionLost(self, reason: Failure) -> None:
            pass

        def bytesMessageReceived(self, data: bytes) -> None:
            pass

        def textMessageReceived(self, data: str) -> None:
            if data == "request":
                self.transport.sendTextMessage("response")
            else:
                self.deferred.callback(data)

        def sendRequest(self) -> Deferred[str]:
            self.deferred: Deferred[str] = Deferred()
            self.transport.sendTextMessage("request")
            return self.deferred

    class MyFactory(WebSocketServerProtocolFactory[MyWSP]):
        def buildProtocol(self, request: IRequest) -> MyWSP:
            return MyWSP()

    class MyClientFactory(WebSocketClientProtocolFactory[MyWSP]):
        def buildProtocol(self, uri: str) -> MyWSP:
            return MyWSP()


WSP = TypeVar("WSP", bound="WebSocketProtocol")


@dataclass
class WebSocketFixture(Generic[WSP]):
    clientFactory: WebSocketClientProtocolFactory[WSP] = field()
    reactor: MemoryReactorClock = field(default_factory=MemoryReactorClock)
    resource: Resource = field(default_factory=Resource)
    portNumber: int = 80

    @classmethod
    def new(
        cls, clientFactory: WebSocketClientProtocolFactory[WSP]
    ) -> WebSocketFixture[WSP]:
        self = cls(clientFactory)
        self.resource.putChild(b"connect", WebSocketResource(MyFactory()))
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
