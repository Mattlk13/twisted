from twisted.internet.defer import Deferred
from twisted.internet.testing import MemoryReactorClock
from twisted.python.failure import Failure
from twisted.test.iosim import ConnectionCompleter
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


class WebSocketTests(SynchronousTestCase):
    def test_websocket(self) -> None:
        mrc = MemoryReactorClock()
        resource = Resource()
        resource.putChild(b"connect", WebSocketResource(MyFactory()))
        mrc.listenTCP(80, Site(resource))
        client = WebSocketClientEndpoint(
            _StandardEndpointFactory(mrc, BrowserLikePolicyForHTTPS(), None, None),
            "http://localhost:80/connect",
        )
        connected = Deferred.fromCoroutine(client.connect(MyClientFactory()))
        self.assertNoResult(connected)
        self.assertEqual(len(mrc.tcpServers), 1)
        self.assertEqual(len(mrc.tcpClients), 1)
        cc = ConnectionCompleter(mrc)
        pump = cc.succeedOnce()
        wsp = self.successResultOf(connected)
        requested = wsp.sendRequest()
        self.assertNoResult(requested)
        pump.flush()
        self.assertEqual(self.successResultOf(requested), "response")

    def test_bad(self) -> None:
        mrc = MemoryReactorClock()
        resource = Resource()
        resource.putChild(b"connect", WebSocketResource(MyFactory()))
        mrc.listenTCP(80, Site(resource))
        agent = Agent(mrc)
        response = agent.request(b"GET", b"http://localhost/connect")
        self.assertNoResult(response)
        cc = ConnectionCompleter(mrc)
        pump = cc.succeedOnce()
        self.assertIsNot(pump, None)
        r = self.successResultOf(response)
        self.assertEqual(r.code, BAD_REQUEST)
        body = readBody(r)
        self.assertEqual(
            self.successResultOf(body),
            b"websocket protocol negotiation error",
        )
