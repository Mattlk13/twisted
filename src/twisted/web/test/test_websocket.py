from twisted.internet.defer import Deferred
from twisted.internet.testing import MemoryReactorClock
from twisted.python.failure import Failure
from twisted.test.iosim import ConnectionCompleter
from twisted.trial.unittest import SynchronousTestCase
from twisted.web.client import BrowserLikePolicyForHTTPS, _StandardEndpointFactory
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


class WebSocketTests(SynchronousTestCase):
    def test_websocket(self) -> None:
        mrc = MemoryReactorClock()

        resource = Resource()

        class MyWSP(WebSocketProtocol):
            transport: WebSocketTransport

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
        print("p?", pump)
        wsp = self.successResultOf(connected)
        requested = wsp.sendRequest()
        self.assertNoResult(requested)
        pump.flush()
        self.assertEqual(self.successResultOf(requested), "response")
