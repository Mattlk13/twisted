from twisted.python.failure import Failure
from twisted.web.iweb import IRequest
from twisted.web.resource import Resource
from twisted.web.static import File
from twisted.web.websocket import (
    WebSocketProtocol,
    WebSocketServerProtocolFactory,
    WebSocketResource,
    WebSocketTransport,
)


class WebSocketEcho(WebSocketProtocol):
    def makeConnection(self, transport: WebSocketTransport) -> None:
        self.transport = transport

    def bytesMessageReceived(self, data: bytes) -> None:
        pass

    def textMessageReceived(self, data: str) -> None:
        self.transport.sendTextMessage(f"reply to {data}")

    def connectionLost(self, reason: Failure) -> None:
        pass

class WebSocketEchoFactory(WebSocketServerProtocolFactory[WebSocketEcho]):
    def buildProtocol(self, request: IRequest) -> WebSocketEcho:
        return WebSocketEcho()

resource = Resource()
resource.putChild(b"webskt", WebSocketResource(WebSocketEchoFactory()))
resource.putChild(b"", File("index.html"))
resource.putChild(b"script.js", File("script.js", "text/plain; charset=utf-8"))
