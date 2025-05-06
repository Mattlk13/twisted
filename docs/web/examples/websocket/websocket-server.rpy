from twisted.python.failure import Failure
from twisted.web.iweb import IRequest
from twisted.web.resource import Resource
from twisted.web.static import File
from twisted.web.websocket import WebSocketResource, WebSocketTransport
from twisted.internet.task import LoopingCall


class WebSocketDemo:
    loop: LoopingCall | None = None

    def negotiationStarted(self, transport: WebSocketTransport) -> None:
        self.transport = transport
        self.counter = 0

    def negotiationFinished(self) -> None:
        def heartbeat() -> None:
            self.counter += 1
            self.transport.sendTextMessage(f"heartbeat {self.counter}")

        self.loop = LoopingCall(heartbeat)
        self.loop.start(1.0)

    def textMessageReceived(self, data: str) -> None:
        self.transport.sendTextMessage(f"reply to {data}")

    def connectionLost(self, reason: Failure) -> None:
        if self.loop is not None:
            self.loop.stop()

    def bytesMessageReceived(self, data: bytes) -> None: ...
    def pongReceived(self, payload: bytes) -> None: ...


class WebSocketEchoFactory:
    def buildProtocol(self, request: IRequest) -> WebSocketDemo:
        return WebSocketDemo()


resource = WebSocketResource(WebSocketEchoFactory())
