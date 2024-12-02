from wsproto import Connection, ConnectionType
from wsproto.events import AcceptConnection, Request as WSRequest, TextMessage
from wsproto.handshake import H11Handshake

from twisted.internet.protocol import Protocol
from twisted.python.failure import Failure
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET, Request as TRequest


class WebSocketServerProtocol(Protocol):
    def __init__(self, connection: Connection) -> None:
        self._wsconn = Connection(ConnectionType.SERVER)

    def connectionMade(self) -> None:
        from twisted.internet import reactor
        from twisted.internet.interfaces import IReactorTime

        clock = IReactorTime(reactor)
        print("websocket connection made")
        t = self.transport
        assert t is not None
        clock.callLater(
            2.0,
            lambda: t.write(self._wsconn.send(TextMessage("hello from websockets"))),
        )

    def dataReceived(self, data: bytes) -> None:
        self._wsconn.receive_data(data)
        print("recv")
        for event in self._wsconn.events():
            print(event)

    def connectionLost(self, reason: Failure | None = None) -> None:
        print("websocket connection lost")


class WebSocketResource(Resource):
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
        assert wscon is not None, "connection not accepted"
        t = request.channel.transport
        assert t is not None
        request.channel.upgradeToProtocol(WebSocketServerProtocol(wscon))
        t.write(toSend)
        return NOT_DONE_YET
