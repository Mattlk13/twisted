from twisted.web.websocket import WebSocketResource

from twisted.web.resource import Resource
from twisted.web.static import File

resource = Resource()
resource.putChild(b"webskt", WebSocketResource())
resource.putChild(b"", File("index.html"))
resource.putChild(b"script.js", File("script.js", "text/plain; charset=utf-8"))

