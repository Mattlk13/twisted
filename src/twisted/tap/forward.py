# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Support module forwarding stream connections with C{twist}.
"""
from typing import Any

from twisted.application import strports
from twisted.application.service import IService
from twisted.internet.endpoints import clientFromString
from twisted.protocols.forward import forwarder
from twisted.python import usage


class Options(usage.Options):
    synopsis = "[options]"
    longdesc = "Forwarder that can forward from any stream server to any stream client."
    optParameters = [
        [
            "listen",
            "l",
            "tcp:6667",
            "Server string endpoint description to listen on.",
        ],
        [
            "connect",
            "c",
            "tcp:localhost:6665",
            "Client string endpoint description to connect to.",
        ],
    ]


def makeService(config: Options) -> IService:
    """
    Create a port-forwarding service.
    """
    reactor: Any
    from twisted.internet import reactor

    return strports.service(
        config["listen"],
        forwarder(clientFromString(reactor, config["connect"])),
        reactor,
    )
