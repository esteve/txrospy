#!/usr/bin/env python
from twisted.application import service, internet
from twisted.internet import defer, reactor

from txrospy import protocol_thrift
from beginner_tutorials.thrift_twisted.AddTwoInts import AddTwoInts
import zope


# Run this as a twistd plugin
# E.g. twistd -ny txrospy_server.py

server = 'add_two_ints_server'
ros_service = 'add_two_ints'
ros_rpc_port = 12345
xml_rpc_port = 58260
hostname = 'precise32'
caller_api = 'http://%s:%d/' % (hostname, xml_rpc_port)
rpc_uri = 'thriftrpc://%s:%d' % (hostname, ros_rpc_port)


class AddTwoIntsHandler(object):
  zope.interface.implements(AddTwoInts.Iface)

  def add(self, a, b):
    print "Adding a (%d) and b (%d)" % (a, b)
    response = a + b
    d = defer.Deferred()
    # Simulate a long-running computation
    reactor.callLater(0.1, d.callback, response)
    return d

handler = AddTwoInts.Processor(AddTwoIntsHandler())

application = service.Application('txrospy')
f = protocol_thrift.ThriftROSService(handler, AddTwoInts, server, ros_service,
    rpc_uri, caller_api)
serviceCollection = service.IServiceCollection(application)


def initservice(factory):
    internet.TCPServer(ros_rpc_port, factory).setServiceParent(
        serviceCollection)
f.get_ros_server_factory().addCallback(initservice)
