from twisted.application import service, internet
from twisted.internet import defer, reactor

from txrospy import protocol
from beginner_tutorials.srv import AddTwoInts, AddTwoIntsResponse

# Run this as a twistd plugin
# E.g. twistd -ny txrospy_server.py

server = 'add_two_ints_server'
ros_service = 'add_two_ints'
ros_rpc_port = 12345
xml_rpc_port = 58260
hostname = 'precise32'
caller_api = 'http://%s:%d/' % (hostname, xml_rpc_port)
rpc_uri = 'rosrpc://%s:%d' % (hostname, ros_rpc_port)


def handler(req):
    print "Adding a (%d) and b (%d)" % (req.a, req.b)
    response = AddTwoIntsResponse(req.a + req.b)
    d = defer.Deferred()
    # Simulate a long-running computation
    reactor.callLater(1.0, d.callback, response)
    return d


application = service.Application('txrospy')
f = protocol.ROSService(handler, AddTwoInts, server, ros_service, rpc_uri,
    caller_api)
serviceCollection = service.IServiceCollection(application)


def initservice(factory):
    internet.TCPServer(ros_rpc_port, factory).setServiceParent(
        serviceCollection)
f.get_ros_server_factory().addCallback(initservice)
