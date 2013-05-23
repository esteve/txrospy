from txrospy import protocol
from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol
from twisted.internet import defer
import urlparse
from twisted.internet.endpoints import TCP4ClientEndpoint


class ThriftROSService(protocol.ROSService):
    def build_factory(self):
        return TTwisted.ThriftServerFactory(processor=self.handler,
            iprot_factory=TBinaryProtocol.TBinaryProtocolFactory())


class ThriftROSClient(protocol.ROSClient):
    def build_factory(self):
        return TTwisted.ThriftClientFactory(self.service_class,
            iprot_factory=TBinaryProtocol.TBinaryProtocolFactory())

    @defer.inlineCallbacks
    def wait_for_endpoint(self):
        '''
        Lookup the service on the ROS Master and probe that the endpoint has
        been registered.

        @return A L{ROSMethodProxy} representing the remote endpoint.
        '''
        (status, message, uri) = yield self.lookup_service()
        if status == -1:
            raise Exception("Can't find service")
        o = urlparse.urlparse(uri)
        factory = self.build_factory()
        point = TCP4ClientEndpoint(self.reactor, o.hostname, o.port)
        p = yield point.connect(factory)
        defer.returnValue(p.client)
