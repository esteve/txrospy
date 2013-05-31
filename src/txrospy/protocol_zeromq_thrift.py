import os
import rospy
from txrospy import protocol
from thrift.transport import TTwisted, TTransport
from thrift.protocol import TBinaryProtocol
from twisted.internet import defer
import urlparse
from twisted.internet.endpoints import TCP4ClientEndpoint
from txzmq import ZmqFactory, ZmqEndpoint, ZmqREQConnection, ZmqREPConnection
from rosmaster.genpythrift_twisted.MasterAPI import MasterAPI


class TZeroMQTransport(TTwisted.TMessageSenderTransport):
    def __init__(self, socket, iprot_factory):
        TTwisted.TMessageSenderTransport.__init__(self)
        self.socket = socket
        self.iprot_factory = iprot_factory
        self.recv_map = {}

    def gotResponse(self, response):
        tr = TTransport.TMemoryBuffer("".join(response))
        iprot = self.iprot_factory.getProtocol(tr)
        (fname, mtype, rseqid) = iprot.readMessageBegin()

        try:
            method = self.recv_map[fname]
        except KeyError:
            method = getattr(self.client, 'recv_' + fname)
            self.recv_map[fname] = method

        method(iprot, mtype, rseqid)

    def sendMessage(self, message):
        d = self.socket.sendMsg(message)
        d.addCallback(self.gotResponse)


class ZeroMQThriftROSService(object):
    def __init__(self, processor, service_class, name, ros_service, rpc_uri,
        caller_api, ros_master_uri=None):
        if ros_master_uri is None:
            ros_master_uri = os.environ['ROS_MASTER_URI']
            o = urlparse.urlparse(ros_master_uri)
            ros_master_uri = "tcp://%s:%d" % (o.hostname, o.port + 1)

        self.processor = processor
        self.service_class = service_class
        self.resolved_name = rospy.names.resolve_name(ros_service)
        self.caller_id = rospy.names.resolve_name(name)

        self.zeromq_factory = ZmqFactory()
        self.zeromq_master_endpoint = ZmqEndpoint("connect", ros_master_uri)

        self.zeromq_socket = ZmqREQConnection(self.zeromq_factory,
            self.zeromq_master_endpoint)

        self.protocol_factory = TBinaryProtocol.TBinaryProtocolFactory()
        transport = TZeroMQTransport(self.zeromq_socket, self.protocol_factory)
        protocol = self.protocol_factory.getProtocol(transport)
        client = MasterAPI.Client(transport, self.protocol_factory)
        transport.client = client
        self.master = client

        self.rpc_uri = rpc_uri  # local rpc
        o = urlparse.urlparse(self.rpc_uri)

        self.zeromq_uri = "tcp://%s:%d" % (o.hostname, o.port)

        self.caller_api = caller_api  # zeromq+thrift local uri

        self.zeromq_server_endpoint = ZmqEndpoint("bind", self.zeromq_uri)
        self.zeromq_server_socket = ZmqREPConnection(self.zeromq_factory,
            self.zeromq_server_endpoint)
        self.zeromq_server_socket.gotMessage = self.process_message

    @defer.inlineCallbacks
    def initialize(self):
        yield self.register_service()
        defer.returnValue(True)

    def register_service(self):
        '''Register ourselves to the ROS Master.'''
        return self.master.registerService(self.caller_id,
            self.resolved_name, self.rpc_uri, self.caller_api)

    def process_error(self, error):
        print error

    def process_ok(self, _, message_id, tmo):
        msg = tmo.getvalue()

        if len(msg) > 0:
            self.zeromq_server_socket.reply(message_id, msg)

    def process_message(self, message_id, message):
        tmi = TTransport.TMemoryBuffer(message)
        tmo = TTransport.TMemoryBuffer()

        iprot = self.protocol_factory.getProtocol(tmi)
        oprot = self.protocol_factory.getProtocol(tmo)

        d = self.processor.process(iprot, oprot)
        d.addCallbacks(self.process_ok, self.process_error,
            callbackArgs=(message_id, tmo,))


class ZeroMQThriftROSClient(object):

    def __init__(self, service, caller_id, service_class, ros_master_uri=None,
        reactor=None, persistent=False, headers=None):
        self.resolved_name = rospy.names.resolve_name(service)
        self.caller_id = '/' + caller_id
        if ros_master_uri is None:
            ros_master_uri = os.environ['ROS_MASTER_URI']
            o = urlparse.urlparse(ros_master_uri)
            ros_master_uri = "tcp://%s:%d" % (o.hostname, o.port + 1)

        self.zeromq_factory = ZmqFactory()
        self.zeromq_master_endpoint = ZmqEndpoint("connect", ros_master_uri)

        self.zeromq_socket = ZmqREQConnection(self.zeromq_factory,
            self.zeromq_master_endpoint)

        self.protocol_factory = TBinaryProtocol.TBinaryProtocolFactory()
        transport = TZeroMQTransport(self.zeromq_socket, self.protocol_factory)
        protocol = self.protocol_factory.getProtocol(transport)
        client = MasterAPI.Client(transport, self.protocol_factory)
        transport.client = client
        self.master = client
 
        if reactor is None:
            from twisted.internet import reactor
        self.reactor = reactor

        self.headers = headers
        if persistent:
            if not self.headers:
                self.headers = {}
            self.headers['persistent'] = '1'
        self.service_class = service_class

    def lookup_service(self):
        return self.master.lookupService(self.caller_id, self.resolved_name)

    @defer.inlineCallbacks
    def wait_for_endpoint(self):
        '''
        Lookup the service on the ROS Master and probe that the endpoint has
        been registered.

        @return A L{ROSMethodProxy} representing the remote endpoint.
        '''
        (status, message, uri) = yield self.lookup_service()
        status = int(status)
        if status == -1:
            raise Exception("Can't find service")
        o = urlparse.urlparse(uri)

        zeromq_service_uri = "tcp://%s:%d" % (o.hostname, o.port)
        zeromq_service_endpoint = ZmqEndpoint("connect", zeromq_service_uri)

        zeromq_service_socket = ZmqREQConnection(self.zeromq_factory,
            zeromq_service_endpoint)

        transport = TZeroMQTransport(zeromq_service_socket, self.protocol_factory)
        protocol = self.protocol_factory.getProtocol(transport)
        client = self.service_class(transport, self.protocol_factory)
        transport.client = client
        defer.returnValue(client)
