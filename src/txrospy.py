from cStringIO import StringIO
import os
import urlparse

import struct
from struct import calcsize

from twisted.protocols import basic
from twisted.internet import protocol, defer
from twisted.application import service
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.web import xmlrpc, server


import rosgraph
import rospy
from rospy.impl import tcpros_service

from twisted.internet import reactor


class State(object):
    '''Enum to represent the different states in each ROS protocol'''
    WAIT = object()
    INIT = object()
    RESPONSE = object()
    PUBLISH = object()


class ROSClientProtocol(basic.IntNStringReceiver):
    '''A L{Protocol} for the client-side TCPROS protocol.'''

    structFormat = "<I"
    prefixLength = calcsize(structFormat)

    state = State.WAIT

    def __init__(self):
        self.queue = defer.DeferredQueue()

    def dataReceived(self, data):
        if self.state == State.WAIT:
            basic.IntNStringReceiver.dataReceived(self, data)
        elif self.state == State.RESPONSE:
            # Chop the first byte TODO: check that it's a valid response
            data = data[1:]
            self.state = State.WAIT
            basic.IntNStringReceiver.dataReceived(self, data)

    def stringReceived(self, message):
        self.queue.put(message)

    def send_header(self, header):
        '''Encode the headers and send them to the underlying transport.

        @param header: A C{dict} of key-value pairs of strings.'''
        out = rosgraph.network.encode_ros_handshake_header(header)
        self.transport.write(out)

    def send_request(self, request):
        '''Encode a request and send it.

        @param request: A generated representation of a request.'''
        s = StringIO()
        request.serialize(s)
        v = s.getvalue()
        self.sendString(v)
        self.state = State.RESPONSE

    def send_response(self, response):
        pass


class ROSServerProtocol(basic.IntNStringReceiver):
    '''A L{Protocol} for the server-side TCPROS protocol.'''

    structFormat = "<I"
    prefixLength = calcsize(structFormat)

    state = State.INIT

    def stringReceived(self, message):
        if self.state == State.INIT:
            header = {'service': self.factory.resolved_name,
                'type': self.factory.service_class._type,
                'md5sum': self.factory.service_class._md5sum,
                'callerid': self.factory.caller_id}
            data = rosgraph.network.encode_ros_handshake_header(header)
            self.transport.write(data)

            self.state = State.WAIT
        elif self.state == State.WAIT:
            request = self.factory.service_class._request_class()
            request.deserialize(message)
            d = defer.maybeDeferred(self.factory.handler, request)
            d.addCallback(self._write_response)

    def _write_response(self, response):
        s = StringIO()
        response.serialize(s)
        v = s.getvalue()
        out = struct.pack('<B', 1) + struct.pack(self.structFormat,
            len(v)) + v
        self.transport.write(out)


class ROSClientFactory(protocol.ClientFactory):
    '''A C{ClientFactory} that builds TCPROS client-side protocol instances.'''

    protocol = ROSClientProtocol


class ROSMethodProxy(object):
    '''Proxy to a remote service method.

    @ivar service_class: The generated class that exposes our service.
    @ivar protocol: A L{TCPROSServiceClient} representing the underlying
        transport.
    @ivar hostname: The C{str} remote hostname.
    @ivar port: The C{int} remote port.
    '''

    factory = ROSClientFactory

    def __init__(self, service_class, protocol, hostname, port,
        reactor=None):
        self.service_class = service_class
        self.protocol = protocol
        self.hostname = hostname
        self.port = port

        if reactor:
            self.reactor = reactor
        else:
            from twisted.internet import reactor
            self.reactor = reactor

    @defer.inlineCallbacks
    def call(self, *params):
        '''Encode the method params and issue a request to the service.'''
        point = TCP4ClientEndpoint(self.reactor, self.hostname, self.port)
        factory = self.factory()
        p = yield point.connect(factory)

        header = self.protocol.get_header_fields()
        data = rosgraph.network.encode_ros_handshake_header(header)
        p.transport.write(data)

        message = yield p.queue.get()
        # TODO validate message

        request = self.service_class._request_class(*params)
        p.send_request(request)
        message = yield p.queue.get()

        response = self.service_class._response_class()
        response.deserialize(message)

        defer.returnValue(response)

    def __call__(self, *args, **kwargs):
        return self.call(*args, **kwargs)


class ROSClient(object):
    '''
    A high-level object representing a ROS Client. Manages registration to
    the ROS Master and talking to a ROS Service.

    @ivar service_class: The generated class that exposes our service.
    @ivar resolved_name: The C{str} with which we use to identify our hostname.
    @ivar caller_id: The C{str} to identify our calls to ROS Master and the
        peers. 
    @ivar master: A L{Proxy} to the ROS Master.
    @ivar caller_api: The local XML-RPC endpoint.
    '''

    method_proxy = ROSMethodProxy
    factory = ROSClientFactory

    def __init__(self, service, caller_id, service_class, ros_master_uri=None,
        reactor=None, persistent=False, headers=None):
        self.resolved_name = rospy.names.resolve_name(service)
        self.caller_id = '/' + caller_id
        if ros_master_uri is None:
            ros_master_uri = os.environ['ROS_MASTER_URI']

        self.master = xmlrpc.Proxy(ros_master_uri)

        if reactor is None:
            from twisted.internet import reactor
        self.reactor = reactor

        self.headers = headers
        if persistent:
            if not self.headers:
                self.headers = {}
            self.headers['persistent'] = '1'
        self.service_class = service_class

    @defer.inlineCallbacks
    def probe(self, scheme, hostname, port):
        '''Check that the endpoint is reponding to our requests.

        @param scheme: The C{str} protocol scheme for talking to the endpoint.
        @param hostname: The C{str} hostname of the remote endpoint.
        @param port: The C{int} port to talk to the endpoint.

        @return A L{ROSMethodProxy} for issuing calls to the service.
        '''
        header = {'probe': '1', 'md5sum': '*', 'callerid': self.caller_id,
            'service': self.resolved_name}
        point = self.build_endpoint(hostname, port)
        factory = self.factory()
        p = yield point.connect(factory)
        p.send_header(header)
        # TODO: check response
        response = yield p.queue.get()

        protocol = tcpros_service.TCPROSServiceClient(self.resolved_name,
            self.service_class, headers=self.headers)
        method = self.method_proxy(self.service_class, protocol, hostname,
            port)
        defer.returnValue(method)

    def build_endpoint(self, hostname, port):
        '''Factory method to build a low level transport endpoint'''
        return TCP4ClientEndpoint(self.reactor, hostname, port)

    def lookup_service(self):
        '''Check if the service has been registered on the ROS Master'''
        return self.master.callRemote('lookupService', self.caller_id,
            self.resolved_name)

    def getParam(self, param):
        '''Query the ROS Master for a parameter

        @param param: The C{str} parameter to query.
        '''
        param = '/' + param
        return self.master.callRemote('getParam', self.caller_id, param)

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
        # TODO: check response
        response = yield self.getParam('tcp_keepalive')

        endpoint = yield self.probe(o.scheme, o.hostname, o.port)
        defer.returnValue(endpoint)


class ROSServerFactory(protocol.ServerFactory):
    '''A TCPROS server factory

    @ivar handler: A C{callable} that will be fired when an incoming request
        is received.
    @ivar service_class: The generated class that exposes our service.
    @ivar resolved_name: The C{str} with which we use to identify our hostname.
    @ivar caller_id: The C{str} to identify our calls to ROS Master and the
        peers. 
    '''

    protocol = ROSServerProtocol

    def __init__(self, handler, service_class, resolved_name, caller_id):
        self.handler = handler
        self.service_class = service_class
        self.resolved_name = resolved_name
        self.caller_id = caller_id


class ROSService(service.Service):
    '''A Twisted L{Service} suitable for using as a twistd plugin.

    @ivar handler: A C{callable} that will be fired when an incoming request
        is received.
    @ivar service_class: The generated class that exposes our service.
    @ivar resolved_name: The C{str} with which we use to identify our hostname.
    @ivar caller_id: The C{str} to identify our calls to ROS Master and the
        peers. 
    @ivar master: A L{Proxy} to the ROS Master.
    @ivar caller_api: The local XML-RPC endpoint.
    '''

    factory = ROSServerFactory

    def __init__(self, handler, service_class, name, ros_service, rpc_uri,
        caller_api, ros_master_uri=None):
        if ros_master_uri is None:
            ros_master_uri = os.environ['ROS_MASTER_URI']
        self.handler = handler
        self.service_class = service_class
        self.resolved_name = rospy.names.resolve_name(ros_service)
        self.caller_id = rospy.names.resolve_name(name)

        from twisted.web import xmlrpc
        self.master = xmlrpc.Proxy(ros_master_uri)

        self.rpc_uri = rpc_uri  # local rpc
        self.caller_api = caller_api  # xmlrpc local uri

    @defer.inlineCallbacks
    def get_ros_server_factory(self):
        '''
        Register ourselves to the ROS Master and return a suitable
        L{ServerFactory}.
        '''
        yield self.register_service()
        factory = self.factory(self.handler, self.service_class,
            self.resolved_name, self.caller_id)
        defer.returnValue(factory)

    def register_service(self):
        '''Register ourselves to the ROS Master.'''
        return self.master.callRemote('registerService', self.caller_id,
            self.resolved_name, self.rpc_uri, self.caller_api)


class ROSSubscriber(service.Service):

    def __init__(self, name, handler, topic, topic_type, caller_api,
        ros_master_uri=None):
        if ros_master_uri is None:
            ros_master_uri = os.environ['ROS_MASTER_URI']
        self.handler = handler  # to be called when a new message comes in
        self.caller_id = rospy.names.resolve_name(name)

        from twisted.web import xmlrpc
        self.master = xmlrpc.Proxy(ros_master_uri)

        self.topic = topic
        self.topic_type = topic_type

        self.caller_api = caller_api  # local xml uri

    def register_subscriber(self):
        return self.master.callRemote('registerSubscriber',
            self.caller_id, self.topic, self.topic_type, self.caller_api)


class ROSPublisherEndpoint(object):
    '''A high-level endpoint for publishing messages.

    @ivar factory: A suitable L{ServerFactory} for ROS publishers.
    '''
    def __init__(self, factory):
        self.factory = factory

    def publish(self, text):
        '''Send a message to all the subscribers.

        @param text: The C{str} text to send.'''
        for subscriber in self.factory.subscribers:
            s = self.factory.data_class()
            s.data = text
            out = StringIO()
            s.serialize(out)
            v = out.getvalue()
            subscriber.sendString(v)


class ROSPublisher(object):
    '''
    A high-level object representing a ROS Publisher. Manages registration to
    the ROS Master and talking to ROS subscribers.

    @ivar service_class: The generated class that exposes our service.
    @ivar resolved_name: The C{str} with which we use to identify our hostname.
    @ivar caller_id: The C{str} to identify our calls to ROS Master and the
        peers. 
    @ivar master: A L{Proxy} to the ROS Master.
    @ivar caller_api: The local XML-RPC endpoint.
    '''
 
    def __init__(self, name, topic, topic_type, caller_api, ros_hostname,
        ros_rpc_port, data_class, ros_master_uri=None):
        if ros_master_uri is None:
            ros_master_uri = os.environ['ROS_MASTER_URI']
        self.caller_id = rospy.names.resolve_name(name)

        from twisted.web import xmlrpc
        self.master = xmlrpc.Proxy(ros_master_uri)

        self.topic = topic
        self.topic_type = topic_type

        self.caller_api = caller_api  # local xml uri

        self.xml_rpc_api = ROSPublisherSlaveAPI(ros_hostname, ros_rpc_port,
            self.caller_id, self.topic, data_class)

        self.data_class = data_class
        self.ros_rpc_port = ros_rpc_port

    def publish(self, msg):
        '''Send a message to all the subscribers.

        @param msg: A C{str} with the text to send.'''
        self.factory.publish(msg)

    @defer.inlineCallbacks
    def register_publisher(self):
        '''Register ourselves to the ROS Master'''
        o = urlparse.urlparse(self.caller_api)
        reactor.listenTCP(o.port, server.Site(self.xml_rpc_api))

        response = yield self.master.callRemote('registerPublisher',
            self.caller_id, self.topic, self.topic_type, self.caller_api)
        factory = ROSPublisherFactory(self.topic, self.caller_id,
            self.data_class)

        reactor.listenTCP(self.ros_rpc_port, factory)

        endpoint = ROSPublisherEndpoint(factory)
        defer.returnValue(endpoint)


class ROSPublisherProtocol(basic.IntNStringReceiver):

    structFormat = "<I"
    prefixLength = calcsize(structFormat)

    state = State.WAIT

    def stringReceived(self, data):
        if self.state == State.WAIT:

            header = {'topic': self.factory.topic,
                    'message_definition': self.factory.data_class._full_text,
                    'tcp_nodelay': self.factory.tcp_nodelay,
                    'md5sum': self.factory.data_class._md5sum,
                    'type': self.factory.data_class._type,
                    'callerid': self.factory.caller_id}

            out = rosgraph.network.encode_ros_handshake_header(header)
            self.transport.write(out)
            self.state = State.PUBLISH

            self.factory.subscribers.append(self)


class ROSPublisherFactory(protocol.ServerFactory):
    protocol = ROSPublisherProtocol

    def __init__(self, topic, caller_id, data_class, tcp_nodelay=False):
        self.subscribers = []
        self.caller_id = caller_id
        self.topic = topic
        self.data_class = data_class
        self.tcp_nodelay = tcp_nodelay


class ROSSubscriberProtocol(basic.IntNStringReceiver):

    structFormat = "<I"
    prefixLength = calcsize(structFormat)

    state = State.WAIT

    def stringReceived(self, data):
        if self.state == State.WAIT:
            # TODO: validate message
            self.state = State.RESPONSE
        elif self.state == State.RESPONSE:
            s = self.factory.data_class()
            s.deserialize(data)
            self.factory.handler(s)


class ROSSubscriberFactory(protocol.ClientFactory):
    protocol = ROSSubscriberProtocol

    def __init__(self, data_class, handler):
        self.data_class = data_class
        self.handler = handler


class ROSPublisherSlaveAPI(xmlrpc.XMLRPC):

    def __init__(self, ros_rpc_hostname, ros_rpc_port, name, topic, data_class,
        allowNone=False, useDateTime=False):
        xmlrpc.XMLRPC.__init__(self, allowNone=allowNone,
            useDateTime=useDateTime)
        self.ros_rpc_hostname = ros_rpc_hostname
        self.rpc_port = ros_rpc_port
        self.caller_id = rospy.names.resolve_name(name)
        self.topic = rospy.names.resolve_name(topic)
        self.data_class = data_class

    def xmlrpc_requestTopic(self, subscriber, topic, protocol):
        """
        Return all passed args.
        """
        return [1, 'ready on %s:%d' % (self.ros_rpc_hostname, self.rpc_port),
            ['TCPROS', self.ros_rpc_hostname, self.rpc_port]]


class ROSSubscriberSlaveAPI(xmlrpc.XMLRPC):

    def __init__(self, ros_rpc_hostname, ros_rpc_port, name, topic, data_class,
        handler, allowNone=False, useDateTime=False):
        xmlrpc.XMLRPC.__init__(self, allowNone=allowNone,
            useDateTime=useDateTime)
        self.ros_rpc_hostname = ros_rpc_hostname
        self.rpc_port = ros_rpc_port
        self.caller_id = rospy.names.resolve_name(name)
        self.topic = rospy.names.resolve_name(topic)
        self.data_class = data_class
        self.handler = handler
        self.connected_publishers = set()

    @defer.inlineCallbacks
    def connect_to_publishers(self, publishers):
        new_connected_publishers = set()
        for publisher in publishers:
            if publisher not in self.connected_publishers:
                master = xmlrpc.Proxy(publisher)
                response = yield master.callRemote('requestTopic',
                    self.caller_id, self.topic, [['TCPROS']])

                remote = response[2]
                from twisted.internet import reactor
                point = TCP4ClientEndpoint(reactor, remote[1], remote[2])
                factory = ROSSubscriberFactory(self.data_class, self.handler)
                p = yield point.connect(factory)
                header = {'topic': self.topic,
                    'message_definition': self.data_class._full_text,
                    'tcp_nodelay': '0',
                    'md5sum': self.data_class._md5sum,
                    'type': self.data_class._type,
                    'callerid': self.caller_id}

                out = rosgraph.network.encode_ros_handshake_header(header)
                p.transport.write(out)
            new_connected_publishers.add(publisher)
        self.connected_publishsers = new_connected_publishers

    def xmlrpc_requestTopic(self, subscriber, topic, protocol):
        """
        Return all passed args.
        """
        return [1, 'ready on %s:%d' % (self.ros_rpc_hostname, self.rpc_port),
            ['TCPROS', self.ros_rpc_hostname, self.rpc_port]]

    @defer.inlineCallbacks
    def xmlrpc_publisherUpdate(self, caller_id, topic, publishers):
        yield self.connect_to_publishers(publishers)
        defer.returnValue((1, '', 0))
