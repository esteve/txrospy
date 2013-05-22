#!/usr/bin/env python
import os
import time

from twisted.internet import defer
from twisted.application import service, internet
from twisted.web import server

from std_msgs.msg import String

from txrospy import protocol

# Run this a twistd plugin
# E.g. twistd -ny txrospy_listener

ros_service = 'add_two_ints'

ros_rpc_port = 12345
xml_rpc_port = 58260
hostname = 'precise32'
caller_api = 'http://%s:%d/' % (hostname, xml_rpc_port)

rpc_uri = 'rosrpc://%s:%d' % (hostname, ros_rpc_port)


def handler(message):
    print "Incoming message"
    print message

name = 'listener'
anonymous = True
if anonymous:
    name = "%s_%s_%s" % (name, os.getpid(), int(time.time() * 1000))
topic = 'chatter'
topic_type = String._type

application = service.Application('txrospy')
serviceCollection = service.IServiceCollection(application)

s = protocol.ROSSubscriber('txrospy_listener', handler, topic, topic_type,
    caller_api)
s.setServiceParent(serviceCollection)


@defer.inlineCallbacks
def registered(res):
    publishers = res[-1]
    r = protocol.ROSSubscriberSlaveAPI(hostname, ros_rpc_port, name,
        topic, String, handler)
    try:
        yield r.connect_to_publishers(publishers)
    except Exception:
        print "We could not connect to publisher"
    internet.TCPServer(xml_rpc_port, server.Site(r)).setServiceParent(
        serviceCollection)
s.register_subscriber().addCallback(registered)
