import time

from twisted.web import server

from twisted.internet import defer, reactor
from twisted.internet.task import deferLater

import txrospy
from std_msgs.msg import String


@defer.inlineCallbacks
def start_client():
    name = 'talker'
    topic = 'chatter'
    data_class = String

    topic_type = String._type

    xml_rpc_port = 58261
    ros_rpc_port = 12345

    hostname = 'precise32'
    caller_api = 'http://%s:%d/' % (hostname, xml_rpc_port)

    s = txrospy.ROSPublisher(name, topic, topic_type, caller_api, hostname,
        ros_rpc_port, data_class)

    endpoint = yield s.register_publisher()

    while True:
        out = 'hello ' + str(time.time())
        endpoint.publish(out)
        yield deferLater(reactor, 1.0, lambda: None)


if __name__ == '__main__':
    start_client()
    reactor.run()
