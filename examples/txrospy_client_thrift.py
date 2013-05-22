from twisted.internet import defer, reactor

from txrospy import protocol_thrift
from beginner_tutorials.thrift_twisted.AddTwoInts import AddTwoInts


@defer.inlineCallbacks
def start_client():
    client = protocol_thrift.ThriftROSClient('add_two_ints', 'unnamed',
        AddTwoInts.Client)
    proxy = yield client.wait_for_endpoint()

    p2 = yield proxy.add(1, 2)
    print p2

    p3 = yield proxy.add(1234567, 1)
    print p3

    reactor.stop()

if __name__ == '__main__':
    start_client()
    reactor.run()
