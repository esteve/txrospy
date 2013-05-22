from twisted.internet import defer, reactor

from txrospy import protocol
from beginner_tutorials.srv import AddTwoInts


@defer.inlineCallbacks
def start_client():
    client = protocol.ROSClient('add_two_ints', 'unnamed', AddTwoInts)
    proxy = yield client.wait_for_endpoint()

    p2 = yield proxy.call(1, 2)
    print p2

    p3 = yield proxy.call(1234567, 1)
    print p3

    reactor.stop()

if __name__ == '__main__':
    start_client()
    reactor.run()
