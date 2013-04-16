from collections import OrderedDict
import time
from multiprocessing import Pipe, Process

import eventlet
from zmq_helpers.rpc import ZmqJsonRpcProxy, ZmqJsonRpcTask
from zmq_helpers.utils import get_random_tcp_uri


class TestTask(ZmqJsonRpcTask):
    def get_uris(self):
        uris = OrderedDict(rpc=get_random_tcp_uri('*'))
        print uris
        return uris

    def on__hello_world(self, *args, **kwargs):
        for i in range(4):
            print '[hello_world] %d' % i
            time.sleep(1)
        return 'hello, world'


def test__main():
    main()


def main():
    parent, child = Pipe()
    e = TestTask(control_pipe=child)
    uri = e.uris['rpc']
    p = Process(target=e.run)
    try:
        p.start()
        z = ZmqJsonRpcProxy(uri.replace('*', 'localhost'))

        # Rather than calling the `hello_world` proxy method directly, we
        # instead call the `spawn` attribute of the proxy method.  This causes
        # the proxy call to be performed using eventlet green threads.  Here,
        # `d` is an `eventlet.event.Event` instance, which can be queried for
        # when the result is ready.  The final result can be obtained by
        # calling the event's `wait` method.  By calling `eventlet.sleep`, we
        # yield execution to the RPC green thread to check again for a
        # response.
        d = z.hello_world.spawn()
        while not d.ready():
            print 'Something is happening in the "main" thread!'
            eventlet.sleep(0.5)
        result = d.wait()
        print 'result is ready:', result
        assert(result == 'hello, world')
    finally:
        parent.send('shutdown')
        time.sleep(0.1)
        p.terminate()
        del p


if __name__ == '__main__':
    main()
