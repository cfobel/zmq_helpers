from collections import OrderedDict
import time
from multiprocessing import Pipe, Process

import zmq
from zmq_helpers.rpc import ZmqJsonRpcProxy, ZmqJsonRpcTask
from zmq_helpers.utils import get_random_tcp_uri


class TestTask(ZmqJsonRpcTask):
    def get_uris(self):
        return OrderedDict(rpc=get_random_tcp_uri('*'))

    def on__hello_world(self, *args, **kwargs):
        print '[hello_world]'


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
        m = z.available_handlers
        success = False
        try:
            result = m(__flags__=zmq.NOBLOCK)
            success = True
        except (Exception, ), e:
            # Failed first try.
            pass

        if not success:
            # The first try failed, so try up to 10 more times
            for i in range(10):
                try:
                    result = m.recv_response()
                    break
                except (Exception, ), e:
                    time.sleep(0.001)
        assert(set(result) == set(('available_handlers', 'hello_world')))
    finally:
        parent.send('shutdown')
        time.sleep(0.1)
        p.terminate()
        del p


if __name__ == '__main__':
    main()
