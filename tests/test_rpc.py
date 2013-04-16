from collections import OrderedDict
import time
from multiprocessing import Pipe, Process

from zmq_helpers.rpc import ZmqJsonRpcTask
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
    p = Process(target=e.run)
    try:
        p.start()
        p.join()
    finally:
        parent.send('shutdown')
        time.sleep(0.1)
        p.terminate()
        del p


if __name__ == '__main__':
    main()
