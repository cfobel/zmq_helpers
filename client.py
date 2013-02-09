from multiprocessing import Process
import time

import zmq

from async_task_queue import Consumer, get_uris


class Client(Process):
    def __init__(self, rep_uri):
        super(Client, self).__init__()
        self.rep_uri = rep_uri

    def run(self):
        ctx = zmq.Context.instance()
        sock = zmq.Socket(ctx, zmq.REQ)

        sock.connect(self.rep_uri)

        uris = get_uris(sock)

        for i in range(5):
            sock.send_json({"command": 'hello_world'})
            response = sock.recv_json()
            print response
            #time.sleep(0.1)


if __name__ == '__main__':
    import sys

    c = Client(sys.argv[1])
    c.start()
    c.join()
