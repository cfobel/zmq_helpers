from pprint import pprint

import zmq

from async_task_queue import Producer, get_uris, Consumer


class Server(object):
    def __init__(self, rep_uri, pub_uri_prefix):
        self.rep_uri = rep_uri
        self.pub_uri_prefix = pub_uri_prefix
        self.producer = Producer(self.rep_uri, self.pub_uri_prefix)
        self.producer.start()

    def run(self):
        ctx = zmq.Context.instance()
        sock = zmq.Socket(ctx, zmq.REQ)
        sock.connect(self.rep_uri)
        uris = get_uris(sock)
        pprint(uris)

        c = Consumer(uris['push'], uris['pull'], 1)
        try:
            c.run()
        except KeyboardInterrupt:
            pass
        self.producer.terminate()

if __name__ == '__main__':
    server = Server('ipc://ASYNC_SERVER_REP:1', 'ipc://ASYNC_SERVER_PUB:1')
    server.run()
