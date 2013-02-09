from datetime import datetime
from multiprocessing import Process
from uuid import uuid4
import time

import zmq
from zmq.utils import jsonapi
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream


class Consumer(object):
    def __init__(self, pull_uri, push_uri, delay=0):
        self.pull_uri = pull_uri
        self.push_uri = push_uri
        self.delay = delay

    def run(self):
        self.ctx = zmq.Context.instance()
        self.pull_sock = zmq.Socket(self.ctx, zmq.PULL)
        self.pull_sock.connect(self.pull_uri)
        self.push_sock = zmq.Socket(self.ctx, zmq.PUSH)
        self.push_sock.connect(self.push_uri)

        io_loop = IOLoop.instance()
        stream = ZMQStream(self.pull_sock, io_loop)
        stream.on_recv(self.on_recv)
        io_loop.start()

    def on_recv(self, multipart_message):
        message = self._deserialize(multipart_message[0])
        print datetime.now(), message
        if self.delay > 0:
            time.sleep(self.delay)
        message['type'] = 'result'
        message['result'] = None
        self.push_sock.send(self._serialize(message))

    def _serialize(self, message):
        return jsonapi.dumps(message)

    def _deserialize(self, message):
        return jsonapi.loads(message)

class Producer(Process):
    def __init__(self, rep_uri_prefix, pub_uri_prefix):
        super(Producer, self).__init__()

        uri_data = rep_uri_prefix.split(':')
        if len(uri_data) > 2:
            self.rep_port = uri_data[-1]
        else:
            self.rep_port = None
        self.rep_uri_prefix = ':'.join(uri_data[:2])

        uri_data = pub_uri_prefix.split(':')
        if len(uri_data) > 2:
            self.pub_port = uri_data[-1]
        else:
            self.pub_port = None
        self.pub_uri_prefix = ':'.join(uri_data[:2])

    def run(self):
        self.ctx = zmq.Context.instance()

        # Front-end
        self.rep_sock = zmq.Socket(self.ctx, zmq.REP)
        if self.rep_port is None:
            self.rep_port = self.rep_sock.bind_to_random_port(self.rep_uri_prefix)
            self.rep_uri = '%s:%s' % (self.rep_uri_prefix, self.rep_port)
        else:
            self.rep_uri = '%s:%s' % (self.rep_uri_prefix, self.rep_port)
            self.rep_sock.bind(self.rep_uri)

        self.pub_sock = zmq.Socket(self.ctx, zmq.PUB)
        if self.pub_port is None:
            self.pub_port = self.pub_sock.bind_to_random_port(self.pub_uri_prefix)
            self.pub_uri = '%s:%s' % (self.pub_uri_prefix, self.pub_port)
        else:
            self.pub_uri = '%s:%s' % (self.pub_uri_prefix, self.pub_port)
            self.pub_sock.bind(self.pub_uri)
        print '[Producer] REP bound to:', self.rep_uri
        print '[Producer] PUB bound to:', self.pub_uri

        # Back-end
        self.push_sock = zmq.Socket(self.ctx, zmq.PUSH)
        self.push_uri_prefix = 'ipc://PRODUCER_BACKEND_PUSH'
        self.push_port = self.push_sock.bind_to_random_port(self.push_uri_prefix)
        self.push_uri = '%s:%s' % (self.push_uri_prefix, self.push_port)

        self.pull_sock = zmq.Socket(self.ctx, zmq.PULL)
        self.pull_uri_prefix = 'ipc://PRODUCER_BACKEND_PULL'
        self.pull_port = self.pull_sock.bind_to_random_port(self.pull_uri_prefix)
        self.pull_uri = '%s:%s' % (self.pull_uri_prefix, self.pull_port)

        io_loop = IOLoop.instance()

        rep_stream = ZMQStream(self.rep_sock, io_loop)
        rep_stream.on_recv(lambda multipart: self.handle_request(multipart[0]))
        pull_stream = ZMQStream(self.pull_sock, io_loop)
        pull_stream.on_recv(lambda multipart: self.handle_response(multipart[0]))

        io_loop.start()

    def handle_response(self, message):
        # Forward response to publish socket
        self.pub_sock.send(message)

    def handle_request(self, message):
        # Tag request with a unique `async_id` field.
        request = self._deserialize(message)
        response = request.copy()
        response['async_id'] = '%s [%s]' % (datetime.now(), uuid4())
        if request['command'] in ('pub_uri', 'rep_uri', 'pull_uri',
                                  'push_uri'):
            response['type'] = 'result'
            response['result'] = getattr(self, request['command'])
        else:
            response['type'] = 'async'
            serialized_response = self._serialize(response)
            self.push_sock.send(serialized_response)
            response['type'] = 'ack'
        serialized_response = self._serialize(response)
        self.rep_sock.send(serialized_response)

    def _serialize(self, message):
        return jsonapi.dumps(message)

    def _deserialize(self, message):
        return jsonapi.loads(message)


def get_uris(sock):
    uris = {}
    for uri_label in ('rep', 'push', 'pull', 'pub'):
        sock.send_json({"command": '%s_uri' % uri_label})
        response = sock.recv_json()
        uris[uri_label] = response['result']
    return uris
