from datetime import datetime
import time
from multiprocessing import Process
from uuid import uuid4
import logging
from collections import OrderedDict

import zmq

from socket_group import DeferredSocket, EchoServer, run_sock_configs,\
        get_run_context, PeriodicCallback, SockConfigsTask


class Consumer(SockConfigsTask):
    def __init__(self, req_uri, push_uri, pull_uri, delay=0, push_bind=True,
                 pull_bind=True):
        self.delay = delay
        self.uris = OrderedDict([
                ('req', req_uri),
                ('push', push_uri),
                ('pull', pull_uri),
        ])
        self.sock_configs = OrderedDict([
                ('req', DeferredSocket(zmq.REQ).connect(req_uri)),
                ('push', DeferredSocket(zmq.PUSH)),
                ('pull', DeferredSocket(zmq.PULL)
                            .stream_callback('on_recv_stream', self.process_input)),
        ])
        if push_bind:
            self.sock_configs['push'].bind(push_uri)
        else:
            self.sock_configs['push'].connect(push_uri)
        if pull_bind:
            self.sock_configs['pull'].bind(pull_uri)
        else:
            self.sock_configs['pull'].connect(pull_uri)

    def process_input(self, socks, streams, stream, multipart_message):
        logging.debug('[CONSUMER:process_input] %s %s' % (stream,
                                                          multipart_message, ))
        async_id = multipart_message[0]
        message = multipart_message[1:]
        socks['req'].send_multipart(message)
        response = socks['req'].recv_multipart()
        if self.delay > 0:
            time.sleep(self.delay)
        socks['push'].send_multipart([async_id] + response)
        logging.debug('  \--> req response: %s' % response)


class Producer(SockConfigsTask):
    def __init__(self, rep_uri, pub_uri, push_uri, pull_uri, push_bind=False,
                 pull_bind=False):
        self.sock_configs = OrderedDict([
            ('push', DeferredSocket(zmq.PUSH)),
            ('pull', DeferredSocket(zmq.PULL)
                        .stream_callback('on_recv_stream', self.process_response)
            ),
            ('rep', DeferredSocket(zmq.REP)
                        .bind(rep_uri)
                        .stream_callback('on_recv_stream', self.process_request)
            ),
            ('pub', DeferredSocket(zmq.PUB).bind(pub_uri))
        ])

        if push_bind:
            self.sock_configs['push'].bind(push_uri)
        else:
            self.sock_configs['push'].connect(push_uri)
        if pull_bind:
            self.sock_configs['pull'].bind(pull_uri)
        else:
            self.sock_configs['pull'].connect(pull_uri)

    def process_response(self, socks, streams, stream, multipart_message):
        logging.debug('[PRODUCER:process_response] %s %s' % (stream,
                                                             multipart_message,
                                                             ))
        socks['pub'].send_multipart(multipart_message)

    def process_request(self, socks, streams, stream, multipart_message):
        logging.debug('[PRODUCER:process_request] %s %s' % (stream,
                                                            multipart_message,
                                                            ))
        async_id = '[%s] %s' % (datetime.now(), uuid4())
        socks['rep'].send_multipart([async_id] + multipart_message)
        socks['push'].send_multipart([async_id] + multipart_message)


class Client(object):
    def __init__(self, req_uri, sub_uri):
        self.sock_configs = OrderedDict([
                ('req', DeferredSocket(zmq.REQ).connect(req_uri)),
                ('sub', DeferredSocket(zmq.SUB)
                            .connect(sub_uri)
                            .setsockopt(zmq.SUBSCRIBE, '')
                            .stream_callback('on_recv_stream',
                                             self.process_response))
        ])

    def process_response(self, socks, streams, stream, multipart_message):
        logging.debug('[CLIENT:process_response] %s %s' % (stream,
                                                             multipart_message,
                                                             ))

    def do_request(self):
        self.socks['req'].send_multipart(['hello world'])
        response = self.socks['req'].recv_multipart()
        logging.debug('[CLIENT:do_request] %s' % response)

    def run(self):
        ctx, io_loop, self.socks, streams = get_run_context(self.sock_configs)
        periodic_callback = PeriodicCallback(self.do_request, 1000, io_loop)

        try:
            periodic_callback.start()
            io_loop.start()
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    echo_server = Process(target=EchoServer('ipc://ECHO:1', ).run)
    consumer = Process(target=Consumer('ipc://ECHO:1',
                                       'ipc://CONSUMER_TEST_PUSH_BE:1',
                                       'ipc://CONSUMER_TEST_PULL_BE:1',
                                       0.5).run
    )
    producer = Process(target=Producer(
            'ipc://PRODUCER_REP:1',
            'ipc://PRODUCER_PUB:1',
            'ipc://CONSUMER_TEST_PULL_BE:1',
            'ipc://CONSUMER_TEST_PUSH_BE:1').run
    )
    client = Process(target=Client('ipc://PRODUCER_REP:1',
                                   'ipc://PRODUCER_PUB:1').run)

    try:
        echo_server.start()
        consumer.start()
        producer.start()
        client.start()
        client.join()
        producer.join()
        consumer.join()
        echo_server.join()
    except KeyboardInterrupt:
        producer.terminate()
        client.terminate()
        consumer.terminate()
        echo_server.terminate()
