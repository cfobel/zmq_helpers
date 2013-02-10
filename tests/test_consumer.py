from datetime import datetime
import time
from multiprocessing import Process
from uuid import uuid4
import logging
from collections import OrderedDict

import zmq

from socket_group import DeferredSocket, run_echo_server, run_sock_configs,\
        get_run_context, PeriodicCallback, IOLoop, create_sockets_and_streams


def run_consumer(req_uri, push_uri, pull_uri, delay=0, push_bind=True,
                 pull_bind=True):
    logging.debug('[run_consumer] %s' % dict(
        req_uri=req_uri,
        push_uri=push_uri,
        pull_uri=pull_uri,
        delay=delay,
        push_bind=push_bind,
        pull_bind=pull_bind
    ))
    def process_input(socks, streams, stream, multipart_message):
        logging.debug('[CONSUMER:process_input] %s %s' % (stream,
                                                          multipart_message, ))
        async_id = multipart_message[0]
        message = multipart_message[1:]
        socks['req'].send_multipart(message)
        response = socks['req'].recv_multipart()
        if delay > 0:
            time.sleep(delay)
        socks['push'].send_multipart([async_id] + response)
        logging.debug('  \--> req response: %s' % response)

    sock_configs = OrderedDict([
            ('pull', DeferredSocket(zmq.PULL)
                        .stream_callback('on_recv_stream', process_input)),
            ('push', DeferredSocket(zmq.PUSH)),
            ('req', DeferredSocket(zmq.REQ).connect(req_uri))
    ])
    if push_bind:
        sock_configs['push'].bind(push_uri)
    else:
        sock_configs['push'].connect(push_uri)
    if pull_bind:
        sock_configs['pull'].bind(pull_uri)
    else:
        sock_configs['pull'].connect(pull_uri)
    run_sock_configs(sock_configs)


def run_producer(rep_uri, pub_uri, push_uri, pull_uri, push_bind=False,
        pull_bind=False):
    def process_response(socks, streams, stream, multipart_message):
        logging.debug('[PRODUCER:process_response] %s %s' % (stream,
                                                             multipart_message,
                                                             ))
        socks['pub'].send_multipart(multipart_message)

    def process_request(socks, streams, stream, multipart_message):
        logging.debug('[PRODUCER:process_request] %s %s' % (stream,
                                                            multipart_message,
                                                            ))
        async_id = '[%s] %s' % (datetime.now(), uuid4())
        socks['rep'].send_multipart([async_id] + multipart_message)
        socks['push'].send_multipart([async_id] + multipart_message)

    sock_configs = OrderedDict([
        ('push', DeferredSocket(zmq.PUSH)),
        ('pull', DeferredSocket(zmq.PULL)
                    .stream_callback('on_recv_stream', process_response)
        ),
        ('rep', DeferredSocket(zmq.REP)
                    .bind(rep_uri)
                    .stream_callback('on_recv_stream', process_request)
        ),
        ('pub', DeferredSocket(zmq.PUB).bind(pub_uri))
    ])

    if push_bind:
        sock_configs['push'].bind(push_uri)
    else:
        sock_configs['push'].connect(push_uri)
    if pull_bind:
        sock_configs['pull'].bind(pull_uri)
    else:
        sock_configs['pull'].connect(pull_uri)

    run_sock_configs(sock_configs)


def run_client(req_uri, sub_uri):
    def process_response(socks, streams, stream, multipart_message):
        logging.debug('[CLIENT:process_response] %s %s' % (stream,
                                                             multipart_message,
                                                             ))

    def do_request():
        socks['req'].send_multipart(['hello world'])
        response = socks['req'].recv_multipart()
        logging.debug('[CLIENT:do_request] %s' % response)

    sock_configs = OrderedDict([
            ('req', DeferredSocket(zmq.REQ).connect(req_uri)),
            ('sub', DeferredSocket(zmq.SUB)
                        .connect(sub_uri)
                        .setsockopt(zmq.SUBSCRIBE, '')
                        .stream_callback('on_recv_stream', process_response))
    ])

    ctx, io_loop, socks, streams = get_run_context(sock_configs)
    periodic_callback = PeriodicCallback(do_request, 1000, io_loop)

    try:
        periodic_callback.start()
        io_loop.start()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    echo_server = Process(target=run_echo_server, args=('ipc://ECHO:1', ))
    consumer = Process(target=run_consumer, args=(
            'ipc://ECHO:1', 'ipc://CONSUMER_TEST_PUSH_BE:1',
            'ipc://CONSUMER_TEST_PULL_BE:1', 0.5, ))
    producer = Process(target=run_producer, args=(
            'ipc://PRODUCER_REP:1',
            'ipc://PRODUCER_PUB:1',
            'ipc://CONSUMER_TEST_PULL_BE:1',
            'ipc://CONSUMER_TEST_PUSH_BE:1',
    ))
    client = Process(target=run_client, args=('ipc://PRODUCER_REP:1',
                                              'ipc://PRODUCER_PUB:1'))

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
