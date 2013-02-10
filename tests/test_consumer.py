from datetime import datetime
import time
from multiprocessing import Process
from uuid import uuid4
import logging

import zmq
from zmq.eventloop.zmqstream import ZMQStream

from socket_group import ProcessSocketGroupDevice, DeferredSocket,\
        get_echo_server, IOLoop


def get_consumer(delay=0):
    consumer = ProcessSocketGroupDevice()

    def process_input(self, stream, multipart_message):
        logging.debug('[CONSUMER:process_input] %s %s' % (stream,
                                                          multipart_message, ))
        async_id = multipart_message[0]
        message = multipart_message[1:]
        self.socks['req'].send_multipart(message)
        response = self.socks['req'].recv_multipart()
        if delay > 0:
            time.sleep(delay)
        self.socks['push'].send_multipart([async_id] + response)
        logging.debug('  \--> req response: %s' % response)

    consumer.set_sock('pull',
        DeferredSocket(zmq.PULL)
            .bind('ipc://CONSUMER_TEST_PULL_BE:1')
            .stream_callback('on_recv_stream', process_input)
    )
    consumer.set_sock('push',
                      DeferredSocket(zmq.PUSH)
                            .bind('ipc://CONSUMER_TEST_PUSH_BE:1'))
    consumer.set_sock('req', DeferredSocket(zmq.REQ) .connect('ipc://ECHO:1'))
    return consumer


def get_client():
    client = ProcessSocketGroupDevice()

    def process_response(self, stream, multipart_message):
        logging.debug('[CLIENT:process_response] %s %s' % (stream,
                                                             multipart_message,
                                                             ))

    def do_request(self):
        self.socks['req'].send_multipart(['hello world'])
        response = self.socks['req'].recv_multipart()
        logging.debug('[CLIENT:do_request] %s' % response)

    client.set_sock('req',
        DeferredSocket(zmq.REQ)
            .connect('ipc://PRODUCER_REP:1')
    )

    client.set_periodic_callback('do_request', (do_request, 1000))

    client.set_sock('sub',
        DeferredSocket(zmq.SUB)
            .connect('ipc://PRODUCER_PUB:1')
            .setsockopt(zmq.SUBSCRIBE, '')
            .stream_callback('on_recv_stream', process_response)
    )
    return client


def run_producer():
    ctx = zmq.Context.instance()

    push = zmq.Socket(ctx, zmq.PUSH)
    push.connect('ipc://CONSUMER_TEST_PULL_BE:1')
    pull = zmq.Socket(ctx, zmq.PULL)
    pull.connect('ipc://CONSUMER_TEST_PUSH_BE:1')
    rep = zmq.Socket(ctx, zmq.REP)
    rep.bind('ipc://PRODUCER_REP:1')
    sub = zmq.Socket(ctx, zmq.PUB)
    sub.bind('ipc://PRODUCER_PUB:1')

    def process_response(stream, multipart_message):
        logging.debug('[PRODUCER:process_response] %s %s' % (stream,
                                                             multipart_message,
                                                             ))
        sub.send_multipart(multipart_message)

    def process_request(stream, multipart_message):
        logging.debug('[PRODUCER:process_request] %s %s' % (stream,
                                                            multipart_message,
                                                            ))
        async_id = '[%s] %s' % (datetime.now(), uuid4())
        rep.send_multipart([async_id] + multipart_message)
        push.send_multipart([async_id] + multipart_message)

    io_loop = IOLoop.instance()

    pull_stream = ZMQStream(pull, io_loop)
    pull_stream.on_recv_stream(process_response)

    rep_stream = ZMQStream(rep, io_loop)
    rep_stream.on_recv_stream(process_request)

    try:
        io_loop.start()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    echo_server = get_echo_server('ipc://ECHO:1')
    consumer = get_consumer(0.2)
    client = get_client()

    producer = Process(target=run_producer)

    try:
        echo_server.start()
        consumer.start()
        time.sleep(0.2)
        producer.start()
        client.start()
        client.join()
        producer.join()
        consumer.join()
        echo_server.join()
    except KeyboardInterrupt:
        producer.terminate()
        client.launcher.terminate()
        consumer.launcher.terminate()
        echo_server.launcher.terminate()
