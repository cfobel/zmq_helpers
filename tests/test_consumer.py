import time

import zmq

from socket_group import ProcessSocketGroupDevice, DeferredSocket, get_echo_server


def get_consumer():
    consumer = ProcessSocketGroupDevice()

    def process_input(self, stream, multipart_message):
        print '[process_input] %s' % (multipart_message, )
        self.socks['req'].send_multipart(multipart_message + ['[processed]'])
        response = self.socks['req'].recv_multipart()
        print '[process_input] req response:', response
        self.socks['push'].send_multipart(response)

    consumer.set_sock('pull',
        DeferredSocket(zmq.PULL)
            .bind('ipc://CONSUMER_TEST_PULL_BE:1')
            .stream_callback('on_recv_stream', process_input)
    )
    consumer.set_sock('push', DeferredSocket(zmq.PUSH).bind('ipc://CONSUMER_TEST_PUSH_BE:1'))
    consumer.set_sock('req', DeferredSocket(zmq.REQ) .connect('ipc://ECHO:1'))
    return consumer


def get_producer():
    producer = ProcessSocketGroupDevice()

    def process_request(self, stream, multipart_message):
        print '[process_request] %s' % (multipart_message, )
        self.socks['rep'].send_multipart(multipart_message + ['ack'])
        self.socks['push'].send_multipart(multipart_message)


    producer.set_sock('rep',
        DeferredSocket(zmq.REP)
            .bind('ipc://PRODUCER_REP:1')
            .stream_callback('on_recv_stream', process_request)
    )


    def process_response(self, stream, multipart_message):
        print '[process_response] %s' % (multipart_message, )


    producer.set_sock('pull',
        DeferredSocket(zmq.PULL)
            .connect('ipc://CONSUMER_TEST_PUSH_BE:1')
            .stream_callback('on_recv_stream', process_response)
    )

    producer.set_sock('push',
        DeferredSocket(zmq.PUSH)
            .connect('ipc://CONSUMER_TEST_PULL_BE:1')
    )
    return producer


if __name__ == '__main__':
    echo_server = get_echo_server('ipc://ECHO:1')
    consumer = get_consumer()
    producer = get_producer()

    try:
        echo_server.start()
        consumer.start()
        time.sleep(0.2)
        producer.start()
        consumer.join()
        echo_server.join()
        producer.join()
    except KeyboardInterrupt:
        echo_server.launcher.terminate()
        consumer.launcher.terminate()
        producer.launcher.terminate()
