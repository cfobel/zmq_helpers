import zmq

from socket_group import ProcessSocketGroupDevice, DeferredSocket, get_echo_server


if __name__ == '__main__':
    echo_server = get_echo_server('ipc://ECHO:1')

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

    try:
        echo_server.start()
        consumer.start()
        consumer.join()
        echo_server.join()
    except KeyboardInterrupt:
        echo_server.launcher.terminate()
        consumer.launcher.terminate()
