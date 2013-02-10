from datetime import datetime

import zmq

from socket_group import ProcessSocketGroupDevice, DeferredSocket, get_echo_server


def get_client():
    # Configure client -----------------
    #    The client simply sends a request every half second.  Each response
    #    received is then dumped to `stdout`.
    #    Each request contains a time-stamp and the words 'hello world'
    client = ProcessSocketGroupDevice()

    def ping(self):
        self.socks['req'].send('[%s] hello world' % datetime.now())

    def pong(self, multipart_message):
        print 'PONG:', multipart_message[0]

    client.set_sock('req',
        DeferredSocket(zmq.REQ)
               .connect('ipc://ECHO:1')
               .stream_callback('on_recv', pong)
    )
    client.set_periodic_callback('ping', (ping, 500))
    return client


if __name__ == '__main__':
    server = get_echo_server('ipc://ECHO:1')
    client = get_client()
    try:
        server.start()
        client.start()
        client.join()
    except KeyboardInterrupt:
        client.launcher.terminate()
    server.launcher.terminate()
