from datetime import datetime

from socket_group import ProcessSocketGroupDevice, DeferredSocket, PeriodicCallback


if __name__ == '__main__':
    from pprint import pprint
    import zmq

    s = ProcessSocketGroupDevice()

    def ping(self):
        self.socks['req'].send('[%s] hello world' % datetime.now())

    def pong(self, multipart_message):
        print 'PONG:', multipart_message[0], self.socks['req']

    s.set_sock('req',
        DeferredSocket(zmq.REQ)
               .connect('ipc://ECHO:1')
               .stream_callback('on_recv', pong)
    )
    s.set_periodic_callback('ping', (ping, 500))

    try:
        s.start()
        s.join()
    except KeyboardInterrupt:
        s.launcher.terminate()
