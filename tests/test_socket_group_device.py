from socket_group import SocketGroupDevice, ProcessSocketGroupDevice, DeferredSocket, PeriodicCallback


class TestDeviceMixin(object):
    def _setup_loop_after_streams(self):
        print '[TestDeviceMixin] _setup_loop_after_streams'
        if not hasattr(self, 'callbacks'):
            self.callbacks = {}
            callback = PeriodicCallback(self.do_event, 1000, self.io_loop)
            self.callbacks['pingpong'] = callback
            callback.start()

    def do_event(self):
        from datetime import datetime

        out_message = 'hello world %s' % datetime.now()
        print 'PING: %s' % out_message
        self.socks['req'].send(out_message)
        in_message = self.socks['req'].recv_multipart()
        print 'PONG: %s' % ''.join(in_message)

class TestDevice(TestDeviceMixin, SocketGroupDevice):
    pass


class TestDeviceProcess(TestDeviceMixin, ProcessSocketGroupDevice):
    pass


if __name__ == '__main__':
    from pprint import pprint
    import zmq

    print 'Create process'
    s = TestDeviceProcess()
    s.set_sock('req', DeferredSocket(zmq.REQ)
               .connect('ipc://ECHO:1')
               .stream_callback('on_recv', lambda *args, **kwargs: pprint(args, kwargs)))
    try:
        s.start()
        s.join()
    except KeyboardInterrupt:
        s.launcher.terminate()
