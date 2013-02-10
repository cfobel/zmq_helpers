import functools
from threading import Thread
from datetime import datetime
import time
import logging
try:
    from multiprocessing import Process
except ImportError:
    Process = None
'''
Note:
    We can use something like the following in the case where multiprocessing
    is not available:

        __all__ = [ 'Device', 'ThreadDevice']
        if Process is not None:
            __all__.append('ProcessDevice')
'''
from collections import OrderedDict

import zmq
from zmq.eventloop.ioloop import IOLoop, PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream


class DeferredSocket(object):
    def __init__(self, sock_type):
        self.sock_type = sock_type
        self.binds = []
        self.connects = []
        self.sockopts = []
        self.stream_callbacks = []

    def bind(self, addr):
        self.binds.append(addr)
        return self

    def connect(self, addr):
        self.connects.append(addr)
        return self

    def setsockopt(self, attr, value):
        self.sockopts.append((attr, value))
        return self

    def stream_callback(self, stream_event, callback):
        self.stream_callbacks.append((stream_event, callback))
        return self


class DeferredSocketGroup(object):
    def __init__(self, deferred_socks=None):
        if deferred_socks is None:
            self._deferred_socks = OrderedDict()
        else:
            self._deferred_socks = OrderedDict(deferred_socks.items())

    def set_sock(self, label, deferred_sock):
        self._deferred_socks[label] = deferred_sock

    def create_sockets(self, ctx):
        socks = OrderedDict()

        for label, deferred_sock in self._deferred_socks.iteritems():
            socks[label] = zmq.Socket(ctx, deferred_sock.sock_type)

        # set sockopts (must be done first, in case of zmq.IDENTITY)
        for label, s in self._deferred_socks.iteritems():
            for opt, value in s.sockopts:
                socks[label].setsockopt(opt, value)

        for label, s in self._deferred_socks.iteritems():
            for bind_uri in s.binds:
                socks[label].bind(bind_uri)

        for label, s in self._deferred_socks.iteritems():
            for connect_uri in s.connects:
                socks[label].connect(connect_uri)
        return socks

    def create_streams(self, socks, io_loop):
        streams = OrderedDict()
        logging.debug('[create_streams] socks = %s' % socks)
        for label, s in self._deferred_socks.iteritems():
            if label in socks and s.stream_callbacks:
                sock = socks[label]
                stream = ZMQStream(sock, io_loop)
                for stream_event, callback in s.stream_callbacks:
                    logging.debug('[create_streams] %s %s %s %s' % (
                            label, stream, stream_event, callback))
                    # Register callback for stream event
                    #   e.g., stream_event='on_recv'
                    f = getattr(stream, stream_event)

                    # Prepend `self` to `args` list for `callback`, to allow
                    # callback targets to use the context of the containing
                    # group, i.e., access the created sockets, etc.  This is
                    # particularly useful when running the IOLoop in a thread
                    # or process.
                    f(lambda *args, **kwargs: callback(self, *args, **kwargs))
                streams[label] = stream
        return streams


class SocketGroupDevice(DeferredSocketGroup):
    """
    A Threadsafe set of 0MQ sockets.
    """
    context_factory = zmq.Context.instance

    def __init__(self, *args, **kwargs):
        periodic_callbacks = kwargs.pop('periodic_callbacks', None)
        if periodic_callbacks is None:
            self._periodic_callbacks = OrderedDict()
        else:
            self._periodic_callbacks = OrderedDict(periodic_callbacks.items())
        super(SocketGroupDevice, self).__init__(*args, **kwargs)

    def set_periodic_callback(self, label, callback_defn):
        assert(isinstance(callback_defn, tuple) and len(callback_defn) == 2)
        self._periodic_callbacks[label] = callback_defn
        return self

    def _setup_periodic_callbacks(self, io_loop):
        return self.create_periodic_callbacks(self._periodic_callbacks, io_loop)

    def create_periodic_callbacks(self, callback_defns, io_loop):
        periodic_callbacks = OrderedDict()
        for label, (callback, period_ms) in callback_defns.items():
            wrapped = functools.partial(callback, self)
            periodic_callback = PeriodicCallback(
                    lambda *args, **kwargs: wrapped(*args, **kwargs),
                    period_ms, io_loop)
            periodic_callbacks[label] = periodic_callback
        for callback in periodic_callbacks.values():
            callback.start()
        return periodic_callbacks

    def _setup_streams(self, socks, io_loop):
        streams = self.create_streams(socks, io_loop)
        return streams

    def _setup_sockets(self, ctx):
        socks = self.create_sockets(ctx)
        return socks

    def _setup_loop(self):
        io_loop = IOLoop()
        return io_loop

    def _setup_loop_after_streams(self):
        pass

    def run(self):
        """The runner method.

        Do not call me directly, instead call ``self.start()``, just like a
        Thread.
        """
        self.context = self.context_factory()
        self.socks = self._setup_sockets(self.context)
        self.io_loop = self._setup_loop()
        self.periodic_callbacks = self._setup_periodic_callbacks(self.io_loop)
        self.streams = self._setup_streams(self.socks, self.io_loop)
        #self._setup_loop_after_streams()

        try:
            self.io_loop.start()
        except KeyboardInterrupt:
            pass
        self.done = True

    def start(self):
        """Start the device. Override me in subclass for other launchers."""
        return self.run()

    def join(self,timeout=None):
        """wait for me to finish, like Thread.join.

        Reimplemented appropriately by sublcasses."""
        tic = time.time()
        toc = tic
        while not self.done and not (timeout is not None and toc-tic > timeout):
            time.sleep(.001)
            toc = time.time()


class BackgroundSocketGroupDevice(SocketGroupDevice):
    """Base class for launching SocketGroupDevices in background processes and threads."""

    launcher=None
    _launch_class=None

    def __init__(self, *args, **kwargs):
        super(BackgroundSocketGroupDevice, self).__init__(*args, **kwargs)
        self.daemon = True

    def start(self):
        self.launcher = self._launch_class(target=self.run)
        self.launcher.daemon = self.daemon
        return self.launcher.start()

    def join(self, timeout=None):
        return self.launcher.join(timeout=timeout)


class ThreadSocketGroupDevice(BackgroundSocketGroupDevice):
    """A SocketGroup that will be run in a background Thread.

    See SocketGroup for details.
    """
    _launch_class=Thread


class ProcessSocketGroupDevice(BackgroundSocketGroupDevice):
    """A SocketGroup that will be run in a background Process.

    See SocketGroup for details.
    """
    _launch_class=Process
    context_factory = zmq.Context


def get_echo_server(bind_uri):
    # Configure server
    #    The server simply echoes any message received, by sending the same
    #    message back as a response.
    def echo(self, multipart_message):
        self.socks['rep'].send_multipart(multipart_message)

    server = ProcessSocketGroupDevice()
    server.set_sock('rep',
        DeferredSocket(zmq.REP)
               .bind(bind_uri)
               .stream_callback('on_recv', echo)
    )
    return server
