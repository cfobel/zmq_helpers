from threading import Thread
import time
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
from zmq.eventloop.ioloop import IOLoop


class DeferredSocket(object):
    def __init__(self, sock_type):
        self.sock_type = sock_type
        self.binds = []
        self.connects = []
        self.sockopts = []

    def bind(self, addr):
        self.binds.append(addr)
        return self

    def connect(self, addr):
        self.connects.append(addr)
        return self

    def setsockopt(self, attr, value):
        self.sockopts.append((attr, value))
        return self


class SocketGroup(object):
    """
    A Threadsafe set of 0MQ sockets.
    """
    context_factory = zmq.Context.instance

    def __init__(self, deferred_socks=None):
        if deferred_socks is None:
            self._deferred_socks = OrderedDict()
        else:
            self._deferred_socks = OrderedDict(deferred_socks.items())

    def set_sock(self, label, deferred_sock):
        self._deferred_socks[label] = deferred_sock

    def _setup_sockets(self):
        ctx = self.context_factory()

        self._context = ctx

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

    def run(self):
        """The runner method.

        Do not call me directly, instead call ``self.start()``, just like a
        Thread.
        """
        self.socks = self._setup_sockets()
        io_loop = IOLoop.instance()
        self._init_streams(io_loop)
        try:
            io_loop.start()
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


class BackgroundSocketGroup(SocketGroup):
    """Base class for launching Devices in background processes and threads."""

    launcher=None
    _launch_class=None

    def start(self):
        self.launcher = self._launch_class(target=self.run)
        self.launcher.daemon = self.daemon
        return self.launcher.start()

    def join(self, timeout=None):
        return self.launcher.join(timeout=timeout)


class ThreadDevice(BackgroundSocketGroup):
    """A SocketGroup that will be run in a background Thread.

    See SocketGroup for details.
    """
    _launch_class=Thread


class ProcessSocketGroup(BackgroundSocketGroup):
    """A SocketGroup that will be run in a background Process.

    See SocketGroup for details.
    """
    _launch_class=Process
    context_factory = zmq.Context

