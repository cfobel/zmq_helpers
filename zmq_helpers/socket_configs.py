import functools
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

from utils import log_label


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


class SockConfigsTask(object):
    def __init__(self, sock_configs, on_run=None, control_pipe=None, **kwargs):
        self.sock_configs = sock_configs
        self.on_run = on_run
        self.control_pipe = control_pipe

    def run(self):
        on_run = getattr(self, 'on_run', None)
        run_sock_configs(self.sock_configs, on_run, self.control_pipe)


def create_sockets(ctx, deferred_socks, socks=None):
    if socks is None:
        socks = OrderedDict()

    for label, deferred_sock in deferred_socks.iteritems():
        socks[label] = zmq.Socket(ctx, deferred_sock.sock_type)

    # set sockopts (must be done first, in case of zmq.IDENTITY)
    for label, s in deferred_socks.iteritems():
        for opt, value in s.sockopts:
            socks[label].setsockopt(opt, value)

    for label, s in deferred_socks.iteritems():
        for bind_uri in s.binds:
            try:
                socks[label].bind(bind_uri)
            except:
                logging.getLogger(log_label()).error('Could not bind: %s',
                                                     bind_uri)
                raise
    for label, s in deferred_socks.iteritems():
        for connect_uri in s.connects:
            try:
                socks[label].connect(connect_uri)
            except:
                logging.getLogger(log_label()).error('Could not connect: %s',
                                                     connect_uri)
                raise
    return socks


def create_streams(deferred_socks, socks, io_loop):
    streams = OrderedDict()
    logging.getLogger(log_label()).debug('socks = %s' % socks)
    for label, s in deferred_socks.iteritems():
        if label in socks and s.stream_callbacks:
            sock = socks[label]
            stream = ZMQStream(sock, io_loop)
            for stream_event, callback in s.stream_callbacks:
                logging.getLogger(log_label()).debug('%s %s %s %s' % (
                        label, stream, stream_event, callback))
                # Register callback for stream event
                #   e.g., stream_event='on_recv'
                env = OrderedDict([
                    ('ctx', sock.context),
                    ('io_loop', io_loop),
                    ('socks', socks),
                    ('streams', streams),
                ])
                f = functools.partial(callback, env)
                getattr(stream, stream_event)(f)
            streams[label] = stream
    return streams


def create_sockets_and_streams(ctx, deferred_socks, io_loop):
    socks = create_sockets(ctx, deferred_socks)
    streams = create_streams(deferred_socks, socks, io_loop)
    return socks, streams


def get_run_context(sock_configs):
    ctx = zmq.Context()
    io_loop = IOLoop()
    socks, streams = create_sockets_and_streams(ctx, sock_configs, io_loop)

    return ctx, io_loop, socks, streams


def run_sock_configs(sock_configs, on_run=None, control_pipe=None):
    ctx, io_loop, socks, streams = get_run_context(sock_configs)

    def _on_run():
        on_run(ctx, io_loop, socks, streams)

    if on_run:
        io_loop.add_callback(_on_run)

    def watchdog():
        if control_pipe.poll():
            io_loop.stop()

    if control_pipe:
        callback = PeriodicCallback(watchdog, 500, io_loop=io_loop)
        callback.start()

    try:
        io_loop.start()
    except KeyboardInterrupt:
        pass


def echo(env, multipart_message, delay=0, verbose=False):
    if delay > 0:
        time.sleep(delay)
    if verbose:
        print multipart_message
    env['socks']['rep'].send_multipart(multipart_message)


class EchoServer(SockConfigsTask):
    def __init__(self, bind_uri, delay=0, verbose=False, control_pipe=None):
        # Configure server
        #    The server simply echoes any message received, by sending the same
        #    message back as a response.
        wrapped = functools.partial(echo, delay=delay, verbose=verbose)
        self.control_pipe = control_pipe
        self.sock_configs = OrderedDict([
                ('rep', DeferredSocket(zmq.REP)
                            .bind(bind_uri)
                            .stream_callback('on_recv', wrapped))
        ])


def run_echo_server(bind_uri, delay=0):
    echo_server = EchoServer(bind_uri, delay=delay)
    echo_server.run()


