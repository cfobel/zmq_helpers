import inspect
from collections import OrderedDict
try:
    import cPickle as pickle
except ImportError:
    import pickle
from datetime import datetime

import zmq
from zmq.utils import jsonapi

from .socket_configs import SockConfigsTask, DeferredSocket


class HandlerMixin(object):
    @property
    def handler_methods(self):
        if not hasattr(self, '_handler_methods'):
            if not getattr(self, '_refreshing_methods', False):
                return self.refresh_handler_methods()
            else:
                return OrderedDict()
        return self._handler_methods

    def get_handler(self, command):
        '''
        Check to see if a handler exists for a given command.  If the command
        is not found in the dictionary of handler methods, refresh the
        dictionary to be sure the handler is still not available.
        '''
        if not command in self._handler_methods:
            self.refresh_handler_methods()
        return self._handler_methods.get(command)

    def refresh_handler_methods(self):
        '''
        While requests can be handled manually from within the
        `process_request` method, as a convenience, callbacks are automatically
        registered based on the following method naming convention:

            def on__<request command>(self, env, request, response): ...

        This method (`refresh_handler_methods`) updates the current dictionary
        mapping each command name to the corresponding handler method, based on
        the handler method name.  The resulting dictionary is used by the
        `get_handler` method to check if a handler exists for a given command.
        '''
        self._refreshing_methods = True
        self._handler_methods = OrderedDict(sorted([(k[len('on__'):], v)
                for k, v in inspect.getmembers(self)
                if k.startswith('on__') and inspect.ismethod(v) or
                        inspect.isfunction(v)]))
        self._refreshing_methods = False
        return self._handler_methods


class ZmqRpcMixin(HandlerMixin):
    '''
    Exposes the class's methods through pickle-API over a ZeroMQ socket.

    Requests are multipart and of the form:

        [<timestamp>]
        [<command>]
        [<args>]
        [<kwargs>]
    '''
    @property
    def rpc_sock_name(self):
        if not getattr(self, '_rpc_sock_name', None):
            # Default sock name to 'rpc'
            setattr(self, '_rpc_sock_name', 'rpc')
        return self._rpc_sock_name

    @rpc_sock_name.setter
    def rpc_sock_name(self, value):
        self._rpc_sock_name = value

    def send_response(self, socks, response):
        '''
        The base modifier controller uses pickle encoding for messages, so here
        we must serialize the message before sending it.  The pickling is
        performed by `send_pyobj`.
        '''
        data = map(self.serialize_frame, response[:-1])
        try:
            error = self.serialize_frame(response[-1])
        except:
            error = self.serialize_frame(None)
        socks[self.rpc_sock_name].send_multipart(data + [error])

    def serialize_frame(self, frame):
        return pickle.dumps(frame)

    def deserialize_frame(self, frame):
        return pickle.loads(frame)

    def process_rpc_request(self, env, multipart_message):
        '''
        While requests can be handled manually by overriding this method,
        as a convenience, callbacks are automatically registered based on the
        following method naming convention:

            def on__<request command>(self, env, request, response): ...

        after the `refresh_handler_methods` has been called.
        '''
        timestamp, command, args, kwargs, result, error, error_str = [None] * 7
        try:
            timestamp, command, args, kwargs = map(self.deserialize_frame,
                                                multipart_message)
            handler = self.get_handler(command)
            if handler is None:
                raise ValueError, 'Unknown command: %s' % command
            args = args or tuple()
            kwargs = kwargs or {}
            result = handler(env, *args, **kwargs)
        except (Exception, ), error:
            import traceback
            # In the case of an exception, return a formatted string
            # representation of the error.
            error_str = traceback.format_exc().strip()
        data = ['%s' % datetime.now(), command, args, kwargs, result, error_str, error]
        self.send_response(env['socks'], data)


class ZmqJsonRpcMixin(ZmqRpcMixin):
    '''
    Exposes the class's methods through JSON-API over a ZeroMQ socket.
    '''
    def serialize_frame(self, frame):
        return jsonapi.dumps(frame)

    def deserialize_frame(self, frame):
        return jsonapi.loads(frame)


class ZmqRpcTaskBase(SockConfigsTask):
    '''
    Note that this class may only be used with either `ZmqRpcMixin` or
    `ZmqJsonRpcMixin`.
    '''
    def __init__(self, rpc_uri, on_run=None, control_pipe=None, **kwargs):
        self.uris = OrderedDict(rpc=rpc_uri)

        super(ZmqRpcTaskBase, self).__init__(self.get_sock_configs(),
                                         on_run=on_run,
                                         control_pipe=control_pipe)
        for k in self.sock_configs:
            if kwargs.get(k + '_bind', True):
                self.sock_configs[k].bind(self.uris[k])
            else:
                self.sock_configs[k].connect(self.uris[k])
        self.refresh_handler_methods()

    def get_sock_configs(self):
        return OrderedDict([
                ('rpc', DeferredSocket(zmq.REP)
                            .stream_callback('on_recv',
                                             self.process_rpc_request)),
        ])

    def on__available_handlers(self, env):
        return sorted(self.handler_methods.keys())



class ZmqRpcTask(ZmqRpcTaskBase, ZmqRpcMixin):
    pass


class ZmqJsonRpcTask(ZmqRpcTaskBase, ZmqJsonRpcMixin):
    pass


class DeferredRpcCommand(object):
    def __init__(self, command, rpc_uri):
        self.command = command
        self.rpc_uri = rpc_uri

    def _serialize_frame(self, frame):
        return pickle.dumps(frame)

    def _deserialize_frame(self, frame):
        return pickle.loads(frame)

    def __call__(self, *args, **kwargs):
        ctx = zmq.Context.instance()
        sock = zmq.Socket(ctx, zmq.REQ)
        sock.connect(self.rpc_uri)
        try:
            data = ['%s' % datetime.now(), self.command, args, kwargs]
            sock.send_multipart(map(self._serialize_frame, data))
            timestamp, command, args, kwargs, result, error_str, error = map(
                self._deserialize_frame, sock.recv_multipart())
            if error:
                raise error
            if error_str:
                raise RuntimeError, '''
Remote exception occurred:
------------------------------------------------------------------------
%s
========================================================================
                '''.strip() % (error_str, )

        finally:
            sock.close()
            del sock
            del ctx
        return result


class DeferredJsonRpcCommand(DeferredRpcCommand):
    def _serialize_frame(self, frame):
        return jsonapi.dumps(frame)

    def _deserialize_frame(self, frame):
        return jsonapi.loads(frame)


class ZmqRpcProxyBase(object):
    _deferred_command_class = None

    def __init__(self, rpc_uri):
        self._uris = OrderedDict(rpc=rpc_uri)
        self._refresh_handler_methods()

    def _refresh_handler_methods(self):
        self.__handler_methods = self._do_request('available_handlers')
        for m in self.__handler_methods:
            setattr(self, m, self._deferred_command_class(m, self._uris['rpc']))

    @property
    def _handler_methods(self):
        if not hasattr(self, '__handler_methods'):
            self._refresh_handler_methods()
        return self.__handler_methods

    def _do_request(self, command, *args, **kwargs):
        c = self._deferred_command_class(command, self._uris['rpc'])
        return c(*args, **kwargs)


class ZmqRpcProxy(ZmqRpcProxyBase):
    _deferred_command_class = DeferredRpcCommand


class ZmqJsonRpcProxy(ZmqRpcProxyBase):
    _deferred_command_class = DeferredJsonRpcCommand
