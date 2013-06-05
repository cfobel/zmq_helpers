import logging
logging.basicConfig(level=logging.WARNING)
from functools import partial
import inspect
from collections import OrderedDict
try:
    import cPickle as pickle
except ImportError:
    import pickle
from datetime import datetime
from uuid import uuid1, uuid4

import zmq
from zmq.utils import jsonapi

from .socket_configs import SockConfigsTask, DeferredSocket
from .utils import log_label


def _available_handlers(obj, prefix, *args, **kwargs):
    return sorted(obj.handler_methods[prefix].keys())


def get_handler(obj, prefix, command):
    '''
    Check to see if a handler exists for a given command.  If the command
    is not found in the dictionary of handler methods, refresh the
    dictionary to be sure the handler is still not available.
    '''
    if not prefix in obj.handler_methods\
            or not command in obj.handler_methods[prefix]:
        refresh_handler_methods(obj, prefix)
    if command == 'available_handlers' and\
            not command in obj.handler_methods[prefix]:
        # The `available_handlers` has not been overridden, so perform the
        # default handling, which is to return a list of the available
        # handlers.
        handler = partial(_available_handlers, obj, prefix)
        obj._handler_methods[prefix]['available_handlers'] = handler
    return obj.handler_methods[prefix].get(command)


def refresh_handler_methods(obj, prefix):
    '''
    While requests can be handled manually from within the
    `process_request` method, as a convenience, callbacks are automatically
    registered based on the following method naming convention:

        def <handler_prefix><request command>(self, env, request,
                                                response): ...

    This function (`refresh_handler_methods`) updates the current dictionary
    mapping each command name to the corresponding handler method, based on
    the handler method name.  The resulting dictionary is used by the
    `get_handler` method to check if a handler exists for a given command.
    '''
    obj._refreshing_methods = True
    if not hasattr(obj, '_handler_methods'):
        obj._handler_methods = OrderedDict()
    obj._handler_methods[prefix] = OrderedDict(sorted([(k[len(prefix):], v)
            for k, v in inspect.getmembers(obj)
            if k.startswith(prefix)
                and inspect.ismethod(v) or inspect.isfunction(v)
    ]))
    obj._refreshing_methods = False
    return obj._handler_methods[prefix]


class BaseHandlerMixin(object):
    handler_prefixes = tuple()

    @property
    def handler_methods(self):
        if not hasattr(self, '_handler_methods'):
            if not getattr(self, '_refreshing_methods', False):
                self.refresh_handler_methods()
            else:
                return OrderedDict()
        return self._handler_methods

    def get_handler(self, command, prefix=None):
        '''
        Check to see if a handler exists for a given command.  If the command
        is not found in the dictionary of handler methods, refresh the
        dictionary to be sure the handler is still not available.
        '''
        if prefix is None:
            prefix = self.handler_prefixes[0]
        return get_handler(self, prefix, command)

    def refresh_handler_methods(self):
        for prefix in self.handler_prefixes:
            refresh_handler_methods(self, prefix)
        return self.handler_methods


class RpcHandlerMixin(BaseHandlerMixin):
    handler_prefixes = ('rpc__', )


class ZmqRpcMixin(RpcHandlerMixin):
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

    def send_response(self, socks, multipart_message, request, response):
        '''
        The base modifier controller uses pickle encoding for messages, so here
        we must serialize the message before sending it.  The pickling is
        performed by `send_pyobj`.
        '''
        # First element is sender uuid
        data = map(self.serialize_frame, request.values()[1:])
        # First element is timestamp, last element is error
        data += map(self.serialize_frame, response.values()[1:-1])
        try:
            error = self.serialize_frame(response.values()[-1])
        except:
            error = self.serialize_frame(None)
        data.insert(0, self.serialize_frame(response['timestamp']))
        data.append(error)
        try:
            logging.getLogger(log_label(self)).info(
                'request: uuid=%(sender_uuid)s command=%(command)s' % request)
        except:
            logging.getLogger(log_label(self)).info(request)
        socks[self.rpc_sock_name].send_multipart(data)

    def serialize_frame(self, frame):
        return pickle.dumps(frame)

    def deserialize_frame(self, frame):
        return pickle.loads(frame)

    def unpack_request(self, multipart_message):
        request_data = map(self.deserialize_frame, multipart_message)
        fields = ('sender_uuid', 'command', 'args', 'kwargs')
        result = OrderedDict((k, v) for k, v in zip(fields, request_data))
        return result

    def process_rpc_request(self, env, multipart_message):
        '''
        While requests can be handled manually by overriding this method,
        as a convenience, callbacks are automatically registered based on the
        following method naming convention:

            def on__<request command>(self, env, request, response): ...

        after the `refresh_handler_methods` has been called.
        '''
        response = OrderedDict(timestamp=str(datetime.now()))
        request = OrderedDict()
        logging.getLogger(log_label(self)).debug('%s' % multipart_message)
        try:
            request = self.unpack_request(multipart_message)
            handler = self.get_handler(request['command'], prefix='rpc__')
            if handler is None:
                raise ValueError, 'Unknown command: %s' % request['command']
            request['args'] = request['args'] or tuple()
            request['kwargs'] = request['kwargs'] or {}
            # Isolate handler call in `call_handler` method to allow subclasses
            # to perform special-handling, if necessary.
            response['result'] = self.call_handler(handler, env,
                                                   multipart_message, request)
        except (Exception, ), error:
            import traceback

            # Insert empty result to preserve response item count and order.
            response['result'] = None

            # In the case of an exception, return a formatted string
            # representation of the error.
            response['error_str'] = traceback.format_exc().strip()
            response['error'] = error
        # Fill in empty error fields if there was no error.
        response['error_str'] = response.get('error_str')
        response['error'] = response.get('error')
        # Fill in the first position in the `OrderedDict` with the current
        # time, i.e., when the request was completed.
        response['timestamp'] = str(datetime.now())
        self.send_response(env['socks'], multipart_message, request, response)

    def call_handler(self, handler, env, multipart_message, request):
        '''
        Isolate handler call in this method to allow subclasses to perform
        special-handling, if necessary.

        Note that the multipart-message is ignored by default.
        '''
        return handler(env, request['sender_uuid'], *request['args'],
                       **request['kwargs'])


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
    def __init__(self, **kwargs):
        super(ZmqRpcTaskBase, self).__init__(self.get_sock_configs(), **kwargs)
        self.uris = self.get_uris()
        self._bind_or_connect(kwargs)
        self.refresh_handler_methods()

    def _bind_or_connect(self, kwargs):
        for k in self.sock_configs:
            if kwargs.get(k + '_bind', True):
                self.sock_configs[k].bind(self.uris[k])
            else:
                self.sock_configs[k].connect(self.uris[k])

    def get_uris(self):
        raise NotImplementedError

    def get_sock_configs(self):
        return OrderedDict([
                ('rpc', DeferredSocket(zmq.REP)
                            .stream_callback('on_recv',
                                             self.process_rpc_request)),
        ])


class ZmqRpcTask(ZmqRpcMixin, ZmqRpcTaskBase):
    pass


class ZmqJsonRpcTask(ZmqJsonRpcMixin, ZmqRpcTaskBase):
    pass


class DeferredRpcCommand(object):
    def __init__(self, command, rpc_uri, uuid=None):
        self.command = command
        self.rpc_uri = rpc_uri
        self.uuid = uuid

    def _serialize_frame(self, frame):
        return pickle.dumps(frame)

    def _deserialize_frame(self, frame):
        return pickle.loads(frame)

    def __call__(self, *args, **kwargs):
        flags = kwargs.pop('__flags__', None)
        ctx = zmq.Context()
        sock = zmq.Socket(ctx, zmq.REQ)
        sock.connect(self.rpc_uri)
        try:
            self.send_request(sock, *args, **kwargs)
            response = self._unpack_response(self.recv_response(sock,
                                                                flags=flags))
            return response
        finally:
            sock.close()
            del sock
            del ctx

    def spawn(self, *args, **kwargs):
        import eventlet

        event = eventlet.event.Event()
        eventlet.spawn(self._green_call, event, *args, **kwargs)
        return event

    def _green_call(self, event, *args, **kwargs):
        import eventlet
        import zmq.green as gzmq

        none_on_error = kwargs.pop('_d_none_on_error', False)

        flags = gzmq.NOBLOCK
        ctx = gzmq.Context()
        sock = gzmq.Socket(ctx, gzmq.REQ)
        sock.connect(self.rpc_uri)
        self.send_request(sock, *args, **kwargs)
        try:
            while True:
                try:
                    response = self.recv_response(sock, flags=flags)
                    break
                except gzmq.ZMQError, e:
                    if e.errno != gzmq.EAGAIN:
                        raise
                    eventlet.sleep(0.01)
        finally:
            sock.close()
            del sock
            del ctx
        try:
            event.send(self._unpack_response(response))
        except:
            if none_on_error:
                event.send(None)
            else:
                raise

    def send_request(self, sock, *args, **kwargs):
        if self.uuid is None:
            uuid = str(uuid1())
            logging.getLogger(log_label(self)).info(
                'auto-assign uuid: %s' % uuid)
        else:
            uuid = self.uuid
        data = [uuid, self.command, args, kwargs]
        logging.getLogger(log_label(self)).debug(data)
        sock.send_multipart(map(self._serialize_frame, data))

    def recv_response(self, sock, flags=None):
        _kwargs = {}
        if flags:
            _kwargs['flags'] = flags
        return sock.recv_multipart(**_kwargs)

    def _unpack_response(self, response):
        timestamp, command, args, kwargs, result, error_str, error = map(
                self._deserialize_frame, response
        )
        if error_str:
            raise RuntimeError, '''
Remote exception occurred:
------------------------------------------------------------------------
%s
========================================================================
                '''.strip() % (error_str, )
        elif error:
            raise error
        return result


class DeferredJsonRpcCommand(DeferredRpcCommand):
    def _serialize_frame(self, frame):
        return jsonapi.dumps(frame)

    def _deserialize_frame(self, frame):
        return jsonapi.loads(frame)


class DeferredSingleFrameJsonRpcCommand(DeferredJsonRpcCommand):
    def _unpack_response(self, data):
        if 'error_str' in data and data['error_str']:
            raise RuntimeError, '''
Remote exception occurred:
------------------------------------------------------------------------
%s
========================================================================
                '''.strip() % (data['error_str'], )
        elif 'error' in data and data['error']:
            raise data['error']
        return data['result']

    def recv_response(self, sock, flags=None):
        _kwargs = {}
        if flags:
            _kwargs['flags'] = flags
        return sock.recv_json(**_kwargs)

    def send_request(self, sock, *args, **kwargs):
        if self.uuid is None:
            uuid = str(uuid1())
            logging.getLogger(log_label(self)).info(
                'auto-assign uuid: %s' % uuid)
        else:
            uuid = self.uuid
        data = {'uuid': uuid, 'command': self.command, 'args': args,
                'kwargs': kwargs, }
        logging.getLogger(log_label(self)).debug(data)
        sock.send(jsonapi.dumps(data))


class ZmqRpcProxyBase(object):
    _deferred_command_class = None

    def __init__(self, rpc_uri, uuid=None):
        if uuid is None:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid
        self._uris = OrderedDict(rpc=rpc_uri)
        self._refresh_handler_methods()

    def _refresh_handler_methods(self):
        self.__handler_methods = self._do_request('available_handlers')
        for m in self.__handler_methods:
            setattr(self, m, self._deferred_command_class(m, self._uris['rpc'], uuid=self.uuid))

    @property
    def _handler_methods(self):
        if not hasattr(self, '__handler_methods'):
            self._refresh_handler_methods()
        return self.__handler_methods

    def _do_request(self, command, *args, **kwargs):
        c = self._deferred_command_class(command, self._uris['rpc'], uuid=self.uuid)
        return c(*args, **kwargs)


class ZmqRpcProxy(ZmqRpcProxyBase):
    _deferred_command_class = DeferredRpcCommand


class ZmqJsonRpcProxy(ZmqRpcProxyBase):
    _deferred_command_class = DeferredJsonRpcCommand


class ZmqSingleFrameJsonRpcProxy(ZmqJsonRpcProxy):
    _deferred_command_class = DeferredSingleFrameJsonRpcCommand

    def __init__(self, *args, **kwargs):
        super(ZmqSingleFrameJsonRpcProxy, self).__init__(*args, **kwargs)


class ZmqRpcDemoTask(ZmqRpcTask):
    def __init__(self, rpc_uri, **kwargs):
        self.uris = OrderedDict(rpc=rpc_uri)
        super(ZmqRpcDemoTask, self).__init__(**kwargs)

    def get_uris(self):
        return self.uris
