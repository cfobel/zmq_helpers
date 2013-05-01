import logging
from collections import OrderedDict

import zmq
from zmq.utils import jsonapi
from .rpc import ZmqRpcTask, ZmqJsonRpcMixin
from .socket_configs import DeferredSocket
from .utils import log_label


class SingleFrameJson2Adapter(ZmqRpcTask):
    def __init__(self, rpc_uri, multiframe_uri, **kwargs):
        self.swap_contexts = OrderedDict()
        self.uris = OrderedDict([
            ('rpc', rpc_uri),
            ('multiframe', multiframe_uri),
        ])
        _kwargs = kwargs.copy()
        _kwargs['multiframe_bind'] = False
        super(SingleFrameJson2Adapter, self).__init__(**_kwargs)

    def get_uris(self):
        return self.uris

    def get_sock_configs(self):
        sock_configs = super(SingleFrameJson2Adapter, self).get_sock_configs()
        sock_configs['multiframe'] =\
            DeferredSocket(zmq.REQ).stream_callback('on_recv',
                                                    self.process_multiframe_response)
        return sock_configs

    def process_rpc_request(self, env, multipart_message):
        message = jsonapi.loads(multipart_message[0])
        logging.getLogger(log_label()).info(message)
        data = OrderedDict()
        for k in ('uuid', 'command', 'args', 'kwargs'):
            data[k] = message.get(k)
        env['socks']['multiframe'].send_multipart(map(self.serialize_frame,
                                                      data.values()))

    def process_multiframe_response(self, env, multipart_message):
        timestamp, command, args, kwargs, result, error_str, error = map(
                self.deserialize_frame, multipart_message
        )
        data = {'timestamp': timestamp, 'command': command, 'args': args,
                'kwargs': kwargs, 'result': result, 'error_str': error_str,
                'error': error}
        data = self._filter_multiframe_response_fields(data)
        try:
            json_data = jsonapi.dumps(data)
        except Exception, e:
            import traceback
            data = {'result': None, 'error_str': traceback.format_exc()}
            json_data = jsonapi.dumps(data)
        env['socks']['rpc'].send(json_data)

    def _filter_multiframe_response_fields(self, response_data):
        return dict([(k, v) for k, v in response_data.iteritems()
                     if k != 'error'])


class SingleFrameJson2JsonAdapter(SingleFrameJson2Adapter, ZmqJsonRpcMixin):
    def _filter_multiframe_response_fields(self, response_data):
        return dict([(k, v) for k, v in response_data.items() if k != 'error'])


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser
    parser = ArgumentParser(description="""description""")
    parser.add_argument('-j', '--json_remote_rpc', dest='json_remote_rpc',
                        action='store_true')
    parser.add_argument(nargs=1, dest='single_frame_rpc_uri', type=str)
    parser.add_argument(nargs=1, dest='multiframe_rpc_uri', type=str)
    args = parser.parse_args()
    args.single_frame_rpc_uri = args.single_frame_rpc_uri[0]
    args.multiframe_rpc_uri = args.multiframe_rpc_uri[0]
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)

    args = parse_args()
    logging.getLogger(log_label()).info(args)

    if args.json_remote_rpc:
        m = SingleFrameJson2JsonAdapter(args.single_frame_rpc_uri,
                                        args.multiframe_rpc_uri)
    else:
        m = SingleFrameJson2Adapter(args.single_frame_rpc_uri,
                                    args.multiframe_rpc_uri)
    m.run()
