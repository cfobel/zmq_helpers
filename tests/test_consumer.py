from __future__ import division
from multiprocessing import Process, Pipe
import logging
from collections import OrderedDict
from datetime import timedelta

import zmq
from zmq.eventloop.ioloop import PeriodicCallback

from zmq_helpers.socket_configs import DeferredSocket, EchoServer, get_run_context
from zmq_helpers.async import AsyncJsonServerAdapter, get_uris
from zmq_helpers.utils import cleanup_ipc_uris, log_label


class Client(object):
    def __init__(self, req_uri, sub_uri, target_count=5, request_delay=1):
        self.sock_configs = OrderedDict([
                ('req', DeferredSocket(zmq.REQ).connect(req_uri)),
                ('sub', DeferredSocket(zmq.SUB)
                            .connect(sub_uri)
                            .setsockopt(zmq.SUBSCRIBE, '')
                            .stream_callback('on_recv_stream',
                                             self.process_response))
        ])
        self.target_count = target_count
        self.request_delay = request_delay

    def process_response(self, env, stream, multipart_message):
        logging.getLogger(log_label(self)).debug(
                '%s %s' % (stream, multipart_message,))
        self.received_count += 1
        if self.received_count >= self.target_count:
            env['io_loop'].stop()

    def do_request(self):
        self.socks['req'].send_multipart(['hello world'])
        response = self.socks['req'].recv_multipart()
        logging.getLogger(log_label(self)).debug(str(response))

    def run(self):
        self.received_count = 0
        ctx, io_loop, self.socks, streams = get_run_context(self.sock_configs)
        self.io_loop = io_loop

        if self.request_delay > 0:
            periodic_callback = PeriodicCallback(self.do_request,
                    self.request_delay * 1000., io_loop)
            io_loop.add_timeout(timedelta(seconds=0.1), lambda: periodic_callback.start())
        else:
            for i in range(self.target_count):
                io_loop.add_callback(self.do_request)

        try:
            io_loop.start()
        except KeyboardInterrupt:
            pass


class JsonClient(Client):
    def do_request(self):
        self.socks['req'].send_json({'command': 'test_command'})
        response = self.socks['req'].recv_json()
        logging.getLogger(log_label(self)).info('%s' % response)

    def run(self):
        response = super(JsonClient, self).run()
        logging.getLogger(log_label(self)).info(
                    'uris = %s' % get_uris(self.socks['req']))
        logging.getLogger(log_label(self)).info('finished')
        return response


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser
    parser = ArgumentParser(description="""description""")
    parser.add_argument('-r', '--request_delay', dest='request_delay', type=float,
            default=1, help='Number of seconds (float) to delay between client '
                            'requests. (default=%(default)s)')
    parser.add_argument('-d', '--echo_delay', dest='echo_delay', type=float,
            default=0, help='Number of seconds (float) to delay in the '
            'echo server.  Use to simulate a slow backend server. '
            '(default=%(default)s)')
    parser.add_argument(nargs='?', dest='request_count', type=int)
    args = parser.parse_args()
    return args


def main():
    logging.basicConfig(level=logging.INFO)

    args = parse_args()
    print args

    echo_server = Process(target=EchoServer('ipc://ECHO:1',
            args.echo_delay).run)
    client = Process(target=JsonClient('ipc://PRODUCER_REP:1',
                                       'ipc://PRODUCER_PUB:1',
                                       args.request_count,
                                       request_delay=args.request_delay).run)
    parent_pipe, child_pipe = Pipe(duplex=True)
    async_server_adapter = Process(
        target=AsyncJsonServerAdapter('ipc://ECHO:1', 'ipc://PRODUCER_REP:1',
                                      'ipc://PRODUCER_PUB:1', child_pipe).run
    )

    try:
        echo_server.start()
        async_server_adapter.start()
        client.start()
        client.join()
        parent_pipe.send('close')
        async_server_adapter.join()
    except KeyboardInterrupt:
        pass
    client.terminate()
    async_server_adapter.terminate()
    echo_server.terminate()
    # Since ipc:// creates files, delete them on exit
    cleanup_ipc_uris(('ipc://ECHO:1', 'ipc://PRODUCER_REP:1',
            'ipc://PRODUCER_PUB:1'))


if __name__ == '__main__':
    main()
