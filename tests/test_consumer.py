from multiprocessing import Process, Pipe
import logging
from collections import OrderedDict

import zmq
from zmq.eventloop.ioloop import PeriodicCallback

from socket_group import DeferredSocket, EchoServer, get_run_context
from async_task_queue import AsyncJsonServerAdapter, get_uris
from utils import cleanup_ipc_uris, log_label


class Client(object):
    def __init__(self, req_uri, sub_uri):
        self.sock_configs = OrderedDict([
                ('req', DeferredSocket(zmq.REQ).connect(req_uri)),
                ('sub', DeferredSocket(zmq.SUB)
                            .connect(sub_uri)
                            .setsockopt(zmq.SUBSCRIBE, '')
                            .stream_callback('on_recv_stream',
                                             self.process_response))
        ])

    def process_response(self, socks, streams, stream, multipart_message):
        logging.getLogger(log_label(self)).debug(
                '%s %s' % (stream, multipart_message,))

    def do_request(self):
        self.socks['req'].send_multipart(['hello world'])
        response = self.socks['req'].recv_multipart()
        logging.getLogger(log_label(self)).debug(str(response))

    def run(self):
        ctx, io_loop, self.socks, streams = get_run_context(self.sock_configs)
        periodic_callback = PeriodicCallback(self.do_request, 1000, io_loop)

        try:
            periodic_callback.start()
            io_loop.start()
        except KeyboardInterrupt:
            pass


class JsonClient(Client):
    def do_request(self):
        self.socks['req'].send_json({'command': 'test_command'})
        response = self.socks['req'].recv_json()
        logging.getLogger(log_label(self)).info('%s' % response)

    def process_response(self, socks, streams, stream, multipart_message):
        super(JsonClient, self).process_response(socks, streams, stream,
                multipart_message)
        self.received_count += 1
        if self.received_count >= self.target_count:
            self.io_loop.stop()


    def run(self):
        self.target_count = 4
        self.received_count = 0
        ctx, io_loop, self.socks, streams = get_run_context(self.sock_configs)

        def dump_uris():
            logging.getLogger(log_label(self)).info(
                    'uris = %s' % get_uris(self.socks['req']))
        io_loop.add_callback(dump_uris)

        self.io_loop = io_loop

        periodic_callback = PeriodicCallback(self.do_request, 250, io_loop)

        try:
            periodic_callback.start()
            io_loop.start()
        except KeyboardInterrupt:
            pass
        logging.getLogger(log_label(self)).info('finished')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    echo_server = Process(target=EchoServer('ipc://ECHO:1', ).run)
    client = Process(target=JsonClient('ipc://PRODUCER_REP:1',
                                   'ipc://PRODUCER_PUB:1').run)
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
