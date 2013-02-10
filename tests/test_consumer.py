from multiprocessing import Process
import logging
from collections import OrderedDict

import zmq

from socket_group import DeferredSocket, EchoServer, get_run_context,\
        PeriodicCallback
from async_task_queue import AsyncJsonServerAdapter, get_uris


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
        logging.debug('[CLIENT:process_response] %s %s' % (stream,
                                                             multipart_message,
                                                             ))

    def do_request(self):
        self.socks['req'].send_multipart(['hello world'])
        response = self.socks['req'].recv_multipart()
        logging.debug('[CLIENT:do_request] %s' % response)

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
        logging.debug('[CLIENT:do_request] %s' % response)

    def run(self):
        ctx, io_loop, self.socks, streams = get_run_context(self.sock_configs)
        periodic_callback = PeriodicCallback(self.do_request, 1000, io_loop)
        def dump_uris():
            logging.info('[JSON_PRODUCER] uris = %s' % get_uris(self.socks['req']))
        io_loop.add_callback(dump_uris)

        try:
            periodic_callback.start()
            io_loop.start()
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    echo_server = Process(target=EchoServer('ipc://ECHO:1', ).run)
    client = Process(target=JsonClient('ipc://PRODUCER_REP:1',
                                   'ipc://PRODUCER_PUB:1').run)
    async_server_adapter = Process(
        target=AsyncJsonServerAdapter('ipc://ECHO:1', 'ipc://PRODUCER_REP:1',
                                  'ipc://PRODUCER_PUB:1').run
    )

    try:
        echo_server.start()
        async_server_adapter.start()
        client.start()
        client.join()
        async_server_adapter.join()
        echo_server.join()
    except KeyboardInterrupt:
        async_server_adapter.terminate()
        client.terminate()
        echo_server.terminate()
