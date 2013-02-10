from datetime import datetime
from multiprocessing import Process
from uuid import uuid4
import time
from collections import OrderedDict
import logging

import zmq
from zmq.utils import jsonapi

from socket_group import DeferredSocket, SockConfigsTask


class Consumer(SockConfigsTask):
    def __init__(self, req_uri, push_uri, pull_uri, delay=0, push_bind=True,
                 pull_bind=True):
        self.delay = delay
        self.uris = OrderedDict([
                ('req', req_uri),
                ('push', push_uri),
                ('pull', pull_uri),
        ])
        self.sock_configs = OrderedDict([
                ('req', DeferredSocket(zmq.REQ).connect(req_uri)),
                ('push', DeferredSocket(zmq.PUSH)),
                ('pull', DeferredSocket(zmq.PULL)
                            .stream_callback('on_recv_stream', self.process_input)),
        ])
        if push_bind:
            self.sock_configs['push'].bind(push_uri)
        else:
            self.sock_configs['push'].connect(push_uri)
        if pull_bind:
            self.sock_configs['pull'].bind(pull_uri)
        else:
            self.sock_configs['pull'].connect(pull_uri)

    def process_input(self, socks, streams, stream, multipart_message):
        logging.debug('[CONSUMER:process_input] %s %s' % (stream,
                                                          multipart_message, ))
        async_id = multipart_message[0]
        message = multipart_message[1:]
        socks['req'].send_multipart(message)
        response = socks['req'].recv_multipart()
        if self.delay > 0:
            time.sleep(self.delay)
        socks['push'].send_multipart([async_id] + response)
        logging.debug('  \--> req response: %s' % response)


class Producer(SockConfigsTask):
    def __init__(self, rep_uri, pub_uri, push_uri, pull_uri, push_bind=False,
                 pull_bind=False):
        self.uris = OrderedDict([
            ('rep', rep_uri),
            ('pub', pub_uri),
            ('push', push_uri),
            ('pull', pull_uri),
        ])
        self.sock_configs = OrderedDict([
            ('push', DeferredSocket(zmq.PUSH)),
            ('pull', DeferredSocket(zmq.PULL)
                        .stream_callback('on_recv_stream', self.process_response)
            ),
            ('rep', DeferredSocket(zmq.REP)
                        .bind(rep_uri)
                        .stream_callback('on_recv_stream', self.process_request)
            ),
            ('pub', DeferredSocket(zmq.PUB).bind(pub_uri))
        ])

        if push_bind:
            self.sock_configs['push'].bind(push_uri)
        else:
            self.sock_configs['push'].connect(push_uri)
        if pull_bind:
            self.sock_configs['pull'].bind(pull_uri)
        else:
            self.sock_configs['pull'].connect(pull_uri)

    def process_response(self, socks, streams, stream, multipart_message):
        logging.debug('[PRODUCER:process_response] %s %s' % (stream,
                                                             multipart_message,
                                                             ))
        socks['pub'].send_multipart(multipart_message)

    def process_request(self, socks, streams, stream, multipart_message):
        logging.debug('[PRODUCER:process_request] %s %s' % (stream,
                                                            multipart_message,
                                                            ))
        async_id = '[%s] %s' % (datetime.now(), uuid4())
        socks['rep'].send_multipart([async_id] + multipart_message)
        socks['push'].send_multipart([async_id] + multipart_message)


class JsonProducer(Producer):
    def process_response(self, socks, streams, stream, multipart_message):
        '''
        Extract async_id from first frame of the message, strip the first frame
        from the message, and embed the id into the JSON-encoded message.
        '''
        logging.debug('[JSON_PRODUCER:process_response] %s %s' % (stream,
                                                             multipart_message,
                                                             ))
        # multipart_message should have two parts: 1) async_id, 2) JSON-message
        assert(len(multipart_message) == 2)
        async_id = multipart_message[0]
        message = jsonapi.loads(multipart_message[1])
        message['async_id'] = async_id
        socks['pub'].send_json(message)

    def process_request(self, socks, streams, stream, multipart_message):
        '''
        Generate a unique async_id and:
            1) Add it as the first frame the message before sending to the PUSH socket
            2) Embed the async_id into the JSON-encoded message before sending
                the response on the REP socket.  The response to the REP socket
                is intended as an acknowledgement of the request (not the
                result).  The result will be published to the PUB socket once
                the request has been processed.
        '''
        logging.debug('[JSON_PRODUCER:process_request] %s %s' % (stream,
                                                            multipart_message,
                                                            ))
        assert(len(multipart_message) == 1)
        request = jsonapi.loads(multipart_message[0])
        response = request.copy()
        if request['command'] in ('pub_uri', 'rep_uri', 'pull_uri',
                                  'push_uri'):
            response['type'] = 'result'
            response['result'] = self.uris[request['command'].split('_')[0]]
        else:
            async_id = '[%s] %s' % (datetime.now(), uuid4())
            response['type'] = 'async'
            response['async_id'] = async_id
            socks['push'].send_multipart([async_id] + multipart_message)
        socks['rep'].send_json(response)


class AsyncServerAdapter(object):
    producer_class = Producer

    def __init__(self, backend_rep_uri, frontend_rep_uri, frontend_pub_uri):
        unique_ipc_uri = lambda: 'ipc://' + uuid4().hex
        self.uris = OrderedDict([
            ('backend_rep', backend_rep_uri),
            ('consumer_push_be', unique_ipc_uri()),
            ('consumer_pull_be', unique_ipc_uri()),
            ('frontend_rep_uri', frontend_rep_uri),
            ('frontend_pub_uri', frontend_pub_uri)
        ])
        logging.info("uris: %s", self.uris)

    def run(self):
        consumer = Process(target=Consumer(self.uris['backend_rep'],
                                        self.uris['consumer_push_be'],
                                        self.uris['consumer_pull_be'],
                                        0.5).run
        )
        producer = Process(target=self.producer_class(
                self.uris['frontend_rep_uri'],
                self.uris['frontend_pub_uri'],
                self.uris['consumer_pull_be'],
                self.uris['consumer_push_be']).run
        )
        try:
            consumer.start()
            producer.start()
            producer.join()
            consumer.join()
        except KeyboardInterrupt:
            producer.terminate()
            consumer.terminate()


class AsyncJsonServerAdapter(AsyncServerAdapter):
    producer_class = JsonProducer


def get_uris(sock):
    uris = {}
    for uri_label in ('rep', 'push', 'pull', 'pub'):
        sock.send_json({"command": '%s_uri' % uri_label})
        response = sock.recv_json()
        uris[uri_label] = response['result']
    return uris
