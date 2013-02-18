import sys
import logging
from uuid import uuid4

from path import path


def log_label(obj=None, function_name=True):
    parts = []
    if obj:
        parts += [obj.__class__.__module__, obj.__class__.__name__]
    if function_name:
        parts += [callersname()]
    if parts[0] == '__main__':
        del parts[0]
    return '[%s]' % '.'.join(parts)


def unique_ipc_uri():
    return 'ipc:///tmp/' + uuid4().hex


def cleanup_ipc_uris(uris):
    for uri in uris:
        if uri.startswith('ipc://'):
            uri = path(uri.replace('ipc://', ''))
            if uri.exists():
                logging.getLogger('[utils.cleanup_ipc_uris]').debug('remove: %s' % uri)
                uri.remove()

## {{{ http://code.activestate.com/recipes/66062/ (r1)
# use sys._getframe() -- it returns a frame object, whose attribute
# f_code is a code object, whose attribute co_name is the name:

def whoami():
    return sys._getframe(1).f_code.co_name

# this uses argument 1, because the call to whoami is now frame 0.
# and similarly:
def callersname():
    return sys._getframe(2).f_code.co_name


def get_available_port(interface_addr='*'):
    import zmq
    ctx = zmq.Context.instance()
    sock = zmq.Socket(ctx, zmq.PUSH)
    port = sock.bind_to_random_port('tcp://' + interface_addr)
    return port


def get_random_tcp_uri(addr):
    port = get_available_port(addr)
    return 'tcp://%s:%s' % (addr, port)
