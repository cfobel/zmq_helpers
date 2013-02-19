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


def get_available_ports(count=1, interface_addr='*', exclude=None):
    import zmq

    if exclude is None:
        exclude = []
    exclude = set(exclude)
    ctx = zmq.Context()
    socks = []
    ports = []

    def bind_socket(ctx, port=None):
        sock = zmq.Socket(ctx, zmq.PUSH)
        if port is None:
            port = sock.bind_to_random_port('tcp://' + interface_addr)
        return port, sock

    preferred_ports_path = path('~/.zmq_helpers/preferred_ports.txt').expand()
    if preferred_ports_path.isfile():
        for p in map(int, preferred_ports_path.lines()):
            if len(ports) >= count:
                break
            elif p in exclude:
                continue
            try:
                port, sock = bind_socket(ctx, p)
                ports.append(p)
            except zmq.ZMQError:
                pass
            finally:
                exclude.add(p)

    while len(ports) < count:
        port, sock = bind_socket(ctx)
        if port not in exclude:
            ports.append(port)
        socks.append(sock)
    for sock in socks:
        sock.close()
    del ctx
    return ports


def get_random_tcp_uris(addr, count=1, exclude_ports=None):
    if exclude_ports is None:
        exclude_ports = []
    ports = get_available_ports(count, addr, exclude_ports)
    return ['tcp://%s:%s' % (addr, port) for port in ports]


def get_random_tcp_uri(addr, exclude_ports=None):
    return get_random_tcp_uris(addr, exclude_ports=exclude_ports)[0]
