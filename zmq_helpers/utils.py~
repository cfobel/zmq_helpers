import urllib2
import sys
import logging
from uuid import uuid4

import zmq
from path import path


# URL for service names and port numbers, as defined in Internet Engineering
# Task Force (IETF) [RFC6335][1], which:
#
# > defines the procedures that the Internet Assigned Numbers Authority
# > (IANA) uses when handling assignment and other requests related to
# > the Service Name and Transport Protocol Port Number registry".
#
# [1]: http://tools.ietf.org/html/rfc6335
IANA_LIST_URL = ('http://www.iana.org/assignments/'
                 'service-names-port-numbers/'
                 'service-names-port-numbers.csv')


def bind_to_random_port(sock, port_ranges=None):
    if port_ranges is None:
        port_ranges = get_unassigned_port_ranges()
    for i, port_range in port_ranges.iterrows():
        for port in xrange(port_range.start, port_range.end + 1):
            try:
                sock.bind('tcp://*:%d' % port)
                return port
            except zmq.ZMQError:
                pass
    raise


def get_unassigned_port_ranges(csv_path=None):
    import pandas as pd

    if csv_path is None:
        import pkg_resources

        base_path = path(pkg_resources.resource_filename('zmq_helpers', ''))
        # Load cached unassigned port ranges, as defined in Internet
        # Engineering Task Force (IETF) [RFC6335][1], which:
        #
        # > defines the procedures that the Internet Assigned Numbers Authority
        # > (IANA) uses when handling assignment and other requests related to
        # > the Service Name and Transport Protocol Port Number registry".
        #
        # [1]: http://tools.ietf.org/html/rfc6335
        cache_path = base_path.joinpath('static',
                                        'service-names-port-numbers.h5')
        return pd.read_hdf(str(cache_path), '/unassigned_ranges')

    df = pd.read_csv(csv_path)
    unassigned = df.loc[df.Description.str.lower() == 'unassigned'].copy()
    port_ranges = pd.DataFrame([[int(x), int(x)]
                                if '-' not in x else map(int, x.split('-'))
                                for x in unassigned['Port Number']],
                               columns=['start', 'end'])
    port_ranges['count'] = port_ranges.end - port_ranges.start
    port_ranges['inv_mod'] = 10 - (port_ranges['start'] % 10)
    port_ranges.sort(['inv_mod', 'count'], inplace=True)
    return port_ranges.drop('inv_mod', axis=1)[::-1].reset_index(drop=True)


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


def test_port(addr, port=None):
    import socket
    from random import randint

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if port is None:
        port = randint(1024, 65535)
    s.bind((addr, port))
    return port


def get_available_ports(count=1, interface_addr='*', exclude=None):
    from random import randint

    if exclude is None:
        exclude = []
    exclude = set(exclude)
    ports = []

    if interface_addr == '*':
        addr = 'localhost'
    else:
        addr = interface_addr

    preferred_ports_path = path('~/.zmq_helpers/preferred_ports.txt').expand()
    preferred_ports = []
    if preferred_ports_path.isfile():
        preferred_ports += map(int, preferred_ports_path.lines())

    while len(ports) < count:
        try:
            if preferred_ports:
                port = preferred_ports.pop(0)
            else:
                port = randint(1024, 65535)
            if port not in exclude:
                port = test_port(addr, port)
                ports.append(port)
                exclude.add(port)
        except (Exception, ), e:
            #import traceback
            #traceback.print_exc()
            pass
    return ports


def get_random_tcp_uris(addr, count=1, exclude_ports=None):
    if exclude_ports is None:
        exclude_ports = []
    ports = get_available_ports(count, addr, exclude_ports)
    return ['tcp://%s:%s' % (addr, port) for port in ports]


def get_random_tcp_uri(addr, exclude_ports=None):
    return get_random_tcp_uris(addr, exclude_ports=exclude_ports)[0]


def get_public_ip():
    return urllib2.urlopen('http://myip.dnsomatic.com').read()
