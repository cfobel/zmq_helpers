import sys
import logging

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
