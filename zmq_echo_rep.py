#!/usr/bin/env python

import sys
import zmq

ctx = zmq.Context()
poll = zmq.Poller()

addrs = {}

for addr in sys.argv[1:]:
    print "Binding", addr
    sock = ctx.socket(zmq.REP)
    addrs[sock] = addr
    sock.bind(addr)
    poll.register(sock, zmq.POLLIN)

while True:
    print "Running..."
    items = dict(poll.poll())
    for sock in items:
        msg = sock.recv_multipart()
        addr = addrs[sock]
        print "I (%s): %r" % (addr, msg)
        sock.send_multipart(msg)
