# Overview

This project is a proof-of-concept asynchronous ZeroMQ server.

## Usage

In terminal 1, run:

```
    python server.py
```

[Optional] In second terminal, run:

```
    zmqc -rc SUB 'ipc://ASYNC_SERVER_PUB:1'
```

In a final terminal, run:

```
    python client.py "ipc://ASYNC_SERVER_REP:1"
```

### Expected behaviour

The client will connect to the server, make 5 asynchronous requests, and then
exit.  In the server terminal, the requests will be written to `stdout` as
they are processed by the server's `Consumer` class.  In the `zmqc` subscriber
terminal, any responses published by the server on the server's `PUB` port will
be dumped to `stdout`.

