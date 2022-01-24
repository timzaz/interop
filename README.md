# Interop

A light-weight PubSub application for python asyncio using RabbitMQ that
can stand alone or be embedded in other async code.

## Usage

Setup:

- [PyEnv](https://github.com/pyenv/pyenv) and choose python >=3.10.1
- [Poetry](https://python-poetry.org/)
- [RabbitMQ](https://www.rabbitmq.com/)

```python
from interop import Interop

#: Be sure to load your env values

config: typing.Dict[str, typing.Any] = dict()  #: populate accordingly
interop = Interop(
    os.getenv("IMPORT_NAME"),
    os.getenv("RMQ_BROKER_URI", ""),
    type="publish"  #: or "subscribe"
)

#: later
async def start():
    await interop.init_app(app=config)
    #: ignore next line if embedded in an async application like Starlette
    await interop()

```

or scaffold standalone application with cli script:
```bash

interop init

```

also, see the `examples` folder

## Configuration Values
```bash

#: Import name for your application
#: CLI commands need the directory for your application to work
IMPORT_NAME="my_app"

#: Rabbit MQ
RMQ_HOST="localhost"
RMQ_PASSWORD="guest"
RMQ_PORT="5672"
#: Use amqps for secure connections
RMQ_SCHEME="amqp"
RMQ_USER="guest"

RMQ_URI="${RMQ_SCHEME}://${RMQ_USER}:${RMQ_PASSWORD}@${RMQ_HOST}:${RMQ_PORT}"
RMQ_BROKER_URI="${RMQ_URI}/%2F?connection_attempts=3&heartbeat=3600"

```

## Sample Publisher
```python

import asyncio
import typing

from interop import publish


@publish
async def sample(app: typing.Dict[str, typing.Any]):
    """This function monitors some external state and publishes directly to the
    rabbitmq broker.

    """

    while True:
        #: Add monitoring code here

        await asyncio.sleep(2)  #: Use appropriate sleep time

```

## Sample Subscriber
```python

from interop import Packet
from interop import subscribe


@subscribe("<routing-key>", "<exchange-name>")
async def sample(packet: Packet):
    """This function reacts to messages routed to <routing-key> at
    <exchange-name> on the rabbitmq broker.

    """

```
