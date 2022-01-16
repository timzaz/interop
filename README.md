# Interop

A light-weight PubSub application for python asyncio using RabbitMQ that
can stand alone or be embedded in other async code.

## Usage
See examples folder

## Configuration Values
```bash
RMQ_HOST="localhost"
RMQ_PASSWORD="guest"
RMQ_PORT="5672"
RMQ_SCHEME="amqp"
RMQ_USER="guest"
RMQ_BROKER_URI="${RMQ_SCHEME}://${RMQ_USER}:${RMQ_PASSWORD}@${RMQ_HOST}:${RMQ_PORT}/%2F?connection_attempts=3&heartbeat=3600"

```