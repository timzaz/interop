import os

import dotenv

try:
    #: Set the environment variables
    env_dir = None
    path = dotenv.find_dotenv(".env.local", usecwd=True)

    if path and env_dir is None:
        env_dir = os.path.dirname(path)
        dotenv.load_dotenv(path)

    #: The remainder of the code should be run from the .env.local directory
    if env_dir and os.getcwd() != env_dir:
        os.chdir(env_dir)
finally:
    import asyncio
    import os
    import signal
    import sys
    import typing
    from uuid import uuid4

    from interop import Exchanges
    from interop import Interop
    from interop import Packet
    from interop import publish
    from interop import subscribe
    from interop.utils import now


@publish
async def emitter(app: typing.Dict[str, typing.Any]):
    while True:
        now(
            {"message": "Some generic info message", "sync": uuid4().hex},
            Exchanges.NOTIFY.value,
            "simple.message",
        )

        await asyncio.sleep(2)


print(emitter)


def make_handler(name: str):
    async def print_log(packet: Packet):
        log_record_message = typing.cast(
            typing.Dict[str, typing.Any], packet.data
        )["message"]

        log_record_sync = typing.cast(
            typing.Dict[str, typing.Any], packet.data
        )["sync"]

        print(
            f"{name} handled: {packet.exchange} ({packet.routing_key})"
            f"    {log_record_sync} {log_record_message}"
        )

    return print_log


subscribe("simple.#", Exchanges.NOTIFY.value)(make_handler("Logger"))
the_interop = Interop(
    "examples.simple",
    os.getenv("RMQ_BROKER_URI", ""),
    type="publish"
)


def signint_handler(p0, p1):
    print("stoppiing")
    the_interop.publisher.stop()
    the_interop.subscriber.stop()

    sys.exit(0)


async def main():
    await the_interop.init_app(
        app={
            "DEBUG": True,
            "IMPORT_NAME": "examples.simple",
            "RMQ_BROKER_URI": os.getenv("RMQ_BROKER_URI"),
        },
    )

    await the_interop()


if __name__ == "__main__":

    signal.signal(signal.SIGINT, signint_handler)
    asyncio.run(main(), debug=True)
