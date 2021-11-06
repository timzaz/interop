import dotenv
import os

try:
    #: Set the environment variables
    env_dir = None
    path = dotenv.find_dotenv(".env", usecwd=True)

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
    from interop import interop_ready
    from interop.utils import Config
    from interop.utils import now


@interop_ready.connect
def register(sender: Interop):
    """Register subscriber consumers and crunchers.

    """

    sender.add_handler("#", Exchanges.NOTIFY.value, make_handler("All"))
    sender.add_handler(
        "ERROR.#",
        Exchanges.NOTIFY.value,
        make_handler("Error")
    )
    sender.add_handler(
        "INFO.#",
        Exchanges.NOTIFY.value,
        make_handler("Info")
    )
    sender.add_handler(
        "WARNING.#",
        Exchanges.NOTIFY.value,
        make_handler("Warning")
    )
    sender.add_cruncher(emitter_info)
    sender.add_cruncher(emitter_error)
    sender.add_cruncher(emitter_warn)


async def emitter_info(app):
    while True:
        now(
            {
                "message": "Some generic info message",
                "sync": uuid4().hex
            },
            Exchanges.NOTIFY.value,
            "INFO.message"
        )

        await asyncio.sleep(2)


async def emitter_error(app):
    while True:
        now(
            {
                "message": "Some generic error message",
                "sync": uuid4().hex
            },
            Exchanges.NOTIFY.value,
            "ERROR.message"
        )

        await asyncio.sleep(2)


async def emitter_warn(app):

    while True:
        now(
            {
                "message": "Some generic warning message",
                "sync": uuid4().hex
            },
            Exchanges.NOTIFY.value,
            "WARNING.message"
        )

        await asyncio.sleep(2)


def make_handler(name: str):
    async def print_log(packet: Packet):
        log_record_message = typing.cast(
            typing.Dict[str, typing.Any],
            packet.data
        )['message']

        log_record_sync = typing.cast(
            typing.Dict[str, typing.Any],
            packet.data
        )['sync']

        print(
            f"{name} handled: {packet.exchange} ({packet.routing_key})"
            f"    {log_record_sync} {log_record_message}"
        )

    return print_log


the_interop = Interop(name="Simple Interop")


def signint_handler(p0, p1):
    the_interop.publisher.stop()
    the_interop.subscriber.stop()

    sys.exit(0)


async def main():
    await the_interop.init_app(
        Config(
            root_path=os.getcwd(),
            defaults={
                "DEBUG": True,
                "IMPORT_NAME": "examples.simple",
                "RMQ_BROKER_URI": os.getenv("RMQ_BROKER_URI")
            }
        )
    )

    await the_interop()


if __name__ == "__main__":

    signal.signal(signal.SIGINT, signint_handler)
    asyncio.run(main(), debug=True)
