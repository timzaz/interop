import os

import dotenv

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

    import uvicorn
    from asgiref.typing import ASGI3Application
    from fastapi.applications import FastAPI

finally:
    import asyncio
    import os
    import typing
    from uuid import uuid4

    from interop import Exchanges
    from interop import Interop
    from interop import Packet
    from interop import interop_ready
    from interop.utils import now


@interop_ready.connect
def register(sender: Interop):
    """Register subscriber consumers and crunchers."""

    sender.add_handler(
        "simple.#", Exchanges.NOTIFY.value, make_handler("Error")
    )
    sender.add_cruncher(emitter)


async def emitter(app):
    while True:
        now(
            {"message": "Some generic info message", "sync": uuid4().hex},
            Exchanges.NOTIFY.value,
            "simple.message",
        )

        await asyncio.sleep(2)


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


the_interop = Interop(name="Simple Interop")

app = FastAPI(
    description="Interop embedded in a web application.",
    docs_url="/_api/docs",
    openapi_url="/_api/openapi.json",
    redoc_url="/_api/redoc",
    title="Interop API",
    version="1.0.0",
)
app.debug = True


@app.get("/")
async def index():
    return {"Hello": "World"}


@app.on_event("startup")
async def startup_event():
    await the_interop.init_app(
        root_path=os.getcwd(),
        app={
            "DEBUG": True,
            "IMPORT_NAME": "examples.simple",
            "RMQ_BROKER_URI": os.getenv("RMQ_BROKER_URI"),
        },
    )


@app.on_event("shutdown")
async def shutdown_event():
    the_interop.publisher.stop()
    the_interop.subscriber.stop()


uvicorn.run(
    typing.cast(ASGI3Application, app), host="0.0.0.0", port=8000, debug=True
)
