import importlib
import inspect
import os
import types
import typing

import typer
from jinja2 import Environment

from .utils import get_import_name
from .utils import get_templates_directory
from .utils import snake_case


class SubscriberCli(typer.Typer):
    def __init__(
        self,
        *,
        help: typing.Optional[str] = "Scaffolds a new subscriber.",
        name: typing.Optional[str] = "subscriber",
        **kwargs: typing.Any,
    ):

        super().__init__(name=name, help=help, **kwargs)
        self.callback(invoke_without_command=True)(self.subscriber)

    def subscriber(
        self,
        *,
        name: str = typer.Argument(
            ...,
            help="The name of the subscriber.",
        ),
    ):
        """Scaffolds a new subscriber"""

        import_name = get_import_name()
        default_module = f"{import_name}.subscribers"

        module: str = typer.prompt(
            "The module at whose directory the subscriber will be placed",
            default=default_module,
        )

        subscriber_dir: str | None = None
        try:
            if module == default_module:
                import_module: typing.Optional[
                    types.ModuleType
                ] = importlib.import_module(import_name)
                subscriber_dir = os.path.join(
                    os.path.dirname(inspect.getfile(import_module)),
                    "subscribers"
                )

                if not os.path.exists(subscriber_dir):
                    os.mkdir(subscriber_dir)

                    init_dir: str = os.path.join(subscriber_dir, "__init__.py")
                    if not os.path.exists(init_dir):
                        with open(init_dir, "w") as file:
                            file.write("")
            else:
                module_module: typing.Optional[
                    types.ModuleType
                ] = importlib.import_module(module)
                subscriber_dir = os.path.dirname(
                    inspect.getfile(module_module)
                )
        except:  # noqa
            typer.echo(f"Could not import module: {module}\n\n")
            raise typer.Abort()

        if not subscriber_dir:
            typer.echo("Could not deduce subscriber directory.\n\n")
            raise typer.Abort()

        dir = subscriber_dir
        env = Environment()
        funcname: str = snake_case(name).replace("-", "_")
        kwargs = {"name": funcname}
        templates_dir: str = get_templates_directory()

        working_file: str = f"{funcname}.py"
        with open(os.path.join(dir, working_file), "w") as file:
            with open(
                os.path.join(templates_dir, "subscriber.py.jinja2")
            ) as jinja_source:
                template = env.from_string(jinja_source.read())
                file.write(template.render(**kwargs))
