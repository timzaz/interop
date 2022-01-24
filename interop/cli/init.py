import os
import typer
import typing

from enum import Enum
from jinja2 import Environment

from .utils import get_templates_directory
from .utils import snake_case
from .utils import space_case


class InteropType(str, Enum):
    publish = "publish"
    subscribe = "subscribe"


def complete_type():
    return ["publish", "subscribe"]


class InitCli(typer.Typer):
    def __init__(
        self,
        *,
        help: typing.Optional[str] = "Initialises a new interop application.",
        name: typing.Optional[str] = "init",
        **kwargs: typing.Any,
    ):

        super().__init__(name=name, help=help, **kwargs)
        self.callback(
            invoke_without_command=True
        )(self.init)

        self.app_dir = os.getcwd()

    def init(
        self,
        *,
        name: str = typer.Argument(
            ...,
            help="The name of the application.",
        )
    ):
        """Initialises a new interop application"""

        app_folder: str = os.path.join(os.getcwd(), name)
        if os.path.exists(app_folder):
            typer.echo(
                typer.style(
                    "\nLocation not empty", fg=typer.colors.RED, bold=True
                ),
                color=True
            )
            raise typer.Abort()

        templates_dir: str = get_templates_directory()

        #: Variables
        import_name: str = snake_case(name).replace("-", "_")
        app: str = space_case(import_name).title()
        author: str = typer.prompt("Author")
        email: str = typer.prompt("Author Email")
        type = typer.prompt(
            "Type",
            default=InteropType.subscribe.value,
            show_choices=True,
            show_default=True,
            type=InteropType,
        )
        type = type.split(".")[0]
        dir: str = (
            "publishers"
            if type == InteropType.publish.value
            else "subscribers"
        )

        env = Environment()
        kwargs = {
            "app": app,
            "author": author,
            "dir": dir,
            "email": email,
            "import_name": import_name,
            "name": name,
            "type": type
        }

        #: App folder (name)
        os.mkdir(app_folder)

        #: README.md
        working_file: str = "README.md"
        with open(os.path.join(app_folder, working_file), "w") as file:
            with open(
                os.path.join(templates_dir, f"{working_file}.jinja2")
            ) as jinja_source:
                template = env.from_string(jinja_source.read())
                file.write(template.render(**kwargs))

        #: .gitignore
        working_file: str = ".gitignore"
        with open(os.path.join(app_folder, working_file), "w") as file:
            with open(
                os.path.join(templates_dir, f"{working_file}.jinja2")
            ) as jinja_source:
                template = env.from_string(jinja_source.read())
                file.write(template.render(**kwargs))

        #: Makefile
        working_file: str = "Makefile"
        with open(os.path.join(app_folder, working_file), "w") as file:
            with open(
                os.path.join(templates_dir, f"{working_file}.jinja2")
            ) as jinja_source:
                template = env.from_string(jinja_source.read())
                file.write(template.render(**kwargs))

        #: pyproject.toml
        working_file: str = "pyproject.toml"
        with open(os.path.join(app_folder, working_file), "w") as file:
            with open(
                os.path.join(templates_dir, f"{working_file}.jinja2")
            ) as jinja_source:
                template = env.from_string(jinja_source.read())
                file.write(template.render(**kwargs))

        #: setup.cfg
        working_file: str = "setup.cfg"
        with open(os.path.join(app_folder, working_file), "w") as file:
            with open(
                os.path.join(templates_dir, f"{working_file}.jinja2")
            ) as jinja_source:
                template = env.from_string(jinja_source.read())
                file.write(template.render(**kwargs))

        #: .sample.env.local
        working_file: str = ".sample.env.local"
        with open(os.path.join(app_folder, working_file), "w") as file:
            with open(
                os.path.join(templates_dir, f"{working_file}.jinja2")
            ) as jinja_source:
                template = env.from_string(jinja_source.read())
                file.write(template.render(**kwargs))

        #: inner application folder (import_name)
        os.mkdir(os.path.join(app_folder, import_name))
        import_folder = os.path.join(app_folder, import_name)

        #: application.__init__.py
        working_file: str = "__init__.py"
        with open(os.path.join(import_folder, working_file), "w") as file:
            with open(
                os.path.join(templates_dir, f"{working_file}.jinja2")
            ) as jinja_source:
                template = env.from_string(jinja_source.read())
                file.write(template.render(**kwargs))

        #: application.__main__.py
        working_file: str = "__main__.py"
        with open(os.path.join(import_folder, working_file), "w") as file:
            with open(
                os.path.join(templates_dir, f"{working_file}.jinja2")
            ) as jinja_source:
                template = env.from_string(jinja_source.read())
                file.write(template.render(**kwargs))
