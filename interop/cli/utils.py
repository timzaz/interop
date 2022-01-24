import dotenv
import importlib
import inspect
import os
import re
import typer
import types
import typing


def get_import_name():
    #: Set the environment variables
    env_dir = None
    path = dotenv.find_dotenv(".env.local", usecwd=True)

    if path and env_dir is None:
        env_dir = os.path.dirname(path)
        dotenv.load_dotenv(path)

    #: The remainder of the code should be run from the .env.local directory
    if env_dir and os.getcwd() != env_dir:
        os.chdir(env_dir)

    name: str | None = os.getenv("IMPORT_NAME")

    if not name:
        raise RuntimeError(
            "Make sure you specify the IMPORT_NAME in .env.local"
        )


def get_templates_directory() -> str:
    """Gets the template directory"""

    typer.echo(
        typer.style(
            "\nDeducing templates directory",
            fg=typer.colors.BLUE,
            bold=True
        )
    )

    templates_module_name: str = typer.prompt(
        "Enter the containing directory or module for the templates directory",
        default="interop.cli",
        type=str
    )
    templates_module: typing.Optional[
        types.ModuleType
    ] = None

    try:
        templates_module: typing.Optional[
            types.ModuleType
        ] = importlib.import_module(templates_module_name)
    except ImportError:
        pass

    templates_dir: str = (
        os.path.join(
            os.path.dirname(inspect.getfile(templates_module)),
            "templates"
        )
        if templates_module is not None
        else templates_module_name
    )

    if not os.path.exists(templates_dir):
        typer.echo(
            typer.style(
                "\nTemplates directory not found",
                fg=typer.colors.RED,
                bold=True
            )
        )
        raise typer.Abort()

    return templates_dir


def snake_case(name, lower=True):
    """Returns the snake case for the string passed.

    TODO: Possible false positive
    """

    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    if lower:
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
    else:
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)


def space_case(name, lower=True):
    """Returns the space case for the string passed."""

    if lower:
        return re.sub(r"(_)([A-Za-z0-9])", r" \2", name).lower()
    else:
        return re.sub(r"(_)([A-Za-z0-9])", r" \2", name)
