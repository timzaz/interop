import typer

from .init import InitCli
from .publisher import PublisherCli
from .subscriber import SubscriberCli

#: main cli app
cli: typer.Typer = typer.Typer()

cli.add_typer(InitCli())
cli.add_typer(PublisherCli())
cli.add_typer(SubscriberCli())


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
