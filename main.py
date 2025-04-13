import click
from settings import ROOT
from service.crawl import CrawlingService


@click.group()
def cli():
    pass


@cli.command()
@click.option("-a", "--ano-limite", default=2021)
@click.option("-d", "--destino", default=ROOT)
def download(ano_limite: int, destino: str):
    if not destino.startswith("/"):
        destino = f"{ROOT}/{destino}"

    crawl = CrawlingService(ano_limite, destino)
    crawl.run()


if __name__ == "__main__":
    cli()
