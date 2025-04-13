import click
from service.extract import ExctractionService
from settings import ROOT, DatabaseConfig
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


@cli.command()
@click.option("-d", "--dir", default=ROOT)
def extract(dir: str):
    if not dir.startswith("/"):
        dir = f"{ROOT}/{dir}"

    crawl = ExctractionService(dir)
    crawl.run()


@cli.command()
@click.option("-d", "--dir", default=f"{ROOT}/data")
@click.option("-h", "--host", default="localhost")
@click.option("-p", "--port", default=5432)
@click.option("-u", "--username", default="postgres")
@click.option("-p", "--password", default="postgres")
@click.option("-n", "--namedb", default="microdados")
def load(dir: str, host: str, port: int, username: str, password: str, dbname: str):
    database_uri = DatabaseConfig(
        dbname=dbname, username=username, password=password, host=host, port=port
    ).uri
    click.echo(database_uri)


if __name__ == "__main__":
    cli()
