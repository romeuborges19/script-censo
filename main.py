import click
from service.extract import ExctractionService
from service.load import LoadingService
from settings import ROOT, DatabaseConfig
from service.crawl import CrawlingService


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "-a",
    "--ano-limite",
    default=2021,
    help="Os downloads serão feitos de 2024 até o ano especificado.",
)
@click.option("-o", "--output", default=ROOT, help="Diretório de destino do download.")
def download(ano_limite: int, destino: str):
    if not destino.startswith("/"):
        destino = f"{ROOT}/{destino}"

    crawl = CrawlingService(ano_limite, destino)
    crawl.run()


@cli.command()
@click.option(
    "-d", "--dir", default=ROOT, help="Diretório onde os arquivos .zip estão."
)
@click.option("-o", "--output", default="", help="Diretório de destino do download.")
@click.option(
    "-a",
    "--ano-limite",
    default=2021,
    help="Os downloads serão feitos de 2024 até o ano especificado.",
)
def extract(dir: str, output: str, ano_limite: int):
    if not dir.startswith("/"):
        dir = f"{ROOT}/{dir}"

    crawl = CrawlingService(ano_limite, dir)
    crawl.run()

    if output and not output.startswith("/"):
        output = f"{ROOT}/{output}"

    if not output:
        output = dir

    e = ExctractionService(dir, output)
    e.run()


@cli.command()
@click.option("-d", "--dir", default=f"{ROOT}/data")
@click.option("-h", "--host", default="localhost")
@click.option("-p", "--port", default=5432)
@click.option("-u", "--username", default="postgres")
@click.option("-p", "--password", default="postgres")
@click.option("--dbname", required=True)
@click.option("-t", "--table", required=True)
def load(
    dir: str,
    host: str,
    port: int,
    username: str,
    password: str,
    dbname: str,
    table: str,
):
    database_uri = DatabaseConfig(
        dbname=dbname, username=username, password=password, host=host, port=port
    ).uri
    click.echo(dir)
    load = LoadingService(dir, database_uri)
    load.run(table)


if __name__ == "__main__":
    cli()
