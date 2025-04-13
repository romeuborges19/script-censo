import os
from bs4 import BeautifulSoup
from urllib.request import urlretrieve
import click
import requests
from typing import List

from settings import ANO_MAXIMO, URL_MICRODADOS


class CrawlingService:
    """Responsável por baixar os arquivos comprimidos dos microdados."""

    def __init__(self, ano_limite: int, destino: str):
        self.anos = list(range(ANO_MAXIMO, ano_limite - 1, -1))
        self.destino = destino

    def _baixar_arquivos(self, urls: List[str]):
        current_files = os.listdir(self.destino)
        arquivos_baixados = []

        try:
            for url in urls:
                filename = url.split("/")[-1]
                path_file = f"{self.destino}/{filename}"

                if filename in current_files:
                    arquivos_baixados.append(path_file)
                    continue

                path, _ = urlretrieve(url, path_file)
                arquivos_baixados.append(path)

                click.echo(f"Donwload concluído. Arquivo salvo em: {path}")
        except Exception as e:
            click.echo(f"Não foi possível baixar arquivo. Erro: {e}")
            return False

        return arquivos_baixados

    def run(self) -> bool:
        try:
            response = requests.get(URL_MICRODADOS)
        except Exception as e:
            click.echo(f"Não foi possível acessar a URL {URL_MICRODADOS}. Erro: {e}")
            return False

        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.select("a[href]")

        urls_filtrados = []
        urls_extraidos = 0

        for link in links:
            classe = link.get("class")
            if classe and classe[0] == "external-link":
                href = link.get("href")
                if str(self.anos[urls_extraidos]) in href:
                    urls_filtrados.append(href)
                    urls_extraidos += 1

                if urls_extraidos >= len(self.anos):
                    break

        return self._baixar_arquivos(urls_filtrados)
