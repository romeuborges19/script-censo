import os
from bs4 import BeautifulSoup
from urllib.request import urlretrieve
import click
import requests
from typing import List

from settings import ANO_MAXIMO, URL_MICRODADOS


class CrawlingService:
    def __init__(self, ano_limite: int, destino: str):
        self.anos = list(range(ANO_MAXIMO, ano_limite - 1, -1))
        self.destino = destino

    def _baixar_arquivos(self, urls: List[str]):
        current_files = os.listdir(self.destino)

        try:
            for url in urls:
                filename = url.split("/")[-1]
                if filename in current_files:
                    continue

                filename = f"{self.destino}/{filename}"
                path, _ = urlretrieve(url, filename)
                click.echo(f"Donwload concluído. Arquivo salvo em: {path}")
        except Exception as e:
            click.echo(f"Não foi possível baixar arquivo. Erro: {e}")
            return False

        return True

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
