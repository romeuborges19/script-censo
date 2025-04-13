import os
import zipfile
import shutil


class ExctractionService:
    """Responsável por extrair o .csv desejado do arquivo baixado."""

    def __init__(self, destino: str) -> None:
        self.destino = destino

    def run(self):
        arquivos = os.listdir(self.destino)
        arquivos_extrair = []
        nomes = []

        for arquivo in arquivos:
            nome, extensao = arquivo.split(".")
            if extensao == "zip":
                arquivos_extrair.append(f"{self.destino}/{arquivo}")
                nomes.append(nome)

        copias = []
        for id, arquivo in enumerate(arquivos_extrair):
            with zipfile.ZipFile(arquivo, "r") as zip_ref:
                destino = f"{self.destino}/{nomes[id]}"
                zip_ref.extractall(destino)

                pasta_extracao = f"{destino}/{os.listdir(destino)[0]}/dados"
                for csv in os.listdir(pasta_extracao):
                    if csv.startswith("microdados"):
                        copia = shutil.copy(
                            f"{pasta_extracao}/{csv}", f"{self.destino}/{csv}"
                        )
                        copias.append(copia)
                        break

                shutil.rmtree(destino)

        return copias
