import os
import zipfile
import shutil


class ExctractionService:
    """Responsável por extrair o .csv desejado do arquivo baixado."""

    def __init__(self, origem: str, output: str) -> None:
        self.origem = origem
        self.output = output

    def run(self):
        arquivos = os.listdir(self.origem)
        arquivos_extrair = []
        nomes = []

        for arquivo in arquivos:
            nome, extensao = arquivo.split(".")
            if extensao == "zip":
                arquivos_extrair.append(f"{self.origem}/{arquivo}")
                nomes.append(nome)

        copias = []
        for id, arquivo in enumerate(arquivos_extrair):
            with zipfile.ZipFile(arquivo, "r") as zip_ref:
                output = f"{self.output}/{nomes[id]}"
                zip_ref.extractall(output)

                pasta_extracao = f"{output}/{os.listdir(output)[0]}/dados"
                for csv in os.listdir(pasta_extracao):
                    if csv.startswith("microdados"):
                        copia = shutil.copy(
                            f"{pasta_extracao}/{csv}", f"{self.output}/{csv}"
                        )
                        copias.append(copia)
                        break

                shutil.rmtree(output)

        return copias
