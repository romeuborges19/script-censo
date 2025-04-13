from datetime import date, datetime
from io import StringIO
import os
import click
import pandas as pd

from sqlalchemy.engine import create_engine
from sqlalchemy.orm import Session

from multiprocessing import Process

MAX_WORKERS = 5


class LoadingService:
    """Carrega os dados csv no banco especificado."""

    def __init__(self, dir: str, uri: str) -> None:
        self.engine = create_engine(uri)
        self.db = Session(self.engine)
        self.dir = dir

    def _process(self, batch: pd.DataFrame, conn):
        start = datetime.now()

        output = StringIO()
        batch.to_csv(output, sep="\t", header=False)
        end = datetime.now()
        click.echo(f"Conversão para CSV:{end - start}")

        output.seek(0)
        contents = output.getvalue()

        start = datetime.now()

        conn = self.engine.raw_connection()
        cur = conn.cursor()
        cur.copy_from(output, "microdados", null="")
        conn.commit()
        cur.close()
        conn.close()

        end = datetime.now()

        click.echo(f"Inserção: {end - start}")

    def run(self):
        conn = self.engine.raw_connection()
        cur = conn.cursor()
        cur.execute("truncate table microdados")
        conn.commit()
        cur.close()

        arquivos = [
            f"{self.dir}/{a}" for a in os.listdir(self.dir) if a.split(".")[-1] == "csv"
        ]

        df_todos = [pd.read_csv(a, delimiter=";", encoding="latin1") for a in arquivos]

        df = pd.concat(df_todos)

        offset = 0
        num_chunks = 10
        chunksize = int(len(df) / num_chunks) + 1

        tamanho = len(df)
        lower_limit = offset
        upper_limit = offset

        start = datetime.now()
        processes = []
        while upper_limit <= tamanho:
            lower_limit = offset
            upper_limit = lower_limit + chunksize
            if upper_limit > len(df):
                upper_limit = len(df)

            df_batch = df[lower_limit:upper_limit]

            p = Process(target=self._process, args=(df_batch, self.engine))
            p.start()

            processes.append(p)
            if len(processes) == MAX_WORKERS:
                for p in processes:
                    p.join()
                processes = []

            del df_batch
            if upper_limit == tamanho:
                break

            offset += chunksize
        end = datetime.now()
        click.echo(f"total: {end - start}")
