from datetime import date, datetime
from io import StringIO
import os
import click
import pandas as pd
from psycopg2.errors import UndefinedTable

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

    def _process(self, tablename: str, batch: pd.DataFrame):
        output = StringIO()
        batch.to_csv(output, sep="\t", header=False)

        output.seek(0)
        contents = output.getvalue()

        conn = self.engine.raw_connection()
        cur = conn.cursor()

        cur.copy_from(output, tablename, null="")

        conn.commit()
        cur.close()
        conn.close()

    def _batch_insert(self, df: pd.DataFrame, tablename: str):
        tamanho = len(df)
        offset = 0
        num_chunks = 10
        chunksize = int(tamanho / num_chunks) + 1

        lower_limit = offset
        upper_limit = offset

        start = datetime.now()
        processes = []
        while upper_limit <= tamanho:
            lower_limit = offset
            upper_limit = lower_limit + chunksize
            if upper_limit > tamanho:
                upper_limit = tamanho

            df_batch = df[lower_limit:upper_limit]

            p = Process(target=self._process, args=(tablename, df_batch))
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
        return end - start

    def _truncate_table(self, tablename: str):
        conn = self.engine.raw_connection()
        cur = conn.cursor()
        cur.execute(f"truncate table {tablename}")
        conn.commit()
        cur.close()

    def run(self, tablename: str):
        arquivos = [
            f"{self.dir}/{a}" for a in os.listdir(self.dir) if a.split(".")[-1] == "csv"
        ]
        df_todos = [pd.read_csv(a, delimiter=";", encoding="latin1") for a in arquivos]
        df = pd.concat(df_todos)

        try:
            self._truncate_table(tablename)
        except UndefinedTable:
            df[:5].to_sql(tablename, con=self.engine, index=True)
            self._truncate_table(tablename)

        tempo = self._batch_insert(df, tablename)
        click.echo(f"Sucesso. Concluído em {tempo}")
