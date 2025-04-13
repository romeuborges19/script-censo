import pathlib
from dataclasses import dataclass

URL_MICRODADOS = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar"

ANO_MAXIMO = 2024

ROOT = str(pathlib.Path(__file__).parent.resolve())


@dataclass
class DatabaseConfig:
    dbname: str
    username: str = "postgres"
    password: str = "postgres"
    host: str = "localhost"
    port: int = 5432

    @property
    def uri(self):
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.dbname}"
