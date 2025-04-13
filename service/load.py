class LoadingService:
    """Carrega os dados csv no banco especificado."""

    def __init__(self, uri: str) -> None:
        self.uri = uri
