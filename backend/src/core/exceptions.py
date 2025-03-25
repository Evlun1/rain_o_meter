class AlreadyInitialized(Exception):
    def __init__(self) -> None:
        self.message = "Key value DB is already initialized."
        super().__init__(self.message)
