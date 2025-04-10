class AlreadyInitialized(Exception):
    def __init__(self) -> None:
        self.message = "Key value DB is already initialized."
        super().__init__(self.message)


class AlreadyAddedData(Exception):
    def __init__(self) -> None:
        self.message = "Data is already in backend."
        super().__init__(self.message)
