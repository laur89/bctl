class FatalErr(Exception):
    def __init__(self, message) -> None:
        super().__init__(message)


class ExitableErr(FatalErr):
    def __init__(self, message, exit_code: int=1) -> None:
        super().__init__(message)
        self.exit_code: int = exit_code
