class SelfRaisingError(Exception):
    """
    抛出对象本身的错误，为的是在Assert语句中使用直接抛出异常
    """

    def __init__(self, msg: str):
        super().__init__(msg)
        raise self


class ArgumentError(SelfRaisingError):
    pass


class CodingError(SelfRaisingError):
    pass


class OperationError(SelfRaisingError):
    pass


class DataError(SelfRaisingError):
    pass


class RunTimeError(SelfRaisingError):
    pass
