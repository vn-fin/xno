from typing import Self


class WiGroupBaseAPI:
    @classmethod
    def singleton(cls, **kwargs) -> Self:
        if not hasattr(cls, "_instance"):
            cls._instance = cls(**kwargs)
        return cls._instance

    def __init__(self, db_name: str = "xno_data"):
        self._database_name = db_name
