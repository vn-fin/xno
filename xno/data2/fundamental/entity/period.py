from typing import Final


class Period:
    value: str

    YEAR: "Final[Period]" = "year"
    QUARTER: "Final[Period]" = "quarter"

    def __init__(self, value: str):
        self.value = value

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"Period({self.value})"

    def __eq__(self, other) -> bool:
        if isinstance(other, Period):
            return self.value == other.value
        if isinstance(other, str):
            return self.value == other
        return False

    def __hash__(self) -> int:
        return hash(self.value)

    def to_wigroup(self) -> str:
        if self.value == "year":
            return "nam"
        if self.value == "quarter":
            return "quy"
        raise ValueError(f"Unknown period value: {self.value}")

Period.YEAR = Period("YEAR")
Period.QUARTER = Period("QUARTER")
