from typing import Final


class Period:
    value: str

    ANNUALLY: "Final[Period]" = "ANNUALLY"
    QUARTERLY: "Final[Period]" = "QUARTERLY"

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

Period.ANNUALLY = Period("ANNUALLY")
Period.QUARTERLY = Period("QUARTERLY")