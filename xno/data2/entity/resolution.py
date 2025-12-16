class Resolution:
    unit: str
    value: int

    def __init__(self, unit: str, value: int):
        self.unit = unit
        self.value = value

    def validate(self):
        if not isinstance(self.unit, str):
            raise TypeError("unit must be a str")
        if not isinstance(self.value, int):
            raise TypeError("value must be an int")

    def __str__(self) -> str:
        return self.to_string()

    def __eq__(self, value: "str | Resolution") -> bool:
        if isinstance(value, Resolution):
            return self.unit == value.unit and self.value == value.value
        if isinstance(value, str):
            return self.to_string() == value
        return False

    def __hash__(self):
        return hash((self.unit, self.value))

    def to_string(self) -> str:
        return f"{self.value}{self.unit}"

    def to_external(self) -> str:
        if self.unit == "D" and self.value == 1:
            return "DAY"
        if self.unit == "H" and self.value == 1:
            return "HOUR1"
        if self.unit == "M" and self.value == 1:
            return "MIN"
        raise ValueError(f"Unsupported resolution: {self.to_string()}")
    
    def to_external_postgre(self) -> str:
        if self.unit == "D":
            if self.value == 1:
                return "1 day"
            return f"{self.value} days"
        if self.unit == "H":
            if self.value == 1:
                return "1 hour"
            return f"{self.value} hours"
        if self.unit == "M":
            if self.value == 1:
                return "1 minute"
            return f"{self.value} minutes"
        raise ValueError(f"Unsupported resolution for postgre: {self.to_string()}")

    @classmethod
    def from_external(cls, raw: str) -> "Resolution":
        print("raw=", raw)
        raw = raw.lower()
        if raw == "day":
            return cls(unit="D", value=1)
        if raw == "hour1":
            return cls(unit="H", value=1)
        if raw == "min":
            return cls(unit="M", value=1)
        raise ValueError(f"Unsupported resolution: {raw}")

    @classmethod
    def from_string(cls, resolution: str) -> "Resolution":
        resolution = resolution.lower()
        if resolution == "1d":
            return cls(unit="D", value=1)
        if resolution == "1h":
            return cls(unit="H", value=1)
        if resolution == "1m":
            return cls(unit="M", value=1)
        raise ValueError(f"Unsupported resolution string: {resolution}")
