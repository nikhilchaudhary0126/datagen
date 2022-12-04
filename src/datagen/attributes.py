from datetime import datetime


class Attribute:
    def __init__(self, name: str):
        self.name = name


class Expression(Attribute):
    def __init__(self, name: str, expression: str):
        super().__init__(name)
        self.expression = expression


class Lookup(Attribute):
    def __init__(self, name: str, lookup_column: str):
        super().__init__(name)
        self.lookup_column = lookup_column


class Sequence(Attribute):
    def __init__(self, name: str, start: int):
        super().__init__(name)
        self.start = start


class RandomSequence(Attribute):
    def __init__(self, name: str, start: int):
        super().__init__(name)
        self.start = start


class Range(Attribute):
    def __init__(self, name: str, start: int, end: int):
        super().__init__(name)
        self.start = start
        self.end = end


class DateRange(Attribute):
    def __init__(self, name: str, start_date: datetime, end_date: datetime):
        super().__init__(name)
        self.start_date = start_date
        self.end_date = end_date
