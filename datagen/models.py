import json
from datetime import datetime

from datagen.attributes import Expression, Lookup, Range, Sequence, DateRange
from datagen.distributions import Distribution, MixedProbabilityDistribution


class Collection:
    def __init__(self, name: str, size: int):
        self.name = name
        self.size = size
        self.attributes = []

    def to_json(self):
        return json.dumps(self.__dict__, default=lambda o: o.__dict__, indent=4)

    def add_expression(self, name: str, expression: str):
        self.attributes.append(Expression(name, expression))

    def add_lookup(self, name: str, lookup_column: str):
        self.attributes.append(Lookup(name, lookup_column))

    def add_distribution(self, distribution: Distribution | MixedProbabilityDistribution):
        self.attributes.append(distribution)

    def add_range(self, name: str, start: int, end: int):
        self.attributes.append(Range(name, start, end))

    def add_sequence(self, name: str, start: int):
        self.attributes.append(Sequence(name, start))

    def add_date(self, name: str, start_date: str, end_date: str, date_format: str):
        self.attributes.append(
            DateRange(name, datetime.strptime(start_date, date_format), datetime.strptime(end_date, date_format)))

    def get_attribute_names(self):
        names = []
        for attribute in self.attributes:
            if isinstance(attribute, MixedProbabilityDistribution):
                names.extend(attribute.names)
            else:
                names.append(attribute.name)
        return names


class FlatFileOutputFormat:
    def __init__(self, path: str):
        self.path = path

    def to_json(self):
        return json.dumps(self.__dict__, default=lambda o: o.__dict__, indent=4)


class MySqlOutputFormat:
    def __init__(self, username: str, password: str, host: str, database: str):
        self.username = username
        self.password = password
        self.host = host
        self.database = database

    def to_json(self):
        return json.dumps(self.__dict__, default=lambda o: o.__dict__, indent=4)


class Export:
    def __init__(self, output_format: MySqlOutputFormat | FlatFileOutputFormat, collections: list[Collection]):
        self.output_format = output_format
        self.collections = collections

    def add_collection(self, collection: Collection):
        self.collections.append(collection)

    def to_json(self):
        return json.dumps(self.__dict__, default=lambda o: o.__dict__, indent=4)
