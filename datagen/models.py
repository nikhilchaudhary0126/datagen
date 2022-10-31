import json

from typing import List


class Attribute:
    def __init__(self, name: str, attribute_type: str, value: str = None, population: str = None, weights: str = None):
        self.name = name
        self.attribute_type = attribute_type
        self.value = value
        self.population = population
        self.weights = weights

    def to_json(self):
        return json.dumps(self.__dict__, default=lambda o: o.__dict__, indent=4)


class Collection:
    def __init__(self, name: str, size: int, attributes: List[Attribute]):
        self.name = name
        self.size = size
        self.attributes = attributes

    def to_json(self):
        return json.dumps(self.__dict__, default=lambda o: o.__dict__, indent=4)


class OutputFormat:
    def __init__(self, destination: str):
        self.destination = destination

    def to_json(self):
        return json.dumps(self.__dict__, default=lambda o: o.__dict__, indent=4)


class Export:
    def __init__(self, output_format: OutputFormat, collections: List[Collection]):
        self.output_format = output_format
        self.collections = collections

    def to_json(self):
        return json.dumps(self.__dict__, default=lambda o: o.__dict__, indent=4)
