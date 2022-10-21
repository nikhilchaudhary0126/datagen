import json
import random
from random import choices
import re
from collections import defaultdict

from models.export import Export, OutputFormat, Attribute, Collection
import pandas as pd

from enum import Enum


class Constants(Enum):
    OUTPUT_FORMAT = 'output_format'
    DESTINATION = 'destination'
    COLLECTION = 'collections'
    COLLECTION_NAME = 'name'
    COLLECTION_SIZE = 'size'
    ATTRIBUTES = 'attributes'
    ATTRIBUTE_VALUE = 'value'
    ATTRIBUTE_NAME = 'name'
    ATTRIBUTE_TYPE = 'attribute_type'
    POPULATION = 'population'
    EXPRESSION = 'expression'
    LOOKUP = 'lookup'
    DISTRIBUTION = 'distribution'
    WEIGHTS = 'weights'
    DISTRIBUTION_SET = 'distribution_set'
    SEQUENCE = 'sequence'
    RANDOM_SEQUENCE = 'random sequence'
    RANGE = 'range'


def read_file(filename: str) -> Export:
    with open(filename, "r") as file:
        input_json_string = ""
        for line in file.readlines():
            input_json_string += line
    return read_string(input_json_string)


def read_string(json_map: str) -> Export:
    json_map = json.loads(json_map)
    output_format = OutputFormat(json_map[Constants.OUTPUT_FORMAT.value][Constants.DESTINATION.value])
    collections = []
    for json_collection in json_map[Constants.COLLECTION.value]:
        collection_name = json_collection[Constants.COLLECTION_NAME.value]
        collection_size = json_collection[Constants.COLLECTION_SIZE.value]
        collection_attributes = []
        for json_collection_attribute in json_collection[Constants.ATTRIBUTES.value]:
            if Constants.ATTRIBUTE_VALUE.value in json_collection_attribute:
                collection_attributes.append(
                    Attribute(json_collection_attribute[Constants.ATTRIBUTE_NAME.value],
                              json_collection_attribute[Constants.ATTRIBUTE_TYPE.value],
                              json_collection_attribute[Constants.ATTRIBUTE_VALUE.value]))
            else:
                collection_attributes.append(
                    Attribute(json_collection_attribute[Constants.ATTRIBUTE_NAME.value],
                              json_collection_attribute[Constants.ATTRIBUTE_TYPE.value], None,
                              json_collection_attribute[Constants.POPULATION.value],
                              json_collection_attribute[Constants.WEIGHTS.value]))
        collections.append(Collection(collection_name, collection_size, collection_attributes))
    return Export(output_format, collections)


def generate(export_job: Export):
    for collection in export_job.collections:

        df = pd.DataFrame(columns=[attribute.name for attribute in collection.attributes])
        distribution = defaultdict(lambda: [])
        for i in range(len(collection.attributes)):
            attribute = collection.attributes[i]
            match attribute.attribute_type:
                case Constants.EXPRESSION.value:
                    df[collection.attributes[i].name] = create_expression(attribute.value, collection.size)
                case Constants.LOOKUP.value:
                    df[collection.attributes[i].name] = create_lookup(attribute.value, collection.size)
                case Constants.DISTRIBUTION.value:
                    df[collection.attributes[i].name] = create_distribution(attribute.population, attribute.weights,
                                                                            collection.size)
                case Constants.DISTRIBUTION_SET.value:
                    distribution[attribute.weights].append([attribute.name, attribute.population])
                case Constants.SEQUENCE.value:
                    sequence_start = int(attribute.value)
                    df[collection.attributes[i].name] = [x for x in
                                                         range(sequence_start, sequence_start + collection.size)]
                case Constants.RANDOM_SEQUENCE.value:
                    sequence_start = int(attribute.value)
                    random_list = [x for x in range(sequence_start, sequence_start + collection.size)]
                    random.shuffle(random_list)
                    df[collection.attributes[i].name] = random_list
                case Constants.RANGE.value:
                    range_start, range_end = int(attribute.value.split("-")[0]), int(attribute.value.split("-")[1])
                    df[collection.attributes[i].name] = [random.randint(range_start, range_end) for _ in
                                                         range(collection.size)]

        if len(distribution) != 0:
            sub_df = create_distribution_set(distribution, collection.size)
            df = df.assign(**sub_df).fillna(df)
        if export_job.output_format.destination == "csv":
            df.to_csv("exports/" + collection.name, index=False)

    return "Finished Export"


def read_expression(input_str):
    i = 0
    pattern = []
    occurrence = []
    escape_characters = ["-", "[", "]", "{", "}"]
    while i < len(input_str):
        choices = []
        if input_str[i] == "[":
            i += 1
            while input_str[i] != "]":
                if len(choices) != 0 and choices[-1] == "-":
                    choices[-2] = choices[-2] + "-" + input_str[i]
                    choices.pop()
                else:
                    choices.append(input_str[i])
                i += 1
            pop_indexes = []
            for index in range(len(choices) - 1):
                if choices[index] == "\\" and choices[index + 1] in escape_characters:
                    choices[index] = choices[index + 1]
                    pop_indexes.append(index + 1)
            for index in pop_indexes:
                choices.pop(index)

            pattern.append(choices)
        if input_str[i] == "{":
            st = i + 1
            while input_str[i] != "}":
                i += 1
            occurrence.append(int(input_str[st:i]))
        i += 1
    return pattern, occurrence


def create_expression(input_str, size):
    result_col = []
    pattern, occurrence = read_expression(input_str)
    for _ in range(size):
        res = ""
        for i in range(len(pattern)):
            for j in range(occurrence[i]):
                if len(pattern) == 0 or len(occurrence) == 0:
                    return ""
                elif len(pattern[i]) == 1:
                    if len(re.findall("[0-9A-Za-z]-[0-9A-Za-z]", pattern[i][0])) < 1:
                        res += pattern[i][0]
                    else:
                        total_range = pattern[i][0].split("-")
                        range_start = total_range[0]
                        range_end = total_range[1]
                        if range_start.isdigit():
                            res += str(random.randint(int(range_start), int(range_end)))
                        else:
                            st, end = ord(range_start), ord(range_end)
                            res += chr(random.randint(st, end))
                else:
                    index = random.randint(0, len(pattern[i]) - 1)
                    if len(re.findall("[0-9A-Za-z]-[0-9A-Za-z]", pattern[i][index])) < 1:
                        res += pattern[i][index]
                    else:
                        total_range = pattern[i][index].split("-")
                        range_start = total_range[0]
                        range_end = total_range[1]
                        if range_start.isdigit():
                            res += str(random.randint(int(range_start), int(range_end)))
                        else:
                            st, end = ord(range_start), ord(range_end)
                            res += chr(random.randint(st, end))
        result_col.append(res)
    return result_col


def create_lookup(dataset, size):
    table = dataset.split(".")[0]
    column = dataset.split(".")[1]

    df = pd.read_csv("datasets/" + table + ".csv")
    result_col = []
    for _ in range(size):
        record = random.randint(0, df.shape[0] - 1)
        result_col.append(df[column][record])
    return result_col


def create_distribution(population_file, weights_file, size) -> list:
    population_filename = population_file.split(".")[0]
    weights_filename = weights_file.split(".")[0]
    population_col = population_file.split(".")[1]
    weights_col = weights_file.split(".")[1]

    population = pd.read_csv("distribution/" + population_filename + ".csv")[population_col].values.tolist()
    weights = pd.read_csv("distribution/" + weights_filename + ".csv")[weights_col].values.tolist()

    samples = choices(population, weights, k=size)
    return samples


def create_distribution_set(distribution, size):
    res = pd.DataFrame()
    for weight, distribution_list in distribution.items():
        weights_filename = weight.split(".")[0]
        weights_col = weight.split(".")[1]
        weights = pd.read_csv("distribution/" + weights_filename + ".csv")[weights_col].values.tolist()
        samples = choices(list(range(0, len(weights))), weights, k=size)
        for distribution in distribution_list:
            population_filename = distribution[1].split(".")[0]
            population_col = distribution[1].split(".")[1]
            population = pd.read_csv("distribution/" + population_filename + ".csv")[population_col].values.tolist()
            sample = []
            for value in samples:
                sample.append(population[value])
            res[distribution[0]] = sample
    return res
