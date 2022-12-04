import json
import random
from collections import defaultdict
from random import choices
import re
from numpy import random as nprandom

from datagen.attributes import Attribute, RandomSequence, DateRange
from datagen.distributions import Distribution, NormalDistribution, ProbabilityDistribution, PoissonDistribution, \
    BinomialDistribution, ExponentialDistribution, LogisticDistribution, MixedProbabilityDistribution
from datagen.models import Export, Collection, CsvFileOutputFormat, MySqlOutputFormat, Expression, Lookup, Sequence, \
    Range, MongoDBOutputFormat
import pandas as pd

from enum import Enum


class AttributeTypes(Enum):
    EXPRESSION = 'expression'
    DISTRIBUTION = 'distribution'
    DISTRIBUTION_SET = 'distribution_set'
    SEQUENCE = 'sequence'
    RANDOM_SEQUENCE = 'random sequence'
    RANGE = 'range'


class Constants(Enum):
    OUTPUT_FORMAT = 'output_format'
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


class OutputTypes(Enum):
    CSV = "csv"
    MYSQL = "mysql"


class OutputTypeParameters(Enum):
    TYPE = "type"
    PATH = "PATH"
    USERNAME = "username"
    PASSWORD = "password"
    HOST = "host"
    DATABASE = "database"


class Cardinality(Enum):
    ONE_TO_ONE = "One-to-One"
    MANY_TO_ONE = "Many-To-One"
    MANY_TO_MANY = "Many-To-Many"


def read_file(filename: str) -> Export:
    with open(filename, "r") as file:
        input_json_string = ""
        for line in file.readlines():
            input_json_string += line
    return read_string(input_json_string)


def read_string(json_map: str) -> Export:
    json_map = json.loads(json_map)
    if json_map[Constants.OUTPUT_FORMAT.value][OutputTypeParameters.TYPE.value] == OutputTypes.CSV.value:
        output_format = CsvFileOutputFormat(json_map[Constants.OUTPUT_FORMAT.value][OutputTypeParameters.PATH.value])
    else:
        output_format = MySqlOutputFormat(json_map[Constants.OUTPUT_FORMAT.value][OutputTypeParameters.USERNAME.value],
                                          json_map[Constants.OUTPUT_FORMAT.value][OutputTypeParameters.PASSWORD.value],
                                          json_map[Constants.OUTPUT_FORMAT.value][OutputTypeParameters.HOST.value],
                                          json_map[Constants.OUTPUT_FORMAT.value][OutputTypeParameters.DATABASE.value])

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


def generate(export_job: Export, dataset_path: str):
    constraints = defaultdict(lambda: [])
    for collection in export_job.collections:
        df = pd.DataFrame(columns=collection.get_attribute_names())
        for i in range(len(collection.attributes)):
            attribute = collection.attributes[i]
            if isinstance(attribute, Expression):
                df[collection.attributes[i].name] = create_expression(attribute.expression, collection.size)
            elif isinstance(attribute, Lookup):
                df[collection.attributes[i].name] = create_lookup(attribute.lookup_column, dataset_path,
                                                                  collection.size)
            elif isinstance(attribute, Distribution):
                df[collection.attributes[i].name] = create_distribution(attribute, collection.size)
            elif isinstance(attribute, MixedProbabilityDistribution):
                sub_df = create_distribution_set(attribute, collection.size)
                df = df.assign(**sub_df).fillna(df)
            elif isinstance(attribute, Sequence):
                sequence_start = attribute.start
                df[collection.attributes[i].name] = [x for x in range(sequence_start, sequence_start + collection.size)]
            elif isinstance(attribute, RandomSequence):
                sequence_start = attribute.start
                random_list = [x for x in range(sequence_start, sequence_start + collection.size)]
                random.shuffle(random_list)
                df[collection.attributes[i].name] = random_list
            elif isinstance(attribute, Range):
                df[collection.attributes[i].name] = [random.randint(attribute.start, attribute.end) for _ in
                                                     range(collection.size)]
            elif isinstance(attribute, DateRange):
                df[collection.attributes[i].name] = [+ (attribute.end_date - attribute.start_date) * random.random() for
                                                     _ in range(collection.size)]

        # for constraint in collection.constraints:
        #     if constraint.cardinality == Cardinality.ONE_TO_ONE:
        #         pass
        #     elif constraint.cardinality == Cardinality.MANY_TO_ONE:
        #         pass
        #     elif constraint.cardinality == Cardinality.MANY_TO_MANY:
        #         pass

        if isinstance(export_job.output_format, CsvFileOutputFormat):
            df.to_csv(export_job.output_format.path + collection.name + ".csv", index=False)
        elif isinstance(export_job.output_format, MySqlOutputFormat):
            import sqlalchemy
            database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                           format(export_job.output_format.username,
                                                                  export_job.output_format.password,
                                                                  export_job.output_format.host,
                                                                  export_job.output_format.database))
            df.to_sql(con=database_connection, name=collection.name, if_exists='replace')
        elif isinstance(export_job.output_format, MongoDBOutputFormat):
            from pymongo import MongoClient
            client = MongoClient(export_job.output_format.host, export_job.output_format.port)
            db = client[export_job.output_format.database]
            mycol = db[collection.name]
            mycol.insert_many(df.to_dict('records'))
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


def create_lookup(dataset, dataset_path, size):
    table = dataset.split(".")[0]
    column = dataset.split(".")[1]

    df = pd.read_csv(dataset_path + table + ".csv")
    result_col = []
    for _ in range(size):
        record = random.randint(0, df.shape[0] - 1)
        result_col.append(df[column][record])
    return result_col


def create_distribution(attribute: Attribute, size: int):
    if isinstance(attribute, NormalDistribution):
        return nprandom.normal(loc=attribute.mean, scale=attribute.standard_deviation, size=size)
    elif isinstance(attribute, LogisticDistribution):
        return nprandom.logistic(loc=attribute.mean, scale=attribute.standard_deviation, size=size)
    elif isinstance(attribute, ExponentialDistribution):
        return nprandom.exponential(scale=attribute.scale, size=size)
    elif isinstance(attribute, BinomialDistribution):
        return nprandom.binomial(n=attribute.total_trials, p=attribute.probability, size=size)
    elif isinstance(attribute, PoissonDistribution):
        return nprandom.poisson(lam=attribute.event_rate, size=size)
    elif isinstance(attribute, ProbabilityDistribution):
        create_probability_distribution(attribute.path, attribute.events_file, attribute.weights_file, size)


def create_probability_distribution(distribution_path, population_file, weights_file, size) -> list:
    population_filename = population_file.split(".")[0]
    weights_filename = weights_file.split(".")[0]
    population_col = population_file.split(".")[1]
    weights_col = weights_file.split(".")[1]

    population = pd.read_csv(distribution_path + population_filename + ".csv")[population_col].values.tolist()
    weights = pd.read_csv(distribution_path + weights_filename + ".csv")[weights_col].values.tolist()

    samples = choices(population, weights, k=size)
    return samples


def create_distribution_set(attribute: MixedProbabilityDistribution, size: int):
    res = pd.DataFrame()
    weights_filename = attribute.weights_file.split(".")[0]
    weights_col = attribute.weights_file.split(".")[1]
    weights = pd.read_csv(attribute.path + weights_filename + ".csv")[weights_col].values.tolist()
    samples = choices(list(range(0, len(weights))), weights, k=size)
    for i in range(len(attribute.events_files)):
        population_filename = attribute.events_files[i].split(".")[0]
        population_col = attribute.events_files[i].split(".")[1]
        population = pd.read_csv(attribute.path + population_filename + ".csv")[population_col].values.tolist()
        sample = []
        for value in samples:
            sample.append(population[value])
        res[attribute.names[i]] = sample
    return res
