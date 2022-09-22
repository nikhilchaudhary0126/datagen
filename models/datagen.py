import json
import random
import re

from models.export import Export, OutputFormat, Attribute, Collection
import pandas as pd


def read_file(filename: str) -> Export:
    with open(filename, "r") as file:
        input_json_string = ""
        for line in file.readlines():
            input_json_string += line
    return read_string(input_json_string)


def read_string(json_map: str) -> Export:
    json_map = json.loads(json_map)
    output_format = OutputFormat(json_map['output_format']["destination"], json_map['output_format']["filename"])
    collections = []
    for json_collection in json_map["collections"]:
        collection_name = json_collection["name"]
        collection_size = json_collection["size"]
        collection_attributes = []
        for json_collection_attribute in json_collection["attributes"]:
            collection_attributes.append(
                Attribute(json_collection_attribute["name"], json_collection_attribute["attribute_type"],
                          json_collection_attribute["value"]))
        collections.append(Collection(collection_name, collection_size, collection_attributes))

    return Export(output_format, collections)


def generate(export_job: Export):
    for collection in export_job.collections:

        df = pd.DataFrame(columns=[attribute.name for attribute in collection.attributes])
        for i in range(len(collection.attributes)):
            attribute = collection.attributes[i]
            if attribute.attribute_type == "expression":  # constant
                for j in range(collection.size):
                    df.at[j, collection.attributes[i].name] = create_expression(attribute.value)
            elif attribute.attribute_type == "lookup":
                for j in range(collection.size):
                    df.at[j, collection.attributes[i].name] = create_lookup(attribute.value)
        if export_job.output_format.destination == "csv":
            df.to_csv("exports/" + export_job.output_format.filename, index=False)

    return "Finished Export"


def create_expression(input_str):
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
    # print(pattern, occurrence)

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
    return res


def create_lookup(dataset):
    table = dataset.split(".")[0]
    column = dataset.split(".")[1]

    df = pd.read_csv("datasets/" + table + ".csv")
    record = random.randint(0, df.shape[0] - 1)
    return df[column][record]
