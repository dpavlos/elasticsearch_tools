#!/usr/bin/env python2

import requests
import sys
import csv

source_host = 'http://localhost:9200'
mappings = '/_mapping'
index = 'logstash-2019.02.10'
version = '6'
list_of_field_types = []


def get_fields(res):
    fields = res[index]["mappings"]["_doc"]["properties"]

    for field in fields:
        if "properties" in fields[field]:
            for one_level_field in fields[field]["properties"]:
                if "properties" in fields[field]["properties"][one_level_field]:
                    for second_level_field in fields[field]["properties"][one_level_field]["properties"]:
                        inner_field_type = \
                            fields[field]["properties"][one_level_field]["properties"][second_level_field]["type"]
                        if (one_level_field, second_level_field, inner_field_type) not in list_of_field_types:
                            list_of_field_types.append((one_level_field, second_level_field, inner_field_type))
                else:
                    inner_field_type = fields[field]["properties"][one_level_field]["type"]
                    if (one_level_field, inner_field_type) not in list_of_field_types:
                        list_of_field_types.append((one_level_field, inner_field_type))
        else:
            inner_field_type = fields[field]["type"]
            if (field, inner_field_type) not in list_of_field_types:
                list_of_field_types.append((field, inner_field_type))
    return list_of_field_types


def get_legacy(res):
    fields = res[index]["mappings"]

    for doc_type in fields:
        if "properties" in fields["mappings"][doc_type]:
            for one_level_field in fields[doc_type]["properties"]:
                if "properties" in fields[doc_type]["properties"][one_level_field]:
                    for second_level_field in fields[doc_type]["properties"][one_level_field]["properties"]:
                        inner_field_type = \
                            fields[doc_type]["properties"][one_level_field]["properties"][
                                second_level_field]["type"]
                        if (one_level_field, second_level_field, inner_field_type) not in list_of_field_types:
                            list_of_field_types.append((one_level_field, second_level_field, inner_field_type))
                else:
                    inner_field_type = fields[doc_type]["properties"][one_level_field]["type"]
                    if (one_level_field, inner_field_type) not in list_of_field_types:
                        list_of_field_types.append((one_level_field, inner_field_type))
        else:
            inner_field_type = fields["mappings"][doc_type]["type"]
            if (doc_type, inner_field_type) not in list_of_field_types:
                list_of_field_types.append((doc_type, inner_field_type))
    return list_of_field_types


def fetch_data():
    try:
        source = requests.get(source_host + mappings)
        source.raise_for_status()
        return source
    except requests.exceptions.HTTPError as http_error:
        print http_error
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print e
        sys.exit(1)


response = fetch_data().json()
if version == '5':
    types = get_legacy(response)
if version == '6':
    types = get_fields(response)

with open('es_fields.' + version + '.csv', 'w') as f:
    csv_out = csv.writer(f)
    for row in types:
        csv_out.writerow(row)
