#!/usr/bin/env python2

import requests
import sys
import csv
import argparse
from operator import itemgetter

__author__ = 'Pavlos Daoglou'

mappings = '/_mapping'
list_of_field_types = []


def get_fields_6(res, ind):

    fields = res[ind]["mappings"]["_doc"]["properties"]

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

    return sorted(list_of_field_types, key=itemgetter(0))


def get_fields_5(res,ind):

    fields = res[ind]["mappings"]

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
    return sorted(list_of_field_types,itemgetter(0))


def fetch_data(s, t):

    try:
        source = requests.get(s + mappings, timeout=t)
        source.raise_for_status()
        return source
    except requests.exceptions.HTTPError as http_error:
        print http_error
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print e
        sys.exit(1)


def fetch_version(s, t):

    try:
        rs = requests.get(s, timeout=t)
        rs.raise_for_status()
        data = rs.json()
        ver = data['version']['number']
        return ver.split('.')[0]
    except requests.exceptions.HTTPError as http_error:
        print http_error
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print e
        sys.exit(1)


def main():

    parser = argparse.ArgumentParser(description="Fetch a sorted list of fields from a specific index")
    parser.add_argument('-s', '--source', help='Source host with port eg. "http://host:9200"', required=True)
    parser.add_argument('-i', '--index', help='Index name', required=True)
    parser.add_argument('-t', '--timeout', help='Timeout in seconds - Default: 10', type=int, required=False,
                        default=10)
    parser.add_argument('-o', '--output', help='Output file name - Default: es_fields.ES_VERSION.csv', required=False)
    args = parser.parse_args()

    response = fetch_data(args.source, args.timeout).json()
    version = fetch_version(args.source, args.timeout)

    if version == '5':
        types = get_fields_5(response, args.index)
    elif version == '6':
        types = get_fields_6(response, args.index)
    else:
        print "Not supported elasticseach version: " + version
        sys.exit(1)

    if args.output is None:
        args.output = 'es_fields.' + version + '.csv'

    with open(args.output, 'w') as f:
        csv_out = csv.writer(f)
        for row in types:
            csv_out.writerow(row)


if __name__ == '__main__':
    main()
