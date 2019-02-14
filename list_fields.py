#!/usr/bin/env python2

import requests
import sys
import csv
import argparse
from operator import itemgetter

__author__ = 'Pavlos Daoglou'

list_of_field_types = []


def get_fields(res, ind, v):

    if v == '5':
        fields = res[ind]["mappings"]
    if v == '6':
        fields = res[ind]["mappings"]["_doc"]["properties"]

    for doc_type in fields:
        if "properties" in fields[doc_type]:
            for field in fields[doc_type]["properties"]:
                if "properties" in fields[doc_type]["properties"][field]:
                    for inner_field in fields[doc_type]["properties"][field]["properties"]:
                        field_type = \
                            fields[doc_type]["properties"][field]["properties"][
                                inner_field]["type"]
                        if (field, inner_field, field_type) not in list_of_field_types:
                            list_of_field_types.append((field, inner_field, field_type))
                else:
                    field_type = fields[doc_type]["properties"][field]["type"]
                    if (field, field_type) not in list_of_field_types:
                        list_of_field_types.append((field, field_type))
        else:
            field_type = fields[doc_type]["type"]
            if (doc_type, field_type) not in list_of_field_types:
                list_of_field_types.append((doc_type, field_type))
    return sorted(list_of_field_types, key=itemgetter(0))


def fetch_data(s, i, t):

    try:
        source = requests.get(s + '/' + i + '/_mapping', timeout=t)
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

    response = fetch_data(args.source, args.index, args.timeout).json()
    version = fetch_version(args.source, args.timeout)

    if version == '5' or version == '6':
        types = get_fields(response, args.index, version)
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
