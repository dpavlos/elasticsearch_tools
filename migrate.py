#!/usr/bin/env python2

import requests
import pandas as pd
import io
import json
import sys
import argparse

__author__ = 'Pavlos Daoglou'

parser = argparse.ArgumentParser(description="Migrate indices from Elasticsearch v1.3 to v5.x")
parser.add_argument('-s', '--source', help='Source host with port eg. "http://host:9200"', required=True)
parser.add_argument('-d', '--destination', help='Destination host with port  eg. "http://host:9200"', required=True)
parser.add_argument('-t', '--timeout', help='Timeout in seconds - Default: 10', type=int, required=False, default=10)
args = parser.parse_args()

failed = []
getindex = '/_cat/indices?v'
reindex = '/_reindex?wait_for_completion=true'
schema = json.dumps({'source': {'remote': {'host': 'localhost'}, 'index': 'my_index'}, 'dest': {'index': 'my_index'}})
req = json.loads(schema)

print 'Fetching lists of indices from source and target hosts...'
try:
    source = requests.get(args.source + getindex, timeout=args.timeout)
    source.raise_for_status()
    # Since elasticsearch 5 we can list also closed indices by using the 'status' option
    # Don't try to reindex already closed indices on target host
    target = requests.get(args.destination + getindex + '&h=index&s=status', timeout=args.timeout)
    target.raise_for_status()
except requests.exceptions.HTTPError as http_error:
    print http_error
    sys.exit(1)
except requests.exceptions.RequestException as e:
    print e
    sys.exit(1)

# Build the df in CSV format
sdf = pd.read_csv(io.StringIO(unicode(source.content)), sep='\s+')
tdf = pd.read_csv(io.StringIO(unicode(target.content)))

# Sort the list of source indices. Just for viewing purposes during the reindexing phase...
sdf_sorted = sdf.sort_values(by=['index'], ascending=True)

print 'Reindexing...'
# Iterate over the source index names
for item in sdf_sorted['index'].values:
    # Check if the specific index has already been migrated
    if not tdf['index'].str.contains(item).any():
        req['source']['remote']['host'] = args.source
        req['source']['index'] = item
        req['dest']['index'] = item

        # Reindex onto the target host
        try:
            p = requests.post(args.destination + reindex, data=json.dumps(req), timeout=None)
            p.raise_for_status()
        except requests.exceptions.HTTPError as http_error:
            print 'Reindexing of: ' + item + ' failed ' + ' with error: ', http_error
            failed.append(item)
        except requests.exceptions.RequestException as e:
            print 'Reindexing of: ' + item + ' failed ' + ' with error: ', e
            failed.append(item)
    else:
        print 'Index: ' + item + ' has already been migrated. Skipping...'

if failed:
    print '\nFailed indices: ' + ', '.join(failed)
else:
    print '\nMigration completed successfully'
