from future.builtins import next

import os
import csv
import re
import collections
import logging
import optparse
from numpy import nan

import dedupe
from unidecode import unidecode
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.context import SparkContext
# from pyspark.sql.functions import *
# from pyspark.sql.session import SparkSession
# import json

# ## Logging

# Dedupe uses Python logging to show or suppress verbose output. Added for convenience.
# To enable verbose logging, run `python examples/csv_example.py -v`
optp = optparse.OptionParser()
optp.add_option('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
(opts, args) = optp.parse_args()
log_level = logging.WARNING
if opts.verbose :
    if opts.verbose == 1:
        log_level = logging.INFO
    elif opts.verbose >= 2:
        log_level = logging.DEBUG
logging.getLogger().setLevel(log_level)

# spark = SparkSession \
#                 .builder \
#                 .appName("Splink") \
#                 .master("local[*]") \
#                 .getOrCreate()
# ## Setup

input_file = '/home/abhishekks446/pyspark/my_project_dir/dedupe/data_input/demo_20.csv'
output_file = '/home/abhishekks446/pyspark/my_project_dir/dedupe/data_output/csv_example_output_v2.csv'



def preProcess(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    import unidecode
    # column = column.decode("utf8")
    column = unidecode.unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    if not column :
        column = None
    return column


def readData(filename):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID and each value is dict
    """

    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
            row_id = int(row['unique_id'])
            data_d[row_id] = dict(clean_row)

    return data_d


print('importing data ...')
data_d = readData(input_file)

# ## Training

fields = [
    {'field' : 'venue', 'type': 'String'},
    {'field' : 'year', 'type': 'String'},
    {'field' : 'title', 'type': 'Exact', 'has missing' : True}
    ]

# Create a new deduper object and pass our data model to it.
deduper = dedupe.Dedupe(fields)

# To train dedupe, we feed it a sample of records.
deduper.sample(data_d, 15000)


dedupe.consoleLabel(deduper)

deduper.train()


threshold = deduper.threshold(data_d, recall_weight=2)


print('clustering...')
clustered_dupes = deduper.match(data_d, threshold)

print('# duplicate sets', len(clustered_dupes))

# ## Writing Results

# Write our original data back out to a CSV with a new column called
# 'Cluster ID' which indicates which records refer to each other.

cluster_membership = {}
cluster_id = 0
for (cluster_id, cluster) in enumerate(clustered_dupes):
    id_set, scores = cluster
    cluster_d = [data_d[c] for c in id_set]
    canonical_rep = dedupe.canonicalize(cluster_d)
    for record_id, score in zip(id_set, scores) :
        cluster_membership[record_id] = {
            "cluster id" : cluster_id,
            "canonical representation" : canonical_rep,
            "confidence": score
        }

singleton_id = cluster_id + 1

with open(output_file, 'w') as f_output:
    writer = csv.writer(f_output)

    with open(input_file) as f_input :
        reader = csv.reader(f_input)

        heading_row = next(reader)
        heading_row.insert(0, 'confidence_score')
        heading_row.insert(0, 'Cluster ID')
        canonical_keys = canonical_rep.keys()
        for key in canonical_keys:
            heading_row.append('canonical_' + key)

        writer.writerow(heading_row)

        for row in reader:
            row_id = int(row[0])
            if row_id in cluster_membership :
                cluster_id = cluster_membership[row_id]["cluster id"]
                # canonical_rep = cluster_membership[row_id]["canonical representation"]
                row.insert(0, cluster_membership[row_id]['confidence'])
                row.insert(0, cluster_id)
                # for key in canonical_keys:
                #     row.append(canonical_rep[key].encode('utf8'))
            else:
                row.insert(0, None)
                row.insert(0, singleton_id)
                singleton_id += 1
                # for key in canonical_keys:
                #     row.append(None)
            writer.writerow(row)
