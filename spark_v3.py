
"""
This code demonstrates how to use dedupe with a comma separated values
(CSV) file. All operations are performed in memory, so will run very
quickly on datasets up to ~10,000 rows.

We start with a CSV file containing our messy data. In this example,
it is listings of early childhood education centers in Chicago
compiled from several different sources.

The output will be a CSV with our clustered results.

For larger datasets, see our [mysql_example](http://open-city.github.com/dedupe/doc/mysql_example.html)
"""
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
import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
import json

spark = SparkSession \
                .builder \
                .appName("Splink") \
                .master("local[*]") \
                .getOrCreate()
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


# ## Setup

input_file = '/home/abhishekks446/pyspark/my_project_dir/dedupe/data_input/demo_20.csv'
output_file = '/home/abhishekks446/pyspark/my_project_dir/dedupe/data_output/csv_example_output.csv'



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
    df= spark.read.option("header","true").csv(filename)
    data_d = {}
    results = df.toJSON().map(lambda j: json.loads(j)).collect()
    for row in results:
        clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
        row_id = row["unique_id"]
        data_d[row_id] = dict(clean_row)

    return data_d


print('importing data ...')
data_d = readData(input_file)


fields = [
    {'field' : 'venue', 'type': 'String'},
    {'field' : 'year', 'type': 'String'},
    {'field' : 'title', 'type': 'Exact', 'has missing' : True}
    ]
deduper = dedupe.Dedupe(fields)
deduper.sample(data_d, 15000)
print('starting active labeling...')

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

reader= spark.read.option("header","true").csv(input_file).withColumn("confidence_score").withColumn("Cluster ID")
row_id = int(row[0])
if row_id in cluster_membership :

# with open(output_file, 'w') as f_output:
#     writer = csv.writer(f_output)

#     reader= spark.read.option("header","true").csv(input_file)

#     heading_row = next(reader)
#     heading_row.insert(0, 'confidence_score')
#     heading_row.insert(0, 'Cluster ID')
#     writer.writerow(heading_row)

#     for row in reader:
#         row_id = int(row[0])
#         if row_id in cluster_membership :
#             cluster_id = cluster_membership[row_id]["cluster id"]
#           
#             row.insert(0, cluster_membership[row_id]['confidence'])
#             row.insert(0, cluster_id)
#         else:
#             row.insert(0, None)
#             row.insert(0, singleton_id)
#             singleton_id += 1
#         writer.writerow(row)
