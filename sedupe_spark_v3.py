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

input_file = '/home/abhishekks446/data/demo_20.csv'
input_train_file = '/home/abhishekks446/data/dedupe_output/train.csv'
output_file = '/home/abhishekks446/data/dedupe_output/csv_example_output.csv'
settings_file = '/home/abhishekks446/data/dedupe_output/csv_example_learned_settings'
training_file = '/home/abhishekks446/data/dedupe_output/csv_example_training.json'


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

if os.path.exists(settings_file):
    print('reading from', settings_file)
    with open(settings_file, 'rb') as f:
        deduper = dedupe.StaticDedupe(f)

else:
    # Define the fields dedupe will pay attention to
    #
    # Notice how we are telling dedupe to use a custom field comparator
    # for the 'Zip' field.
    fields = [
        {'field' : 'venue', 'type': 'String'},
        {'field' : 'title', 'type': 'String', 'has missing' : True},
        ]

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields)

    # To train dedupe, we feed it a sample of records.
    deduper.sample(data_d, 15000)


    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    # __Note:__ if you want to train from scratch, delete the training_file
    if os.path.exists(training_file):
        print('reading labeled examples from ', training_file)
        with open(training_file, 'rb') as f:
            deduper.readTraining(f)

    # ## Active learning
    # Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.
    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    print('starting active labeling...')

    dedupe.consoleLabel(deduper)

    deduper.train()

    # When finished, save our training away to disk
    with open(training_file, 'w') as tf :
        deduper.writeTraining(tf)

    # Save our weights and predicates to disk.  If the settings file
    # exists, we will skip all the training and learning next time we run
    # this file.
    
    with open(settings_file, 'wb') as sf :
        deduper.writeSettings(sf)


# ## Blocking

print('blocking...')

# ## Clustering

# Find the threshold that will maximize a weighted average of our precision and recall.
# When we set the recall weight to 2, we are saying we care twice as much
# about recall as we do precision.
#
# If we had more data, we would not pass in all the blocked data into
# this function but a representative sample.

threshold = deduper.threshold(data_d, recall_weight=2)

# `match` will return sets of record IDs that dedupe
# believes are all referring to the same entity.

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
            # print(row_id)
            if row_id in cluster_membership :
                cluster_id = cluster_membership[row_id]["cluster id"]
                # print(cluster_id)
                canonical_rep = cluster_membership[row_id]["canonical representation"]
                confidence_s=cluster_membership[row_id]['confidence']
                # print(confidence_s)
                row.insert(0, confidence_s)
                row.insert(0, cluster_id)
                for key in canonical_keys:
                    row.append(canonical_rep[key].encode('utf8'))
            else:
                print(singleton_id)
                row.insert(0, None)
                row.insert(0, singleton_id)
                singleton_id += 1
                for key in canonical_keys:
                    row.append(None)
            writer.writerow(row)
