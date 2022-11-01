import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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
import json
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType,FloatType
from boto3 import client

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
BUCKET = 'steerwise-bucket'
FILE_TO_READ = 'dedupe/input_data/app.json'
client = client('s3',
                 aws_access_key_id='AKIA5FYUQOE6KKEFWNYH',
                 aws_secret_access_key='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'
                )
result = client.get_object(Bucket=BUCKET, Key=FILE_TO_READ) 
json_content = result["Body"].read().decode()
data = json.loads(json_content)
input = data["input"]
output = data["output"]
l_fields = data["Fields"]
primary_key=data["primary_key"]
df= spark.read.option("header","true").csv(input)

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
def readData():
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID and each value is dict
    """
    
    data_d = {}
    results = df.toJSON().map(lambda j: json.loads(j)).collect()
    for row in results:
        clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
        row_id = row[primary_key]
        data_d[row_id] = dict(clean_row)

    return data_d
data_d = readData()
fields = l_fields
deduper = dedupe.Dedupe(fields)
deduper.prepare_training(data_d)

labeled_examples = {'match' : [({'store_name' :'hy-vee #3 / bdi / des moines','address' : '3221 se 14th st','zip_code' : '50320'},
			{'store_name' :'hy-vee food store #3 / des moines','address' : '3221 se 14th st','zip_code' : '50315'}),
			
			({'store_name': 'spirits liquor / grimes', 'address': '109 e 1st st. # b', 'zip_code': '50111'},
			{'store_name': 'spirits liquor', 'address': '109 e 1st st. # b', 'zip_code': '50111'}),
			
			({'store_name': 'spirits liquor / grimes', 'address': '109 e 1st st. # b', 'zip_code': '50111'},
			{'store_name': 'spirits liquor', 'address': '109 e 1st st. # b', 'zip_code': '50111'}),
                               
			({'store_name' :'hy-vee #3 / bdi / des moines','address' : '3221 se 14th st','zip_code' : '50320'},
			{'store_name' :'hy-vee food store #3 / des moines','address' : '3221 se 14th st','zip_code' : '50315'}),


			({'store_name' :'hy-vee #3 / bdi / des moines','address' : '3221 se 14th st','zip_code' : '50320'},
			{'store_name' :'hy-vee food store #3 / des moines','address' : '3221 se 14th st','zip_code' : '50315'})],
                    
                    
                    
           'distinct' : [({'store_name' : 'hy-vee food store #4 / cedar rapids','address' : '1556 first avenue ne','zip_code' : '52402'},
			{'store_name' : 'hy-vee food store #5 / cedar rapids','address' : '3235 oakland road ne','zip_code': '52402'}),


			({'store_name' : 'hy-vee food store #2 / cedar rapids','address' : '279 collins road ne','zip_code' : '52402'},
			{'store_name' : 'hy-vee food store #5 / cedar rapids','address' : '3235 oakland road ne','zip_code' : '52402'}),
           
            ({'store_name' : 'hy-vee food store #2 / cedar rapids','address' : '279 collins road ne','zip_code' : '52402'},
			{'store_name' : 'hy-vee food store #5 / cedar rapids','address' : '3235 oakland road ne','zip_code' : '52402'}),



			({'store_name' : 'prime mart / cedar falls','address' : '2323 main st','zip_code' : '50613'},
			{'store_name' : 'prime mart - cedar falls','address' : '2728 center st','zip_code' : '50613'}),


			({'store_name' :'pony creek liquor / glenwood','address' : '411 s locust st','zip_code' : '51534'},
			{'store_name' :'pony creek liquor / glenwood','address' : '924 s. locust st','zip_code' : '51534'}),
            		]}

deduper.mark_pairs(labeled_examples)
deduper.train()
clustered_dupes = deduper.partition(data_d, 0.5)
cluster_membership = {}
for cluster_id, (records, scores) in enumerate(clustered_dupes):
    for record_id, score in zip(records, scores):
        cluster_membership[record_id] = {
            "Cluster ID": cluster_id,
            "confidence_score": score
        }
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

reader= df
ls=[]
for row in reader.collect():
    
    row_id = (row[primary_key])
    if row_id in cluster_membership :
        cluster_id = cluster_membership[row_id]["cluster id"]
        confidence_s=cluster_membership[row_id]['confidence']
        g = str(float("{0:.2f}".format(confidence_s)))
        d_list=[row_id,cluster_id,g]

        ls.append(d_list)


    else:
        d_list=[row_id,singleton_id,'0']
        singleton_id += 1
        ls.append(d_list)
columns = ["row_id", "cluster_id","confidence_score"]    
dataframe = spark.createDataFrame(ls, columns,FloatType())
new_df=reader.join(dataframe ,reader.unique_id ==  dataframe.row_id,"inner")
new_df.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save(output)
job.init(args['JOB_NAME'], args)
job.commit()