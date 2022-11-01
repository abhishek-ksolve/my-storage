# import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from future.builtins import next
# import re
# from numpy import nan
# import dedupe
# from unidecode import unidecode
# import json
# import pyspark.sql.functions as F
# from pyspark.sql.types import IntegerType,FloatType
# from boto3 import client

# ## @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)



# BucketS3 = 'steerwise-bucket'
# ArgumentsJson = 'dedupe/DedupeRecordLink/Input/app.json'
# LabelJson = 'dedupe/DedupeRecordLink/Input/label.json'
# client = client('s3',
#                  aws_access_key_id='AKIA5FYUQOE6KKEFWNYH',
#                  aws_secret_access_key='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'
#                 )
# result = client.get_object(Bucket=BucketS3, Key=ArgumentsJson) 
# jsoncontent = result["Body"].read().decode()
# data = json.loads(jsoncontent)


# InputCsvLocationFirst = data["input_1"]
# InputCsvLocationSecond = data["input_2"]
# Output = data["output"]
# ListFields = data["Fields"]
# PrimaryKey=data["primary_key"]
# Constraint=data["Constraint"]

# Labelresult = client.get_object(Bucket=BucketS3, Key=LabelJson) 
# LabeledExamples = Labelresult["Body"]





# df1= spark.read.option("header","true").csv(InputCsvLocationFirst)
# df2= spark.read.option("header","true").csv(InputCsvLocationSecond)

# def preProcess(column):
#     """
#     Do a little bit of data cleaning with the help of Unidecode and Regex.
#     Things like casing, extra spaces, quotes and new lines can be ignored.
#     """
#     import unidecode
#     # column = column.decode("utf8")
#     column = unidecode.unidecode(column)
#     column = re.sub('  +', ' ', column)
#     column = re.sub('\n', ' ', column)
#     column = column.strip().strip('"').strip("'").lower().strip()
#     if not column :
#         column = None
#     return column
# def readData(df):
#     """
#     Read in our data from a CSV file and create a dictionary of records,
#     where the key is a unique record ID and each value is dict
#     """
    
#     data_d = {}
#     dfn=df.na.fill("null")
#     results = dfn.toJSON().map(lambda j: json.loads(j)).collect()
#     for row in results:
#         clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
#         row_id = row[PrimaryKey]
#         data_d[row_id] = dict(clean_row)

#     return data_d

# DataFirst = readData(df1)
# DataSecond = readData(df2)

# deduper = dedupe.RecordLink(ListFields)
# deduper.prepare_training(DataFirst,DataSecond, LabeledExamples)
# deduper.train()
# dupes_outout=deduper.join(DataFirst,DataSecond, threshold=0.5,constraint=Constraint)

# lis=[list(elem) for elem in dupes_outout]
# ls=[]
# for row in lis:
#     a=list(row[0])
#     c=[str(a[0]),str(a[1]),float(row[1])]
#     ls.append(c)
# columns = ["df1", "df2","confidence_score"]    
# dataframe = spark.createDataFrame(ls,columns)
# dataframe.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save(Output)



# job.init(args['JOB_NAME'], args)
# job.commit()
import boto3

client = boto3.client('glue')
response = client.get_job_runs(
    JobName='jobtestcr-1PreProcessor'
)

print(response)