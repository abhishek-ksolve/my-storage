import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from future.builtins import next
import re
from numpy import nan
import dedupe
from unidecode import unidecode
import json
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType,FloatType
from boto3 import client

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','Input1','Input2','labelJsonLocation','output','listFields','primaryKey','threshold','constraint'])


Input1 = args["Input1"]
Input2 = args["Input2"]
LabelJsonLocation = args["labelJsonLocation"]
Output = args["output"]
ListFields = json.loads(args["listFields"])
PrimaryKey=args["primaryKey"]
Threshold=float(args["threshold"])
Constraint=args["constraint"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)



BucketS3 = 'steerwise-bucket'
client = client('s3',
                 aws_access_key_id='AKIA5FYUQOE6KKEFWNYH',
                 aws_secret_access_key='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'
                )
Labelresult = client.get_object(Bucket=BucketS3, Key=LabelJsonLocation) 
LabeledExamples = Labelresult["Body"]




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
def readData(df):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID and each value is dict
    """
    
    data_d = {}
    dfn=df.na.fill("")
    results = dfn.toJSON().map(lambda j: json.loads(j)).collect()
    for row in results:
        clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
        row_id = row[PrimaryKey]
        data_d[row_id] = dict(clean_row)

    return data_d

if Input2=="null":
    df1= spark.read.option("header","true").csv(Input1)
    DataFirst = readData(df1)
    deduper = dedupe.Dedupe(ListFields)

    deduper.prepare_training(DataFirst, LabeledExamples)
    deduper.train()
    clustered_dupes = deduper.partition(DataFirst, Threshold)

    cluster_membership = {}
    cluster_id = 0
    for (cluster_id, cluster) in enumerate(clustered_dupes):
        id_set, scores = cluster
        cluster_d = [DataFirst[c] for c in id_set]
        for record_id, score in zip(id_set, scores) :
            cluster_membership[record_id] = {
                "cluster id" : cluster_id,
                "confidence": score
            }

    reader= df1
    ls=[]
    for row in reader.collect():
        
        row_id = (row[PrimaryKey])
        if row_id in cluster_membership :
            cluster_id = cluster_membership[row_id]["cluster id"]
            confidence_s=cluster_membership[row_id]['confidence']
            g = str(float("{0:.2f}".format(confidence_s)))
            d_list=[row_id,cluster_id,g]

            ls.append(d_list)

    columns = ["row_id", "cluster_id","confidence_score"]    
    dataframe = spark.createDataFrame(ls, columns,FloatType())
    pr_str = "reader." + PrimaryKey + " == dataframe." +"row_id"
    new_df=reader.join(dataframe ,eval(pr_str),"inner").drop("row_id")
    new_df.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save(Output)
    

else:
    df1= spark.read.option("header","true").csv(Input1)
    df2= spark.read.option("header","true").csv(Input2)

    DataFirst = readData(df1)
    DataSecond = readData(df2)

    deduper = dedupe.RecordLink(ListFields)
    deduper.prepare_training(DataFirst,DataSecond, LabeledExamples)
    deduper.train()
    if Constraint=="null":
        dupes_outout=deduper.join(DataFirst,DataSecond, threshold=Threshold)
    else:
        dupes_outout=deduper.join(DataFirst,DataSecond, threshold=Threshold,constraint=Constraint)

    lis=[list(elem) for elem in dupes_outout]
    ls=[]
    for row in lis:
        a=list(row[0])
        c=[str(a[0]),str(a[1]),float(row[1])]
        ls.append(c)
    columns = ["df1", "df2","confidence_score"]    
    dataframe = spark.createDataFrame(ls,columns)
    dataframe.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save(Output)



job.init(args['JOB_NAME'], args)
job.commit()