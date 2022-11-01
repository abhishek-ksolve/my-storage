import json
import boto3
from schemas.models import JobDefinition, Job, session


    
    
jobId='Kbnw7VLXeDLmvp7LpdAALB'

result = session.query(Job).filter(Job.job_uuid == jobId )

if result[0].job_uuid == jobId and result[0].parent_job_uuid == None:
    result = session.query(Job).filter(Job.parent_job_uuid == jobId)
    parent_job_uuid=(result[0].job_definition_id)
    job_details = session.query(JobDefinition).filter(JobDefinition.id == parent_job_uuid)
    
    parentJobName=(job_details[0].algo_config['config']['jobName'])

    ls=[]
    for value in result:
        outputJson=(value.job_result)
        ls.append(outputJson)

    print(ls)        
    # response={ "jobUuid": jobId,"jobname":parentJobName,"subJobs": ls}
    # print(response)
                    
   
else:
    print("child")
    result = session.query(Job).filter(Job.job_uuid == jobId)
    parent_job_uuid=(result[0].job_definition_id)
    job_details = session.query(JobDefinition).filter(JobDefinition.id == parent_job_uuid)
    parentJobName=(job_details[0].algo_config['config']['jobName'])
    for value in result:
        jobUuid=(value.parent_job_uuid)
   

        job_result=(value.job_result)


        response={ "jobUuid": jobUuid,"jobName":parentJobName,
                                    "subJob": job_result}
        print(response)
     

