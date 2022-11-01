import json
import boto3
import psycopg2
lambda_client = boto3.client('lambda','us-east-1')
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    if event['resource']=="/algo/description/{algoName}":
        algo_name=(event['pathParameters']['algoName']).replace('%20', ' ')
        # print(algo_name.replace('%20', ' '))
        connection = psycopg2.connect(user="postgres",
                                      password="Postgres121",
                                      host="steerwise-dev.cluster-creeuf398buz.us-east-2.rds.amazonaws.com",
                                      port="5432",
                                      database="steerwise")
        cursor = connection.cursor()
      
        sql1 = """SELECT * FROM er.algorithms where name =%s """
        sql2 = """SELECT t1."configName", t1."configParameterValue", t1."description", t1.type, t1.required  FROM er."algoConfig" as t1 INNER JOIN er.algorithms as t2   on t1."algoId" = t2.id where t2.name=%s """

            
        
        cursor.execute(sql1,[algo_name])
        result = cursor.fetchall();
        cursor.execute(sql2,[algo_name])
        r = [dict((cursor.description[i][0], value if cursor.description[i][0] != 'required' else ('yes' if value == 1 else 'no' )) for i, value in enumerate(row)) for row in cursor.fetchall()]
        configDe = json.dumps(r)
        print(result)
        print(r)
        
        res_json=('{ "name": "'+result[0][1]+'", "description": "'+result[0][6]+'", "config": '+str(configDe)+'} ')
        connection.commit()
        connection.close()
        return {'statusCode': 200,'body':res_json }
        
    if event['resource']=="/algo/description":
        connection = psycopg2.connect(user="postgres",
                                      password="Postgres121",
                                      host="steerwise-dev.cluster-creeuf398buz.us-east-2.rds.amazonaws.com",
                                      port="5432",
                                      database="steerwise")
        cursor = connection.cursor()
        
        sql = "SELECT * FROM er.algorithms ORDER BY id"
        cursor.execute(sql)
        result = cursor.fetchall()
        finalResult = '{'
        for el in result:
            print(el[0], end="\n")
            sql2 = 'SELECT "configName", "configParameterValue", description, type, required  FROM er."algoConfig" WHERE "algoId"='+str(el[0])
            cursor.execute(sql2)
            flag = True
            r = [dict((cursor.description[i][0], value if cursor.description[i][0] != 'required' else ('yes' if value == 1 else 'no') ) \
                    for i, value in enumerate(row)) for row in cursor.fetchall()]
            configDe = json.dumps(r)
            res = '{ "name": "'+el[1]+'", "description": "'+el[6]+'", "config": '+str(configDe)+'} '
            finalResult += '"'+str(el[0])+'"' +":"+res+","

        
        finalResult += "}"
        res_json=finalResult
        connection.commit()
        connection.close()
 
        return {'statusCode': 200,'body':res_json }
        
    if event['resource']=="/execute":
        
        data = json.loads(event['body'])
        jobName=data["jobName"]
        Input1=data["dataset1"]
        if "dataset2" in data:
            Input2 = data["dataset2"]
        else:
            Input2 = "null"
        
        input_param = {"Input1": Input1,"Input2": Input2,"jobName":jobName}
        algoConfig={"config":data}
        jsonAlgoConfig=(json.dumps(algoConfig))
        
        try:
            
            connection = psycopg2.connect(user="postgres",
                                          password="Postgres121",
                                          host="steerwise-dev.cluster-creeuf398buz.us-east-2.rds.amazonaws.com",
                                          port="5432",
                                          database="steerwise")
            cursor = connection.cursor()
           
            postgres_insert_query = """INSERT INTO er."jobDefinition"("datasourceId", "infraProviderId","algoConfig")VALUES (%s, %s, %s)"""
            record_to_insert = (1,1,jsonAlgoConfig)
            cursor.execute(postgres_insert_query, record_to_insert)
            
            sql = """SELECT currval(pg_get_serial_sequence('er."jobDefinition"','id'))"""
            
            cursor.execute(sql)
            result = cursor.fetchall();
            if result:
                ls=([seq[0] for seq in result])
                jobDefinitionId=ls[0]
                print(jobDefinitionId)
                
                postgres_insert_query = """INSERT INTO er.job("jobDefinitionId")VALUES (%s)"""
                record_to_insert=(jobDefinitionId,)
                cursor.execute(postgres_insert_query, record_to_insert)
                sql = """SELECT currval(pg_get_serial_sequence('er.job','id'))"""
                cursor.execute(sql)
                result1 = cursor.fetchall();
                ls=([seq[0] for seq in result1])
                parentJobId=ls[0]
                print(parentJobId)
                print("successfull")
            else:
                print("error")
                
    
        
            connection.commit()
            cursor.close()
            connection.close()
            
            input_param_jobDefinitionId=input_param.update({"jobDefinitionId":jobDefinitionId,"parentJobId":parentJobId})
    
        except (Exception) as error:
            print( error)
        
 
        
        try:

            for algo_list in data['algorithms'][0].items():
              
                if "Probabilistic record linkage"== algo_list[0]:
                    
                    probabilisticRecordLinkage=data['algorithms'][0]["Probabilistic record linkage"]
                    probabilisticRecordLinkage.update(input_param)
                    responseProbabilisticRecordLinkage = lambda_client.invoke(
                    FunctionName = 'arn:aws:lambda:us-east-1:905744249148:function:probabilisticRecordLinkageSteerwise',
                    InvocationType = 'RequestResponse',
                    Payload = json.dumps(probabilisticRecordLinkage))
                    responseFromProbabilisticRecordLinkageLamda = json.load(responseProbabilisticRecordLinkage['Payload'])
                    
                    
                if "Supervised DeDuplication"== algo_list[0]:
                    supervisedDeDuplication=data['algorithms'][0]["Supervised DeDuplication"]
                    supervisedDeDuplication.update(input_param)
                    responseSupervisedDeDuplication = lambda_client.invoke(
                    FunctionName = 'arn:aws:lambda:us-east-1:905744249148:function:supervisedDeDuplication',
                    InvocationType = 'RequestResponse',
                    Payload = json.dumps(supervisedDeDuplication))
                    responseFromResponseSupervisedDeDuplicationLambda = json.load(responseSupervisedDeDuplication['Payload'])
                    
                # if "Blocking Based Schema Agnostic"== algo_list[0]:
                #     blockingBasedSchemaAgnostic=data['algorithms'][0]["Blocking Based Schema Agnostic"]
                #     blockingBasedSchemaAgnostic.update(input_param)
                #     responseBlockingBasedSchemaAgnostic = lambda_client.invoke(
                #     FunctionName = 'arn:aws:lambda:us-east-1:905744249148:function:blockingBasedSchemaAgnosticSteerwise',
                #     InvocationType = 'RequestResponse',
                #     Payload = json.dumps(blockingBasedSchemaAgnostic))
                #     responseFromResponseBlockingBasedSchemaAgnosticLambda = json.load(responseBlockingBasedSchemaAgnostic['Payload'])
                    
                # if "Schema Based"== algo_list[0]:
                #     schemaBased=data['algorithms'][0]["Schema Based"]
                #     schemaBased.update(input_param)
                #     responseSchemaBased = lambda_client.invoke(
                #     FunctionName = 'arn:aws:lambda:us-east-1:905744249148:function:schemaBasedSteerwise',
                #     InvocationType = 'RequestResponse',
                #     Payload = json.dumps(schemaBased))
                #     responseFromSchemaBasedLambda = json.load(responseSchemaBased['Payload'])
                    
                # if "Progressive Schema Agnostic"== algo_list[0]:
                #     print("Progressive Schema Agnostic")
                #     progressiveSchemaAgnostic=data['algorithms'][0]["Progressive Schema Agnostic"]
                #     progressiveSchemaAgnostic.update(input_param)
                #     responseProgressiveSchemaAgnostic = lambda_client.invoke(
                #     FunctionName = 'arn:aws:lambda:us-east-1:905744249148:function:progressiveSchemaAgnosticSteerwise',
                #     InvocationType = 'RequestResponse',
                #     Payload = json.dumps(progressiveSchemaAgnostic))
                #     responseFromsProgressiveSchemaAgnosticLambda = json.load(responseProgressiveSchemaAgnostic['Payload'])
                    
        except (Exception) as error:
            print( error)            
       
            
        job_details1=responseFromProbabilisticRecordLinkageLamda["job_submission_details"]
        job_details2=responseFromResponseSupervisedDeDuplicationLambda["job_submission_details"]
        # job_details3=responseFromResponseBlockingBasedSchemaAgnosticLambda["job_submission_details"]
        # job_details4=responseFromSchemaBasedLambda["job_submission_details"]
        # print(job_details2)
        # print(job_details3)
        # print(job_details4)
        
        

        response={ "jobId": parentJobId,
                  "subJobs": [job_details1,job_details2]}
      
        
        
        return {'statusCode': 200,'body': json.dumps(response)}
        
        
    if event['resource']=="/status/job/{jobId}":
        jobId=(event['pathParameters']['jobId'])
        
        if jobId.isnumeric():
            connection = psycopg2.connect(user="postgres",
                                              password="Postgres121",
                                              host="steerwise-dev.cluster-creeuf398buz.us-east-2.rds.amazonaws.com",
                                              port="5432",
                                              database="steerwise")
            cursor = connection.cursor()
            sql_get_status = """SELECT "jobSubmissionDetails",status FROM er.job where "parentJobId" =%s """
            cursor.execute(sql_get_status,[jobId])
            result = cursor.fetchall();
            print(result)
            ls=[]
            for value in result:
                subdetail=(value[0])
                status={"status":value[1]}
                subdetail.update(status)
                ls.append(subdetail)
                
            response={ "jobId": jobId,
                              "status": ls}
                              
            return {'statusCode': 200,'body': json.dumps(response)}
            # return {'statusCode': 200,'body': json.dumps(result)}
        else:
            connection = psycopg2.connect(user="postgres",
                                              password="Postgres121",
                                              host="steerwise-dev.cluster-creeuf398buz.us-east-2.rds.amazonaws.com",
                                              port="5432",
                                              database="steerwise")
            cursor = connection.cursor()
            print(jobId)
            sql_get_status = """SELECT "parentJobId","jobSubmissionDetails",status FROM er.job where "jobSubmissionId" =%s """
            cursor.execute(sql_get_status,[jobId])
            result = cursor.fetchall();
            jobId=(result[0][0])
            JobDetail=(result[0][1])
            status={"status":(result[0][2])}
            JobDetail.update(status)

        
            JobRunStatus={"jobId":result[0][0],"subJob": JobDetail}
            
            return {'statusCode': 200,'body': json.dumps(JobRunStatus)}

        
        
    if event['resource']=="/output/job/{jobId}":
        jobId=(event['pathParameters']['jobId'])
        if jobId.isnumeric():
            connection = psycopg2.connect(user="postgres",
                                              password="Postgres121",
                                              host="steerwise-dev.cluster-creeuf398buz.us-east-2.rds.amazonaws.com",
                                              port="5432",
                                              database="steerwise")
            cursor = connection.cursor()
            sql_get_status = """SELECT "jobResult" FROM er.job where "parentJobId" =%s """
            cursor.execute(sql_get_status,[jobId])
            result = cursor.fetchall();
            
            response={ "jobId": jobId,
                              "subJobs": result}
                              
            return {'statusCode': 200,'body': json.dumps(response)}
        else:
            connection = psycopg2.connect(user="postgres",
                                              password="Postgres121",
                                              host="steerwise-dev.cluster-creeuf398buz.us-east-2.rds.amazonaws.com",
                                              port="5432",
                                              database="steerwise")
            cursor = connection.cursor()
            sql_get_status = """SELECT "parentJobId","jobResult" FROM er.job where "jobSubmissionId" =%s """
            cursor.execute(sql_get_status,[jobId])
            result = cursor.fetchall();
            jobId1=(result[0][0])
            result=(result[0][1])
            
            response={ "jobId": jobId1,
                              "subJob": result}
                              
            return {'statusCode': 200,'body': json.dumps(response)}
            
            connection.commit()
            cursor.close()
            connection.close()