import json
import boto3
import psycopg2
from schemas.models import Algorithms, AlgoConfigs, session

def lambda_handler(event, context):

    if event['resource'] == "/algo/description/{algoName}":
            algo_name = (event['pathParameters']['algoName']).replace('%20', ' ')

            result = session.query(Algorithms).filter(Algorithms.name == "Probabilistic Record Linkage") 
            result2 = session.query(AlgoConfigs).join( Algorithms, AlgoConfigs.algo_id == Algorithms.id).filter(Algorithms.name == "Probabilistic Record Linkage")
            r = [dict(rowToDict(row)) for row in result2]
            configDe = json.dumps(r)

            name = ""
            des = ""
            for r in result:
                for column in r.__table__.columns:
                    if column.name == "name":
                        name = str(getattr(r, column.name))
                    if column.name == "description":
                        des = str(getattr(r, column.name))
                        
                        
            res_json=('{ "name": "'+name+'", "description": "'+des+'", "config": '+str(configDe)+'} ')
        
            return {'statusCode': 200,'body':res_json}
    if event['resource']=="/algo/description":
        result = session.query(Algorithms).order_by(Algorithms.id)
        res_json = '{'
        index = 0
        for res in result:
            sql2 = session.query(AlgoConfigs).filter(AlgoConfigs.algo_id == res.id)
            r = [dict(rowToDict(row)) for row in sql2]
            configDe = json.dumps(r)
            resp = '{ "name": "'+res.name+'", "description": "'+res.description+'", "config": '+str(configDe)+'} '
            res_json += '"'+str(index)+'":{"'+str(res.name)+'"' +":"+resp+"},"
            index += 1

        res_json = res_json[0: len(res_json)-1] +"}"  
        return {'statusCode': 200,'body':res_json }
def rowToDict(row):
    dict = {}
    for column in row.__table__.columns:
        if (column.name == "config_name" or column.name == "config_parameter_value" or column.name == "description" or column.name == "type" or column.name == "required"):
            if column.name == "required":
                dict[column.name] = 'yes' if str(getattr(row, column.name)) == '1' else 'no'
            else:
                temp = str(getattr(row, column.name)).strip()
                dict[column.name] = temp if temp[0] != '[' else json.loads(temp)
    return dict




# def lambda_handler(event, context):
#     if event['resource']=="/algo/description/{algoName}":
#         algo_name=(event['pathParameters']['algoName']).replace('%20', ' ')
#         # print(algo_name.replace('%20', ' '))
#         connection = psycopg2.connect(user="postgres",
#                                       password="Postgres121",
#                                       host="steerwise-dev.cluster-creeuf398buz.us-east-2.rds.amazonaws.com",
#                                       port="5432",
#                                       database="steerwise")
#         cursor = connection.cursor()
      
#         sql1 = """SELECT * FROM er.algorithms where name =%s """
#         sql2 = """SELECT t1.config_name, t1.config_parameter_value, t1.description, t1.type, t1.required  FROM er.algo_config as t1 INNER JOIN er.algorithms as t2   on t1.algo_id = t2.id where t2.name=%s """

            
        
#         cursor.execute(sql1,[algo_name])
#         result = cursor.fetchall();
#         cursor.execute(sql2,[algo_name])
#         r = [dict((cursor.description[i][0], value if cursor.description[i][0] != 'required' else ('yes' if value == 1 else 'no' )) for i, value in enumerate(row)) for row in cursor.fetchall()]
#         configDe = json.dumps(r)

        
#         res_json=('{ "name": "'+result[0][1]+'", "description": "'+result[0][6]+'", "config": '+str(configDe)+'} ')
#         connection.commit()
#         connection.close()
        
#         return {'statusCode': 200,'body':res_json}
        
#     if event['resource']=="/algo/description":
#         connection = psycopg2.connect(user="postgres",
#                                       password="Postgres121",
#                                       host="steerwise-dev.cluster-creeuf398buz.us-east-2.rds.amazonaws.com",
#                                       port="5432",
#                                       database="steerwise")
#         cursor = connection.cursor()
        
#         sql = "SELECT * FROM er.algorithms ORDER BY id"
#         cursor.execute(sql)
#         result = cursor.fetchall()
#         finalResult = '{'
#         for el in result:
#             print(el[0], end="\n")
#             sql2 = 'SELECT config_name, config_parameter_value, description, type, required  FROM er.algo_config WHERE algo_id='+str(el[0])
#             cursor.execute(sql2)
#             flag = True
#             r = [dict((cursor.description[i][0], value if cursor.description[i][0] != 'required' else ('yes' if value == 1 else 'no') ) \
#                     for i, value in enumerate(row)) for row in cursor.fetchall()]
#             configDe = json.dumps(r)
#             res = '{ "name": "'+el[1]+'", "description": "'+el[6]+'", "config": '+str(configDe)+'} '
#             finalResult += '"'+str(el[0])+'"' +":"+res+","
        
#         finalResult = finalResult[0: len(finalResult)-1] +"}"
#         # finalResult += "}"
#         res_json=finalResult
#         connection.commit()
#         connection.close()
 
#         return {'statusCode': 200,'body':res_json }