from common_service import DataServiceException
from inputs import data
from constant import ExceptionMessage

try:
    if "dataset1" not  in data:
        raise DataServiceException(ExceptionMessage.DATA_REQUIRED, "dataset1")

    if data['dataset1'][0:5] != 's3://':
        raise DataServiceException(ExceptionMessage.URL_INCORRECT,"dataset1")
except DataServiceException as dse:
    print(dse.status)
    
try:
    if "dataset2" in data:
        if data['dataset2'][0:5] != 's3://':
            raise DataServiceException(ExceptionMessage.URL_INCORRECT,"dataset2")
except DataServiceException as dse:
    print(dse.status)
    
try:
    if "output" not  in data:
        raise DataServiceException(ExceptionMessage.DATA_REQUIRED, "output")

    if data['output'][0:5] != 's3://':
        raise DataServiceException(ExceptionMessage.URL_INCORRECT,"output")
except DataServiceException as dse:
    print(dse.status)
    
    
    
