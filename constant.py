from enum import Enum

class ExceptionMessage(Enum):
    DATA_REQUIRED = {
        "message": "Data is missing," ,
        "code": "1001"
    }
    URL_INCORRECT = {
        "message": "Incorrect url syntax," ,
        "code": "1002"
    }
    DIVISION_BY_ZERO = {
        "message": "Can not be divide by zero," ,
        "code": "007"
    }
    INDEX_ERROR = {
        "message": "string index out of range," ,
        "code": "008"
    }