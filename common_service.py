
class DataServiceException(Exception):
    def __init__(self, text, varName):
        super().__init__()
        jsonContant = text.value
        jsonContant['message'] += " Occure at "+varName+"."
        self.status = jsonContant
        # print(jsonContant)
        # self.status = {"joiID": "job101", "statusCode": text, "message": msg+" "+varName}
 