from functools import wraps
from string import Template

import json

def on_fail(message,another_details):
    def on_fail_dec(func):
        @wraps(func)
        def on_fail_wrapper(*args, **kw):

            
            try:
                return func(*args, **kw)
            except Exception as e:
                temp = Template("""{ "state": "FAILURE","error": { "message":" $message occour at $occourAt "},"jobDetails": $other }""")
                

                message_addon = str(e).replace("'", '')
                another_detail=str(another_details).replace("'", '"')
            raise Exception(temp.substitute(message=message, occourAt=message_addon,other=another_detail))
        return on_fail_wrapper
    return on_fail_dec