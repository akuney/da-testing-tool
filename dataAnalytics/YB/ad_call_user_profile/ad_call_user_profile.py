import json
@outputSchema("device_history:chararray")  
def toJSON(json_str):
    try:
        device_history = json.loads(str(json_str).replace("'",'"'))['device_history']
        if len(device_history) == 1: 
            if '4' in device_history[0].keys():
                ret = device_history[0]['4']
            else:
                ret = 'key_null'
        else:
            ret = 'field_null'
        return ret
    except ValueError:
        return None
