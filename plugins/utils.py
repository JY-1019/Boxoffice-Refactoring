import os 
import re
import json
import boto3

def get_API_key(setting):
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    secret_file = os.path.join(BASE_DIR, 'secrets.json')
    with open(secret_file) as f:
        secrets = json.loads(f.read())  
        try:
            return secrets[setting]
        except KeyError:
            error_msg = "Set the {} environment variable".format(setting)
            raise Exception(error_msg)


def upload_file_s3(bucket,filename, content):
    access_key_id = get_API_key("AWS_ACCESSKEY_ID")
    secret_access_key = get_API_key("AWS_SECRET_ACCESS_KEY")
    default_region = get_API_key("AWS_DEFAULT_REGION")
    
    client = boto3.client('s3', aws_access_key_id = access_key_id, aws_secret_access_key = secret_access_key, region_name = default_region)

    try :
        client.put_object(Bucket=bucket, Key=filename, Body=content)
        return True
    except :
        return False
    

