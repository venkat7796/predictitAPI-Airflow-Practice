import json
import requests
import boto3
import datetime
from io import BytesIO
import configparser
import logging

def read_config(file_path=".config"):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

def extract_data():
    logging.info("Extraction Started")
    config = read_config()
    aws_access_key = str(config.get('AWS','AWS_ACCESS_KEY_ID'))
    aws_secret_key = str(config.get('AWS','AWS_SECRET_ACCESS_KEY'))
    aws_region = str(config.get('AWS','AWS_REGION'))
    aws_bucket = str(config.get('AWS','BUCKET_NAME'))
    response = requests.request("GET","https://www.predictit.org/api/marketdata/all/")
    json_data = response.json()

    json_bytes = json.dumps(json_data).encode('utf-8')

    json_stream = BytesIO(json_bytes)

    current_datetime = datetime.datetime.now()
    formatted_time = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    object_key= str(formatted_time) + "_predictit.json"
    
    s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key,
                  aws_secret_access_key=aws_secret_key,
                  region_name=aws_region)
    
    s3.put_object(Body=json_stream,Bucket=aws_bucket,Key=object_key)

    print("Extraction Done")
