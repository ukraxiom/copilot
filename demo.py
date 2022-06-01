import boto3
import json
import os
import time
import pandas as pd

s3 = boto3.resource('s3')
s3.create_bucket(Bucket='my-bucket')
bucket = s3.Bucket('my-bucket')
#list all objects in bucket
for obj in bucket.objects.all():
    print(obj.key)

#list objects under test folder in bucket
for obj in bucket.objects.filter(Prefix='test/'):
    print(obj.key)

#delete all objects in test folder
for obj in bucket.objects.filter(Prefix='test/'):
    obj.delete()

#copy all objects from test to processed folder
for obj in bucket.objects.filter(Prefix='test/'):
    bucket.copy(obj.key, 'processed/' + obj.key)

#read all objects in test folder and convert to json
for obj in bucket.objects.filter(Prefix='test/'):
    obj_string = obj.get()['Body'].read().decode('utf-8')
    obj_json = json.loads(obj_string)
    print(obj_json)

#read all objects in test folder and convert to json and write in processed folder
for obj in bucket.objects.filter(Prefix='test/'):
    obj_string = obj.get()['Body'].read().decode('utf-8')
    obj_json = json.loads(obj_string)
    bucket.put_object(Key='processed/' + obj.key, Body=json.dumps(obj_json))


