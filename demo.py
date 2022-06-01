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

#write object in processed folder to redshift table using psycopg2
import psycopg2
conn = psycopg2.connect(host='my-redshift-cluster.cluster-cjqjqjqjqjqjq.us-east-1.redshift.amazonaws.com',
                        port=5439,
                        user='my-user',
                        password='my-password',
                        dbname='my-db')
cur = conn.cursor()
cur.execute(""" COPY my_table FROM 's3://my-bucket/processed/test/' credentials 'aws_access_key_id=my-access-key;aws_secret_access_key=my-secret-key' DELIMITER ',' IGNOREHEADER 1; """)
conn.commit()

#read dynamodb table with filter 
import boto3
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('my-table')
response = table.scan(
    FilterExpression=Attr('my-attribute').eq('my-value')
)

#write item to dynamodb table 
import boto3
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('my-table')
table.put_item(
    Item={
        'my-attribute': 'my-value',
        'my-other-attribute': 'my-other-value'
    }
)

#send email using ses
import boto3
ses = boto3.client('ses')
ses.send_email(
    Source='my-email-address',
    Destination={
        'ToAddresses': [
            'my-email-address'
        ]
    },
    Message={
        'Subject': {
            'Data': 'my-subject'
        },
        'Body': {
            'Text': {
                'Data': 'my-message'
            }
        }
    }
)

#run query on athena
import boto3
athena = boto3.client('athena')
response = athena.start_query_execution(
    QueryString='SELECT * FROM my-table',
    QueryExecutionContext={
        'Database': 'my-database'
    },
    ResultConfiguration={
        'OutputLocation': 's3://my-bucket/my-query-results'
    }
)

#add description to bigquery table using google cloud platform
import google.cloud
from google.cloud import bigquery
client = bigquery.Client()
dataset_ref = client.dataset('my-dataset')
table_ref = dataset_ref.table('my-table')
table = client.get_table(table_ref)
table.description = 'my-description'
client.update_table(table, ['description'])





















