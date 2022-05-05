#!/usr/bin/env python3

import boto3
import uuid
import random


session = boto3.session.Session(
    profile_name='default',
    region_name='us-east-1'
)


client = session.client('dynamodb')
table_name = 'xptass'

# test vars
task_count = 2500
cluster_id = 'test-cluster-id'


def gen_ip():
    return '.'.join(str(random.randint(0, 255)) for _ in range(4))


for x in range(0, task_count):
    task_id = str(uuid.uuid4())
    ip = gen_ip()

    item = {
        'task_id': {
            'S': task_id
        },
        'cluster_id': {
            'S': cluster_id
        },
        'ip': {
            'S': ip
        }
    }
    res = client.put_item(
        TableName=table_name,
        Item=item
    )

    print(res)
