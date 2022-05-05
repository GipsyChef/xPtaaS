#!/usr/bin/env python3

import boto3


session = boto3.session.Session(
    profile_name='default',
    region_name='us-east-1'
)


client = session.client('dynamodb')

table_name = 'xptass'
cluster_id = 'test-cluster-id'
index_name = 'cluster_id-index'

key_condition = {
    'cluster_id': {
        'AttributeValueList': [
            {
                'S': cluster_id
            }
        ],

        'ComparisonOperator': 'EQ'
    }
}


res = client.query(
    TableName=table_name,
    IndexName=index_name,
    Select='SPECIFIC_ATTRIBUTES',
    AttributesToGet=['task_id'],
    KeyConditions=key_condition
)


for i in range(0, res['Count']):
    del_res = client.delete_item(
        TableName=table_name,
        Key={
            'task_id': {'S': res['Items'][i]['task_id']['S']}
        }
    )
    print(del_res)
