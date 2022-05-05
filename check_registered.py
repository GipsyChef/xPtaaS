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


def get_count():
    res = client.query(
        TableName=table_name,
        IndexName=index_name,
        Select='COUNT',
        KeyConditions=key_condition
    )
    return(res['Count'])


count = get_count()
print(count)


res = client.query(
    TableName=table_name,
    IndexName=index_name,
    Select='SPECIFIC_ATTRIBUTES',
    AttributesToGet=['ip'],
    KeyConditions=key_condition
)

print(res)
