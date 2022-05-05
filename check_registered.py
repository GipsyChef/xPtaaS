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
    AttributesToGet=['ip'],
    KeyConditions=key_condition
)
print('IPs Count: {}'.format(res['Count']))

list_of_ips = []
for i in range(0, res['Count']):
    # print(res['Items'][i]['ip']['S'])
    list_of_ips.append(res['Items'][i]['ip']['S'])

print(list_of_ips)
