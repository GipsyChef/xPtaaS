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

# commented out as en example of count only query
"""
def get_count():
    # this can run to make sure all tasks are registered
    res = client.query(
        TableName=table_name,
        IndexName=index_name,
        Select='COUNT',
        KeyConditions=key_condition
    )
    return(res['Count'])


count = get_count()
print('------------------------------------------------------')
print('Example Count Query: {}'.format(count))
"""


def get_res(last_key=None):
    res = {}
    if last_key is None:
        res = client.query(
            TableName=table_name,
            IndexName=index_name,
            Select='SPECIFIC_ATTRIBUTES',
            AttributesToGet=['ip'],
            KeyConditions=key_condition
        )
    else:
        res = client.query(
            TableName=table_name,
            IndexName=index_name,
            Select='SPECIFIC_ATTRIBUTES',
            AttributesToGet=['ip'],
            ExclusiveStartKey=last_key,
            KeyConditions=key_condition
        )
    return(res)


list_of_ips = []
my_count = 0
cycles = 1
res = get_res()
for i in range(0, res['Count']):
    list_of_ips.append(res['Items'][i]['ip']['S'])
my_count = my_count + res['Count']
while 'LastEvaluatedKey' in res:
    res = get_res(res['LastEvaluatedKey'])
    my_count = my_count + res['Count']
    cycles = cycles + 1
    for i in range(0, res['Count']):
        list_of_ips.append(res['Items'][i]['ip']['S'])

# print results
print('IPs Count: {}'.format(my_count))
print('Query Cycles: {}'.format(cycles))
print(list_of_ips)
