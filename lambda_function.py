import boto3
import json
import os
from dynamodb_json import json_util
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
client = boto3.client('dynamodb')

def lambda_handler(event, context):
    try:
        if event['path'] == '/team_rankings':
            batch_keys = {
                os.environ['TABLE_NAME']: {
                    'Keys': [{'PK':'Current', 'SK': ('Team#'+teamId)} for teamId in event['queryStringParameters']['team_ids'].split(',')],
                    'ProjectionExpression':'TeamId,#n,Points,#r',
                    'ExpressionAttributeNames':{'#n': 'Name', '#r':'Rank'}
                }
            }
            response = dynamodb.batch_get_item(RequestItems=batch_keys)
            return {
                'statusCode': 200, 
                'body':json.dumps(json_util.loads(response['Responses'][os.environ['TABLE_NAME']]))
            }
        else:
            paginator = client.get_paginator('query')
            if 'next_token' in locals()['event']['queryStringParameters']:
                response = paginator.paginate(
                    TableName=os.environ['TABLE_NAME'],
                    IndexName= os.environ['POINTS_LSI_NAME'],
                    ScanIndexForward=False,
                    ProjectionExpression='TeamId,#n,Points,#r',
                    KeyConditionExpression='PK = :PK',
                    ExpressionAttributeValues={
                        ':PK': {'S': 'Current'}
                    },
                    ExpressionAttributeNames ={
                        '#n':'Name',
                        '#r':'Rank'
                    },
                    PaginationConfig={
                        'MaxItems': event['queryStringParameters']['number_of_teams'],
                        'PageSize': event['queryStringParameters']['number_of_teams'],
                        'StartingToken': event['queryStringParameters']['next_token']
                    }
                ).build_full_result()
            else:
                response = paginator.paginate(
                    TableName=os.environ['TABLE_NAME'],
                    IndexName= os.environ['POINTS_LSI_NAME'],
                    ScanIndexForward=False,
                    ProjectionExpression='TeamId,#n,Points,#r',
                    KeyConditionExpression='PK = :PK',
                    ExpressionAttributeValues={
                        ':PK': {'S': 'Current'}
                    },
                    ExpressionAttributeNames ={
                        '#n':'Name',
                        '#r':'Rank'
                    },
                    PaginationConfig={
                        'MaxItems': event['queryStringParameters']['number_of_teams'],
                        'PageSize': event['queryStringParameters']['number_of_teams']
                    }
                ).build_full_result()
            try:
                print(locals())
                if 'NextToken' in locals()['response']:
                    print('There is a next token')
                result = {
                    'Items':response['Items'],
                    'NextToken':response['NextToken']
                }
            except:
                result = {
                    'Items':response['Items']
                }
            return {
                'statusCode': 200, 
                'body':json.dumps(json_util.loads(result))
            }
    except Exception as e:
        print(f'Exception: {e}')
    return {
            'statusCode': 500,
            'body': json.dumps('Invalid Request')
        }