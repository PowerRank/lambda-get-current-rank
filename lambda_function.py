import boto3
import json
import os
from dynamodb_json import json_util
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
client = boto3.client('dynamodb')

def lambda_handler(event, context):
    print('About enter try catch...')
    print(event)
    try:
        if event['input']['path'] == '/team_rankings':
            print('In team ranking path...')
            batch_keys = {
                os.environ['TABLE_NAME']: {
                    'Keys': [{'PK':'Current', 'SK': ('Team#'+teamId)} for teamId in event['queryStringParameters']['team_ids']],
                    'ProjectionExpression':'TeamId,#n,Points,#r',
                    'ExpressionAttributeNames':{'#n': 'Name', '#r':'Rank'}
                }
            }
            print('Doing Batch get...')
            response = dynamodb.batch_get_item(RequestItems=batch_keys)
            print('Returning...')
            return {
                'statusCode': 200, 
                'body':json.dumps(json_util.loads(response['Responses'][os.environ['TABLE_NAME']]))
            }
        else:
            print('In global ranking path...')
            paginator = client.get_paginator('query')
            try:
                paginatorConditions ={
                    'TableName':os.environ['TABLE_NAME'],
                    'IndexName': os.environ['POINTS_LSI_NAME'],
                    'ScanIndexForward':False,
                    'ProjectionExpression':'TeamId,#n,Points,#r',
                    'KeyConditionExpression':'PK = :PK',
                    'ExpressionAttributeValues':{
                        ':PK': {'S': 'Current'},
                        '#n':'Name',
                        '#r':'Rank'
                    },
                }
                if event['queryStringParameters']['next_token']:
                    print('There is a next token...')
                    response = paginator.paginate(
                        paginatorConditions,
                        PaginationConfig={
                            'MaxItems': event['queryStringParameters']['number_of_teams'],
                            'PageSize': event['queryStringParameters']['number_of_teams'],
                            'StartingToken': event['queryStringParameters']['next_token']
                        }
                    ).build_full_result()
                    print('Getting is results...')
                    result = {
                        'Items':response['Items'],
                        'NextToken':response['NextToken']
                    }
            except TypeError:
                print('There is no next token...')
                response = paginator.paginate(
                    paginatorConditions,
                    PaginationConfig={
                        'MaxItems': event['queryStringParameters']['number_of_teams'],
                        'PageSize': event['queryStringParameters']['number_of_teams']
                    }
                ).build_full_result()
                print('Getting is results...')
                result = {
                    'Items':response['Items'],
                    'NextToken':response['NextToken']
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