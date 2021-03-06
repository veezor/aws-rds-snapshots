import boto3
from datetime import datetime
import time
import os
import logging
import re

_LOGLEVEL = os.getenv('LOG_LEVEL', 'ERROR').strip()
_TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M'
TAGS_CREATED_BY = [
    {
        'Key': 'CreatedBy',
        'Value': 'DBSSR'
    }
]
TAGS_COPIED = [
    {
        'Key': 'DBSSR',
        'Value': 'copied'
    }
]
TAGS_SHARED = [
    {
        'Key': 'DBSSR',
        'Value': 'shared'
    }
]



logger = logging.getLogger()
logger.setLevel(_LOGLEVEL.upper())

def paginate_api_call(client, api_call, objecttype, *args, **kwargs):
    response = {}
    response[objecttype] = []
    paginator = client.get_paginator(api_call)
    page_iterator = paginator.paginate(**kwargs)
    for page in page_iterator:
        for item in page[objecttype]:
            response[objecttype].append(item)
    
    return response

def find_tag(collection, key, value=''):
    for tag in collection:
        if tag['Key'] == key and (value == '' or tag['Value'] == value):
            return True
    return False

def get_tag(collection, key):
    for tag in collection:
        if tag['Key'] == key:
            return tag['Value']
    return False

def get_vpc_security_groups(collection):
    results = []
    for subnet_group in collection:
        if subnet_group['Status'] == 'active':
            results.append(subnet_group['VpcSecurityGroupId'])
    
    return results

def get_timestamp(snapshot_identifier, snapshot_list):
    pattern = '%s-(.+)' % snapshot_list[snapshot_identifier]['DBClusterIdentifier']
    date_time = re.search(pattern, snapshot_identifier)

    if date_time is not None:
        try:
            return datetime.strptime(date_time.group(1), _TIMESTAMP_FORMAT)
        except Exception:
            return None
    
    return None