import os
from datetime import datetime, timedelta, tzinfo
from re import I
from utils import *
import yaml

LOGLEVEL = os.getenv('LOG_LEVEL', 'ERROR').strip()
SOURCE_REGION = os.getenv('SOURCE_AWS_REGION', os.getenv('AWS_DEFAULT_REGION', 'us-east-1')).strip()
BACKUP_INTERVAL = int(os.getenv('BACKUP_INTERVAL', '24'))
DATABASE_NAME_PATTERN = os.getenv('DATABASE_NAME_PATTERN', 'TAG').strip()
SUPPORTED_ENGINES = [ 'aurora', 'aurora-mysql', 'aurora-postgresql', 'postgres', 'mysql' ]
TARGET_KMS_KEY = os.getenv('AWS_TARGET_KMS_KEY', 'None').strip()
TARGET_ACCOUNT = os.getenv('AWS_TARGET_ACCOUNT', '000000000000').strip()
DEBUG_DATABASE = os.getenv('DEBUG_DATABASE', '').strip()

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())

    # SOURCE
    # 1. retrieve all instances and clusters that match pattern
    # 2. retrieve most recent snapshots within interval that match pattern
    # 3. in case instance/cluster has an available snapshot
    #       if it is tagged 'copied' or 'shared', ignore it
    #       if it is still in progress, ignore it
    #       if it is tagged 'disposable', delete it
    #       if it is of type automated and not tagged, copy it and tag it 'copied'
    #       if it is of type manual and not tagged, tag it 'shared' and share it
    # 4. in case instance/cluster has no available snapshot
    #       take a manual snapshot


def lambda_handler(event, context):
    client = boto3.client('rds', region_name=SOURCE_REGION)
    instances = paginate_api_call(client, 'describe_db_instances', 'DBInstances')
    clusters = paginate_api_call(client, 'describe_db_clusters', 'DBClusters')
    now = datetime.now()
    filtered_instances = filter_databases(DATABASE_NAME_PATTERN, instances)
    filtered_clusters = filter_databases(DATABASE_NAME_PATTERN, clusters)
    database_names = { **filtered_clusters, **filtered_instances }
    logger.info("Found %i database(s) matching %s", len(database_names), DATABASE_NAME_PATTERN)
    cluster_snapshots = paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots', IncludeShared=True)
    instance_snapshots = paginate_api_call(client, 'describe_db_snapshots', 'DBSnapshots', IncludeShared=True)
    filtered_instance_snapshots = filter_available_snapshots(DATABASE_NAME_PATTERN, cluster_snapshots, database_names, BACKUP_INTERVAL)
    filtered_cluster_snapshots = filter_available_snapshots(DATABASE_NAME_PATTERN, instance_snapshots, database_names, BACKUP_INTERVAL)
    available_snapshots = { **filtered_instance_snapshots, **filtered_cluster_snapshots }
    logger.info("Filtered %i snapshots", len(available_snapshots))
    for snapshot in available_snapshots.values():
        logger.info("Database Created: %s, Engine: %s, Type: %s, Status: %s, Name: %s, Action: %s", snapshot.get('SnapshotCreateTime', 'creating'), snapshot['Engine'], snapshot['SnapshotType'], snapshot['Status'], snapshot['id'], snapshot['action']) 
    
    process_snapshots(available_snapshots, database_names, client)
    create_snapshots(database_names, client)

    then = datetime.now()    
    logger.info("Finished in %ss", (then - now).seconds)

def create_snapshots(databases, client):
    for database in databases:
        if databases[database]['snapshots'] == 0:
            logger.info("Creating snapshot for database %s", database)
            target_snapshot = database + '-DBSSR'
            if databases[database]['type'] == 'cluster':
                client.create_db_cluster_snapshot(DBClusterIdentifier=database, DBClusterSnapshotIdentifier=target_snapshot, Tags=TAGS_CREATED_BY)
            else:
                client.create_db_snapshot(DBIdentifier=database, DBSnapshotIdentifier=target_snapshot, Tags=TAGS_CREATED_BY)

def process_snapshots(snapshots, databases, client):
    for snapshot in snapshots.values():
        if snapshot['action'] == 'tbd':
            logger.error("Bug Spotted! Snapshot without action: %s", yaml.dump(snapshot))
            continue

        if snapshot['action'] == 'skip':
            logger.info('Skipping snapshot %s', snapshot['name'])
            continue

        if snapshot['action'] == 'copy':
            logger.info("Copying snapshot %s", snapshot['name'])
            target_snapshot=snapshot['name'].split(':')[1] + '-DBSSR'
            if snapshot['type'] == 'cluster':
                client.copy_db_cluster_snapshot(SourceDBClusterSnapshotIdentifier=snapshot['name'], TargetDBClusterSnapshotIdentifier=target_snapshot, KmsKeyId=TARGET_KMS_KEY, Tags=TAGS_CREATED_BY)
            else:
                client.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshot['name'], TargetDBSnapshotIdentifier=target_snapshot, KmsKeyId=TARGET_KMS_KEY, Tags=TAGS_CREATED_BY)
            client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_COPIED)
            continue
        
        if snapshot['action'] == 'share':
            logger.info("Sharing snapshot %s", snapshot['name'])
            if snapshot['type'] == 'cluster':
                client.modify_db_cluster_snapshot_attribute(DBClusterSnapshotIdentifier=snapshot['name'], AttributeName='restore', ValuesToAdd=[TARGET_ACCOUNT])
            else:
                client.modify_db_snapshot_attribute(DBSnapshotIdentifier=snapshot['name'], AttributeName='restore', ValuesToAdd=[TARGET_ACCOUNT])
            client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_SHARED)
            continue

        if snapshot['action'] == 'delete':
            logger.info("Deleting snapshot %s", snapshot['name'])
            if find_tag(snapshot['TagList'], 'CreatedBy', 'DBSSR'):
                if snapshot['type'] == 'cluster':
                    client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snapshot['name'])
                else:
                    client.delete_db_snapshot(DBSnapshotIdentifier=snapshot['name'])
                continue
            snapshot['action'] = 'unshare'
            logger.info("Did not delete snapshot %s as it wasn't created by DBSSR!", snapshot['name'])

        if snapshot['action'] == 'unshare':
            logger.info("Unsharing snapshot %s", snapshot['name'])
            if snapshot['type'] == 'cluster':
                client.modify_db_cluster_snapshot_attribute(DBClusterSnapshotIdentifier=snapshot['name'], AttributeName='restore', ValuesToRemove=[TARGET_ACCOUNT])
            else:
                client.modify_db_snapshot_attribute(DBSnapshotIdentifier=snapshot['name'], AttributeName='restore', ValuesToRemove=[TARGET_ACCOUNT])
            client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_SHARED)
            continue


def filter_databases(pattern, response):
    results = {}
    databases = 'DBInstances'
    identifier = 'DBInstanceIdentifier'
    status = 'DBInstanceStatus'
    database_type = 'instance'
    arn = 'DBInstanceArn'
    if 'DBClusters' in response:
        databases = 'DBClusters'
        identifier = 'DBClusterIdentifier'
        status = 'Status'
        database_type = 'cluster'
        arn = 'DBClusterArn'
    
    for database in response[databases]:
        # Skip stopped databases
        if database[status] == 'stopped':
            continue

        # Skip cluster instances
        if identifier == 'DBInstanceIdentifier' and 'DBClusterIdentifier' in database:
            continue

        # Skip Read Only Replicas
        if 'ReadReplicaSourceDBInstanceIdentifier' in database:
            continue

        if (pattern == 'ALL' or (pattern == 'TAG' and find_tag(database['TagList'], 'DBSSRSource', 'true')) or re.search(pattern, database[identifier])) and database['Engine'] in SUPPORTED_ENGINES:
            results[database[identifier]] = { 'snapshots': 0, 'type': database_type, 'arn': database[arn] }

    return results

def filter_available_snapshots(pattern, response, databases, backup_interval=None):
    results = {}
    snapshots = 'DBSnapshots'
    identifier = 'DBInstanceIdentifier'
    snapshot_identifier = 'DBSnapshotIdentifier'
    snapshot_type = 'instance'
    arn = 'DBSnapshotArn'
    if 'DBClusterSnapshots' in response:
        snapshots = 'DBClusterSnapshots'
        identifier = 'DBClusterIdentifier'
        snapshot_identifier = 'DBClusterSnapshotIdentifier'
        snapshot_type = 'cluster'
        arn = 'DBClusterSnapshotArn'

    logger.info("Processing %i %s with %s pattern", len(response[snapshots]), snapshots, pattern)
    for snapshot in response[snapshots]:
        snapshot['id'] = snapshot[identifier]
        snapshot['name'] = snapshot[snapshot_identifier]
        snapshot['type'] = snapshot_type
        snapshot['arn'] = snapshot[arn]

        # Ignore AWS Backup snapshots
        if snapshot['SnapshotType'] == 'awsbackup':
            continue

        # Ignore unmatched snapshots
        if snapshot[identifier] not in databases:
            continue

        # Ignore as didn't match pattern or supported engine
        if ((pattern not in ['ALL', 'TAG'] and not re.search(pattern, snapshot[identifier])) or snapshot['Engine'] not in SUPPORTED_ENGINES):
            continue

        # Ignore automated ongoning snapshots
        if 'SnapshotCreateTime' not in snapshot and snapshot['SnapshotType'] == 'automated':
            continue

        # Skip snapshots out of backup interval
        if backup_interval and 'SnapshotCreateTime' in snapshot and snapshot['SnapshotCreateTime'].replace(tzinfo=None) < datetime.utcnow().replace(tzinfo=None) - timedelta(hours=backup_interval):
            continue

        debugger = False
        if snapshot[identifier] == DEBUG_DATABASE:
            debugger = True
            if snapshot[identifier] in results: logger.info('Current Snapshot: %s', results[snapshot[identifier]]['name'])
            logger.info('Snapshot Name: %s, Type: %s', snapshot['name'], snapshot['SnapshotType'])

        if snapshot[identifier] not in results:
            snapshot['action'] = 'tbd'
            results[snapshot[identifier]] = snapshot
            databases[snapshot[identifier]]['snapshots'] += 1
            if debugger: logger.info('Entered A')

        if snapshot['SnapshotType'] == 'shared':
            if snapshot['name'].split(':').pop().replace('-target','') == results[snapshot[identifier]]['name']:
                results[snapshot[identifier]]['SnapshotType'] = 'shared'
                results[snapshot[identifier]]['action'] = 'delete'
                if debugger: logger.info('Entered B1')
                continue
            snapshot['action'] = 'skip'
            results[snapshot[identifier]] = snapshot
            if debugger: logger.info('Entered B')
            continue

        if results[snapshot[identifier]]['SnapshotType'] == 'shared':
            if snapshot['name'] == results[snapshot[identifier]]['name'].split(':').pop().replace('-target',''):
                snapshot['SnapshotType'] = 'shared'
                snapshot['action'] = 'delete'
                results[snapshot[identifier]] = snapshot
                if debugger: logger.info('Entered C1')
                continue
            if results[snapshot[identifier]]['action'] == 'tbd':
                results[snapshot[identifier]]['action'] = 'skip'
            if debugger: logger.info('Entered C')
            continue

        if find_tag(results[snapshot[identifier]]['TagList'], 'DBSSR', 'shared'):
            if results[snapshot[identifier]]['action'] == 'tbd':
                results[snapshot[identifier]]['action'] = 'skip'
            if debugger: logger.info('Entered D')
            continue

        if find_tag(snapshot['TagList'], 'DBSSR', 'shared'):
            snapshot['action'] = 'skip'
            results[snapshot[identifier]] = snapshot
            if debugger: logger.info('Entered E')
            continue

        if 'SnapshotCreateTime' not in results[snapshot[identifier]] and results[snapshot[identifier]]['SnapshotType'] == 'manual':
            if results[snapshot[identifier]]['action'] == 'tbd':
                results[snapshot[identifier]]['action'] = 'skip'
            if debugger: logger.info('Entered F')
            continue

        if 'SnapshotCreateTime' not in snapshot and snapshot['SnapshotType'] == 'manual':
            snapshot['action'] = 'skip'
            results[snapshot[identifier]] = snapshot
            if debugger: logger.info('Entered G')
            continue

        if results[snapshot[identifier]]['Status'] in ['copying', 'creating']:
            if results[snapshot[identifier]]['action'] == 'tbd':
                results[snapshot[identifier]]['action'] = 'skip'
            if debugger: logger.info('Entered H')
            continue

        if snapshot['Status'] == 'copying':
            snapshot['action'] = 'skip'
            results[snapshot[identifier]] = snapshot
            if debugger: logger.info('Entered I')
            continue
        
        if results[snapshot[identifier]]['SnapshotType'] == 'manual':
            if results[snapshot[identifier]]['action'] == 'tbd':
                results[snapshot[identifier]]['action'] = 'share'
            if debugger: logger.info('Entered J')
            continue

        if snapshot['SnapshotType'] == 'manual' and results[snapshot[identifier]]['SnapshotType'] == 'automated':
            snapshot['action'] = 'share'
            results[snapshot[identifier]] = snapshot
            if debugger: logger.info('Entered K')
            continue

        if snapshot['SnapshotType'] == 'manual' and results[snapshot[identifier]]['SnapshotType'] == 'manual':
            snapshot['action'] == 'share'
            if snapshot['SnapshotCreateTime'].replace(tzinfo=None) >= results[snapshot[identifier]]['SnapshotCreateTime'].replace(tzinfo=None):
                results[snapshot[identifier]] = snapshot
            if debugger: logger.info('Entered L')
            continue

        if find_tag(results[snapshot[identifier]]['TagList'], 'DBSSR', 'copied'):
            if results[snapshot[identifier]]['action'] == 'tbd':
                results[snapshot[identifier]]['action'] = 'skip'
            if debugger: logger.info('Entered M')
            continue

        if find_tag(snapshot['TagList'], 'DBSSR', 'copied'):
            snapshot['action'] = 'skip'
            results[snapshot[identifier]] = snapshot
            if debugger: logger.info('Entered N')
            continue

        if snapshot['SnapshotType'] == 'automated' and results[snapshot[identifier]]['SnapshotType'] == 'automated':
            snapshot['action'] = 'copy'

            if snapshot['SnapshotCreateTime'].replace(tzinfo=None) >= results[snapshot[identifier]]['SnapshotCreateTime'].replace(tzinfo=None):
                results[snapshot[identifier]] = snapshot
            if debugger: logger.info('Entered O')
            continue

        if debugger: logger.info('Entered P')
    return results
