import os
from datetime import datetime, timedelta, tzinfo, timezone
from re import I
from utils import *
import yaml
from botocore.exceptions import ClientError



LOGLEVEL = os.getenv('LOG_LEVEL', 'ERROR').strip()
SOURCE_REGION = os.getenv('SOURCE_AWS_REGION', os.getenv('AWS_DEFAULT_REGION', 'us-east-1')).strip()
TARGET_REGION = os.getenv('TARGET_AWS_REGION', os.getenv('AWS_DEFAULT_REGION', 'us-east-1')).strip()
BACKUP_INTERVAL = int(os.getenv('BACKUP_INTERVAL', '24'))
DATABASE_NAME_PATTERN = os.getenv('DATABASE_NAME_PATTERN', 'TAG').strip()
SUPPORTED_ENGINES = [ 'aurora', 'aurora-mysql', 'aurora-postgresql', 'postgres', 'mysql' ]
KMS_KEY = os.getenv('AWS_TARGET_KMS_KEY', 'None').strip()
SOURCE_ACCOUNT = os.getenv('AWS_SOURCE_ACCOUNT', '000000000000').strip()
TARGET_ACCOUNT = os.getenv('AWS_TARGET_ACCOUNT', '000000000000').strip()
DEBUG_DATABASE = os.getenv('DEBUG_DATABASE', '').strip()
SOURCE_KMS_KEY = f"arn:aws:kms:{SOURCE_REGION}:{SOURCE_ACCOUNT}:key/{KMS_KEY}"
TARGET_KMS_KEY = f"arn:aws:kms:{TARGET_REGION}:{SOURCE_ACCOUNT}:key/{KMS_KEY}"
SNAPSHOT_OLD_IN_DAYS = int(os.getenv('SNAPSHOT_OLD_IN_DAYS', '30'))

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
    client_target = boto3.client('rds', region_name=TARGET_REGION)
    instances = paginate_api_call(client, 'describe_db_instances', 'DBInstances')
    clusters = paginate_api_call(client, 'describe_db_clusters', 'DBClusters')
    now = datetime.now()
    now_str = now.strftime("-%Y-%m-%d-%H-%M")
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
    
    process_snapshots(available_snapshots, database_names, client, client_target)
    create_snapshots(database_names, client)
    
    logger.info("Cleaning up old snapshots on source region: %s", SOURCE_REGION)
    filtered_source_old_snapshots = filter_old_snapshots(client, instance_snapshots)
    filtered_source_old_cluster_snapshots = filter_old_cluster_snapshots(client, cluster_snapshots)
    source_old_snapshots = { **filtered_source_old_snapshots, **filtered_source_old_cluster_snapshots }
    for snapshot in source_old_snapshots:
        delete_snapshot(client, snapshot)

    logger.info("Cleaning up old snapshots on target region: %s", TARGET_REGION)
    filtered_target_old_snapshots = filter_old_snapshots(client_target, instance_snapshots)
    filtered_target_old_cluster_snapshots = filter_old_cluster_snapshots(client_target, cluster_snapshots)
    target_old_snapshots = { **filtered_target_old_snapshots, **filtered_target_old_cluster_snapshots }
    for snapshot in target_old_snapshots:
        delete_snapshot(client_target, snapshot)

    then = datetime.now()    
    logger.info("Finished in %ss", (then - now).seconds)

def create_snapshots(databases, client):
    now = datetime.now()
    now_str = now.strftime("-%Y-%m-%d-%H-%M")
    for database in databases:
        if databases[database]['snapshots'] == 0:
            logger.info("Creating snapshot for database %s", database)
            target_snapshot = database + now_str + '-DBSSR'
            if databases[database]['type'] == 'cluster':
                client.create_db_cluster_snapshot(DBClusterIdentifier=database, DBClusterSnapshotIdentifier=target_snapshot, Tags=TAGS_CREATED_BY)
            else:
                client.create_db_snapshot(DBInstanceIdentifier=database, DBSnapshotIdentifier=target_snapshot, Tags=TAGS_CREATED_BY)

def process_snapshots(snapshots, databases, client, client_target):
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
            target_region_snapshot_arn=f"arn:aws:rds:{TARGET_REGION}:{SOURCE_ACCOUNT}:snapshot:{snapshot['name'].split(':')[1]}-dbssr"
            target_region_cluster_snapshot_arn=f"arn:aws:rds:{TARGET_REGION}:{SOURCE_ACCOUNT}:cluster-snapshot:{snapshot['name'].split(':')[1]}-dbssr"
            if SOURCE_REGION == TARGET_REGION:
                if snapshot['type'] == 'cluster':
                    client.copy_db_cluster_snapshot(SourceDBClusterSnapshotIdentifier=snapshot['name'], TargetDBClusterSnapshotIdentifier=target_snapshot, KmsKeyId=SOURCE_KMS_KEY, Tags=TAGS_CREATED_BY)
                else:
                    client.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshot['name'], TargetDBSnapshotIdentifier=target_snapshot, KmsKeyId=SOURCE_KMS_KEY, Tags=TAGS_CREATED_BY)
                client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_COPIED)
                continue
            else:
                if snapshot['type'] == 'cluster':
                    client.copy_db_cluster_snapshot(SourceDBClusterSnapshotIdentifier=snapshot['name'], TargetDBClusterSnapshotIdentifier=target_snapshot, KmsKeyId=SOURCE_KMS_KEY, Tags=TAGS_CREATED_BY)
                    client_target.copy_db_cluster_snapshot(SourceDBClusterSnapshotIdentifier=snapshot['arn'], TargetDBClusterSnapshotIdentifier=target_snapshot, KmsKeyId=TARGET_KMS_KEY, Tags=TAGS_CREATED_BY)
                    client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_COPIED)
                    client_target.add_tags_to_resource(ResourceName=target_region_cluster_snapshot_arn, Tags=TAGS_COPIED)
                else:
                    client.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshot['name'], TargetDBSnapshotIdentifier=target_snapshot, KmsKeyId=SOURCE_KMS_KEY, Tags=TAGS_CREATED_BY)
                    client_target.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshot['arn'], TargetDBSnapshotIdentifier=target_snapshot, KmsKeyId=TARGET_KMS_KEY, Tags=TAGS_CREATED_BY)
                    client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_COPIED)
                    client_target.add_tags_to_resource(ResourceName=target_region_snapshot_arn, Tags=TAGS_COPIED)
                continue
        
        if snapshot['action'] == 'copy_no_key':
            logger.info("Copying snapshot %s without kms key", snapshot['name'])
            now = datetime.now()
            now_str = now.strftime("-%Y-%m-%d-%H-%M")
            target_snapshot = snapshot['id'] + now_str + '-DBSSR'
            target_region_snapshot_arn=f"arn:aws:rds:{TARGET_REGION}:{SOURCE_ACCOUNT}:snapshot:{snapshot['name']}"
            target_region_cluster_snapshot_arn=f"arn:aws:rds:{TARGET_REGION}:{SOURCE_ACCOUNT}:cluster-snapshot:{snapshot['name']}"
            if SOURCE_REGION != TARGET_REGION:
                if snapshot['type'] == 'cluster':
                    client_target.copy_db_cluster_snapshot(SourceDBClusterSnapshotIdentifier=snapshot['arn'], TargetDBClusterSnapshotIdentifier=target_snapshot, Tags=TAGS_CREATED_BY)
                    client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_COPIED)
                    client_target.add_tags_to_resource(ResourceName=target_region_cluster_snapshot_arn, Tags=TAGS_COPIED)
                else:
                    client_target.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshot['arn'], TargetDBSnapshotIdentifier=target_snapshot, Tags=TAGS_CREATED_BY)
                    client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_COPIED)
                    client_target.add_tags_to_resource(ResourceName=target_region_snapshot_arn, Tags=TAGS_COPIED)
                continue

        if snapshot['action'] == 'copy_kms':
            logger.info("Copying snapshot %s to change kms key", snapshot['name'])
            now = datetime.now()
            now_str = now.strftime("-%Y-%m-%d-%H-%M")
            target_snapshot = snapshot['id'] + now_str + '-DBSSR'
            if snapshot['type'] == 'cluster':
                client.copy_db_cluster_snapshot(SourceDBClusterSnapshotIdentifier=snapshot['name'], TargetDBClusterSnapshotIdentifier=target_snapshot, KmsKeyId=SOURCE_KMS_KEY, Tags=TAGS_CREATED_BY)
            else:
                client.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshot['name'], TargetDBSnapshotIdentifier=target_snapshot, KmsKeyId=SOURCE_KMS_KEY, Tags=TAGS_CREATED_BY)
            client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_COPIED_KMS)
            continue

        if snapshot['action'] == 'share':
            logger.info("Sharing snapshot %s", snapshot['name'])
            target_snapshot=snapshot['name']
            target_region_snapshot_arn=f"arn:aws:rds:{TARGET_REGION}:{SOURCE_ACCOUNT}:snapshot:{snapshot['name']}"
            target_region_cluster_snapshot_arn=f"arn:aws:rds:{TARGET_REGION}:{SOURCE_ACCOUNT}:cluster-snapshot:{snapshot['name']}"
            if SOURCE_REGION == TARGET_REGION:
                if snapshot['type'] == 'cluster':
                    client.modify_db_cluster_snapshot_attribute(DBClusterSnapshotIdentifier=snapshot['name'], AttributeName='restore', ValuesToAdd=[TARGET_ACCOUNT])
                else:
                    client.modify_db_snapshot_attribute(DBSnapshotIdentifier=snapshot['name'], AttributeName='restore', ValuesToAdd=[TARGET_ACCOUNT])
                client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_SHARED)
                continue
            else:
                try:
                    if snapshot['type'] == 'cluster':
                        client_target.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=target_snapshot)
                        client_target.modify_db_cluster_snapshot_attribute(DBClusterSnapshotIdentifier=target_snapshot, AttributeName='restore', ValuesToAdd=[TARGET_ACCOUNT])
                        client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_SHARED)
                        client_target.add_tags_to_resource(ResourceName=target_region_cluster_snapshot_arn, Tags=TAGS_SHARED)
                    else:
                        client_target.describe_db_snapshots(DBSnapshotIdentifier=target_snapshot)
                        client_target.modify_db_snapshot_attribute(DBSnapshotIdentifier=target_snapshot, AttributeName='restore', ValuesToAdd=[TARGET_ACCOUNT])
                        client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_SHARED)
                        client_target.add_tags_to_resource(ResourceName=target_region_snapshot_arn, Tags=TAGS_SHARED)
                    continue
                except ClientError as e:
                    if e.response['Error']['Code'] == 'DBSnapshotNotFound' or e.response['Error']['Code'] == 'DBClusterSnapshotNotFoundFault':
                        logger.info("Snapshot does not exist in the target region: %s (%s)", target_snapshot, e.response['Error']['Code'])
                        try:
                            if snapshot['type'] == 'cluster':
                                client_target.copy_db_cluster_snapshot(SourceDBClusterSnapshotIdentifier=snapshot['arn'], TargetDBClusterSnapshotIdentifier=target_snapshot, KmsKeyId=TARGET_KMS_KEY, Tags=TAGS_CREATED_BY)
                            else:
                                client_target.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshot['arn'], TargetDBSnapshotIdentifier=target_snapshot, KmsKeyId=TARGET_KMS_KEY, Tags=TAGS_CREATED_BY)    
                            continue
                        except ClientError as ein:
                            if ein.response['Error']['Code'] == 'DBSnapshotAlreadyExistsFault' or ein.response['Error']['Code'] == 'DBClusterSnapshotAlreadyExistsFault':
                                logger.error("Skipping share: %s (%s)", target_snapshot, ein.response['Error']['Code'])
                                continue
                            else:
                                logger.error("Error trying to copy snapshot to share: %s (%s)", target_snapshot, ein.response['Error']['Code'])
                                continue
                    else:
                        logger.error("Error checking snapshot to share: %s (%s)", target_snapshot, e.response['Error']['Code'])
                        continue
                continue
        if snapshot['action'] == 'share_no_key':
            logger.info("Sharing snapshot %s with out kms key", snapshot['name'])
            target_snapshot=snapshot['name']
            target_region_snapshot_arn=f"arn:aws:rds:{TARGET_REGION}:{SOURCE_ACCOUNT}:snapshot:{snapshot['name']}"
            target_region_cluster_snapshot_arn=f"arn:aws:rds:{TARGET_REGION}:{SOURCE_ACCOUNT}:cluster-snapshot:{snapshot['name']}"
            if SOURCE_REGION == TARGET_REGION:
                if snapshot['type'] == 'cluster':
                    client.modify_db_cluster_snapshot_attribute(DBClusterSnapshotIdentifier=snapshot['name'], AttributeName='restore', ValuesToAdd=[TARGET_ACCOUNT])
                else:
                    client.modify_db_snapshot_attribute(DBSnapshotIdentifier=snapshot['name'], AttributeName='restore', ValuesToAdd=[TARGET_ACCOUNT])
                client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_SHARED)
                continue
            else:
                try:
                    if snapshot['type'] == 'cluster':
                        client_target.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=target_snapshot)
                        client_target.modify_db_cluster_snapshot_attribute(DBClusterSnapshotIdentifier=target_snapshot, AttributeName='restore', ValuesToAdd=[TARGET_ACCOUNT])
                        client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_SHARED)
                        client_target.add_tags_to_resource(ResourceName=target_region_cluster_snapshot_arn, Tags=TAGS_SHARED)
                    else:
                        client_target.describe_db_snapshots(DBSnapshotIdentifier=target_snapshot)
                        client_target.modify_db_snapshot_attribute(DBSnapshotIdentifier=target_snapshot, AttributeName='restore', ValuesToAdd=[TARGET_ACCOUNT])
                        client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_SHARED)
                        client_target.add_tags_to_resource(ResourceName=target_region_snapshot_arn, Tags=TAGS_SHARED)
                    continue
                except ClientError as e:
                    if e.response['Error']['Code'] == 'DBSnapshotNotFound' or e.response['Error']['Code'] == 'DBClusterSnapshotNotFoundFault':
                        logger.info("Snapshot does not exist in the target region: %s (%s)", target_snapshot, e.response['Error']['Code'])
                        try:
                            if snapshot['type'] == 'cluster':
                                client_target.copy_db_cluster_snapshot(SourceDBClusterSnapshotIdentifier=snapshot['arn'], TargetDBClusterSnapshotIdentifier=target_snapshot, Tags=TAGS_CREATED_BY)
                            else:
                                client_target.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshot['arn'], TargetDBSnapshotIdentifier=target_snapshot, Tags=TAGS_CREATED_BY)    
                            client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_COPIED)
                            continue
                        except ClientError as ein:
                            if ein.response['Error']['Code'] == 'DBSnapshotAlreadyExistsFault' or ein.response['Error']['Code'] == 'DBClusterSnapshotAlreadyExistsFault':
                                logger.error("Skipping share no key: %s (%s)", target_snapshot, ein.response['Error']['Code'])
                                continue
                            else:
                                logger.error("Error trying to copy snapshot to share wo key: %s (%s)", target_snapshot, ein.response['Error']['Code'])
                                continue
                    else:
                        logger.error("Error checking snapshot to share wo key: %s (%s)", target_snapshot, e.response['Error']['Code'])
                        continue
                continue

        if snapshot['action'] == 'delete':
            logger.info("Deleting snapshot %s", snapshot['name'])
            target_snapshot=snapshot['name']
            if find_tag(snapshot['TagList'], 'CreatedBy', 'DBSSR'):
                if SOURCE_REGION == TARGET_REGION:
                    if snapshot['type'] == 'cluster':
                        client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snapshot['name'])
                    else:
                        client.delete_db_snapshot(DBSnapshotIdentifier=snapshot['name'])
                    continue
                else:
                    if snapshot['type'] == 'cluster':
                        client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snapshot['name'])
                        try:
                            client_target.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=target_snapshot)
                        except ClientError as e1:
                            if e1.response['Error']['Code'] == 'DBClusterSnapshotNotFoundFault':
                                logger.info("Cluster snapshot does not exist in the target region: %s (%s)", target_snapshot, e1.response['Error']['Code'])
                            else:
                                logger.error("Error checking cluster snapshot to delete: %s (%s)", target_snapshot, e1.response['Error']['Code'])
                                continue        
                    else:
                        client.delete_db_snapshot(DBSnapshotIdentifier=snapshot['name'])
                        try:    
                            client_target.delete_db_snapshot(DBSnapshotIdentifier=target_snapshot)
                        except ClientError as e2:
                            if e2.response['Error']['Code'] == 'DBSnapshotNotFound':
                                logger.info("Snapshot does not exist in the target region: %s (%s)", target_snapshot, e2.response['Error']['Code'])
                            else:
                                logger.error("Error checking snapshot to delete: %s (%s)", target_snapshot, e2.response['Error']['Code'])
                                continue  
                    continue
            snapshot['action'] = 'unshare'
            logger.info("Did not delete snapshot %s as it wasn't created by DBSSR!", snapshot['name'])

        if snapshot['action'] == 'unshare':
            logger.info("Unsharing snapshot %s", snapshot['name'])
            target_snapshot=snapshot['name'] + '-DBSSR'
            target_region_snapshot_arn=f"arn:aws:rds:{TARGET_REGION}:{SOURCE_ACCOUNT}:snapshot:{snapshot['name']}"
            target_region_cluster_snapshot_arn=f"arn:aws:rds:{TARGET_REGION}:{SOURCE_ACCOUNT}:cluster-snapshot:{snapshot['name']}"
            if SOURCE_REGION == TARGET_REGION:
                if snapshot['type'] == 'cluster':
                    client.modify_db_cluster_snapshot_attribute(DBClusterSnapshotIdentifier=snapshot['name'], AttributeName='restore', ValuesToRemove=[TARGET_ACCOUNT])
                else:
                    client.modify_db_snapshot_attribute(DBSnapshotIdentifier=snapshot['name'], AttributeName='restore', ValuesToRemove=[TARGET_ACCOUNT])
                client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_SHARED)
                continue
            else:
                if snapshot['type'] == 'cluster':
                    client_target.modify_db_cluster_snapshot_attribute(DBClusterSnapshotIdentifier=target_snapshot, AttributeName='restore', ValuesToRemove=[TARGET_ACCOUNT])
                    client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_SHARED)
                    client_target.add_tags_to_resource(ResourceName=target_region_cluster_snapshot_arn, Tags=TAGS_SHARED)
                else:
                    client_target.modify_db_snapshot_attribute(DBSnapshotIdentifier=target_snapshot, AttributeName='restore', ValuesToRemove=[TARGET_ACCOUNT])
                    client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_SHARED)
                    client_target.add_tags_to_resource(ResourceName=target_region_snapshot_arn, Tags=TAGS_SHARED)
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

        if snapshot['SnapshotType'] == 'shared' :
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
                try:
                    if results[snapshot[identifier]]['KmsKeyId'] != SOURCE_KMS_KEY:
                        if find_tag(results[snapshot[identifier]]['TagList'], 'DBSSR', 'copied_kms'):
                            results[snapshot[identifier]]['action'] = 'delete'
                        else:
                            results[snapshot[identifier]]['action'] = 'copy_kms'
                        if debugger: logger.info('Entered J1')
                    else:    
                        results[snapshot[identifier]]['action'] = 'share'
                        if debugger: logger.info('Entered J2')
                except KeyError as e:
                    if str(e) == "'KmsKeyId'":
                        if find_tag(results[snapshot[identifier]]['TagList'], 'DBSSR', 'copied'):
                            results[snapshot[identifier]]['action'] = 'share_no_key'
                        else:
                            results[snapshot[identifier]]['action'] = 'copy_no_key'
                        if debugger: logger.info('Entered J3')
                    else:
                        logger.error("Error setting snapshot type: %s, %s", e.response['Error']['Code'])
                        continue
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

        if debugger: logger.info('Entered Q')
    return results

def filter_old_snapshots(client, snapshots):
    response_client = snapshots
    results = {}
    for snapshot in response_client['DBSnapshots']:
        if find_tag(snapshot['TagList'], 'CreatedBy', 'DBSSR') and find_tag(snapshot['TagList'], 'DBSSR', 'shared'):
            if snapshot['SnapshotCreateTime'] < datetime.now(timezone.utc) - timedelta(days=SNAPSHOT_OLD_IN_DAYS):
                results[snapshot['DBSnapshotIdentifier']] = snapshot
    return results

def filter_old_cluster_snapshots(client, snapshots):
    response_client = snapshots
    results = {}
    for snapshot in response_client['DBClusterSnapshots']:
        if find_tag(snapshot['TagList'], 'CreatedBy', 'DBSSR') and find_tag(snapshot['TagList'], 'DBSSR', 'shared'):
            if snapshot['SnapshotCreateTime'] < datetime.now(timezone.utc) - timedelta(days=SNAPSHOT_OLD_IN_DAYS):
                results[snapshot['DBClusterSnapshotIdentifier']] = snapshot
    return results

def delete_snapshot(client, snapshot):
    logger.info('Deleting old snapshot: %s', snapshot)
    try:
        client.delete_db_snapshot(DBSnapshotIdentifier=snapshot)
    except ClientError as e:
        if e.response['Error']['Code'] == 'DBSnapshotNotFound':
            try:
                client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snapshot)
            except ClientError as ein:
                if ein.response['Error']['Code'] == 'DBClusterSnapshotNotFoundFault':
                    logger.info("Snapshot does not exist: %s (%s)", snapshot, ein.response['Error']['Code'])
                else:
                    logger.info("Snapshot state not valid: %s (%s)", snapshot, ein.response['Error']['Code'])
        else:
            logger.info("Snapshot state not valid: %s (%s)", snapshot, e.response['Error']['Code'])
