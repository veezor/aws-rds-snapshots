import os
from datetime import datetime, timedelta, tzinfo
from re import I
from utils import *
import yaml

LOGLEVEL = os.getenv('LOG_LEVEL', 'ERROR').strip()
TARGET_REGION = os.getenv('TARGET_AWS_REGION', os.getenv('AWS_DEFAULT_REGION', 'us-east-1')).strip()
BACKUP_INTERVAL = int(os.getenv('BACKUP_INTERVAL', '24'))
DATABASE_NAME_PATTERN = os.getenv('DATABASE_NAME_PATTERN', 'ALL').strip()
SUPPORTED_ENGINES = [ 'aurora', 'aurora-mysql', 'aurora-postgresql', 'postgres', 'mysql' ]
TARGET_KMS_KEY = os.getenv('AWS_TARGET_KMS_KEY', 'None').strip()
SOURCE_ACCOUNT = os.getenv('AWS_SOURCE_ACCOUNT', '000000000000').strip()
DEBUG_DATABASE = os.getenv('DEBUG_DATABASE', '').strip()

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())

    # TARGET
    # 1. retrieve all instances and clusters that match pattern
    # 2. retrieve most recent shared snapshots within interval that match pattern
    # 3. in case instance/cluster has an available snapshot
    #       if it is tagged 'shared', add tag 'restored' to it
    #          delete the corresponging instance/cluster
    #          restore the snapshot
    #       if it is tagged 'restored' and an instance is not yet available, ignore it
    #       if it is tagged 'restored' and instance creation date is greater than snapshot's, tag it 'disposable'


def lambda_handler(event, context):
    client = boto3.client('rds', region_name=TARGET_REGION)
    instances = paginate_api_call(client, 'describe_db_instances', 'DBInstances')
    clusters = paginate_api_call(client, 'describe_db_clusters', 'DBClusters')
    now = datetime.now()
    filtered_instances = filter_databases(DATABASE_NAME_PATTERN, instances)
    filtered_clusters = filter_databases(DATABASE_NAME_PATTERN, clusters)
    database_names = join_filtered_databases(filtered_clusters, filtered_instances)
    cluster_snapshots = paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots', IncludeShared=True)
    instance_snapshots = paginate_api_call(client, 'describe_db_snapshots', 'DBSnapshots', IncludeShared=True)
    filtered_instance_snapshots = filter_available_snapshots(DATABASE_NAME_PATTERN, cluster_snapshots, database_names, BACKUP_INTERVAL)
    filtered_cluster_snapshots = filter_available_snapshots(DATABASE_NAME_PATTERN, instance_snapshots, database_names, BACKUP_INTERVAL)
    available_snapshots = { **filtered_instance_snapshots, **filtered_cluster_snapshots }
    available_snapshots = define_actions(available_snapshots, database_names)
    logger.info("Found %i database(s) matching %s", len(database_names), DATABASE_NAME_PATTERN)
    logger.info("Filtered %i snapshots", len(available_snapshots))
    for snapshot in available_snapshots.values():
        logger.info("Database Created: %s, Engine: %s, Type: %s, Status: %s, Name: %s, Action: %s", snapshot.get('SnapshotCreateTime', 'creating'), snapshot['Engine'], snapshot['SnapshotType'], snapshot['Status'], snapshot['id'], snapshot['action']) 
    process_snapshots(available_snapshots, database_names, client)

    then = datetime.now()    
    logger.info("Finished in %ss", (then - now).seconds)

def join_filtered_databases(clusters, instances):
    databases = {}
    for database in clusters:
        databases[database] = clusters[database]
        if database in instances and 'cluster' in instances[database] and instances[database]['cluster'] == clusters[database]['identifier']:
            databases[database]['class'] = instances[database]['class']
    
    for database in instances:
        if database not in clusters:
            databases[database] = instances[database]
    
    return databases

def define_actions(snapshots, databases):
    for snapshot in snapshots.values():
        debugger = False
        if snapshot['id'] == DEBUG_DATABASE:
            debugger = True
            logger.info("Current: %s", snapshot['arn'])

        if snapshot['SnapshotType'] == 'shared':
            snapshot['action'] = 'copy'
            if debugger: logger.info('Entrou A')
            continue

        if snapshot['SnapshotType'] == 'manual' and snapshot['Status'] == 'copying':
            snapshot['action'] = 'skip'
            if debugger: logger.info('Entrou B')
            continue

        if snapshot['SnapshotType'] == 'manual' and not find_tag(snapshot['TagList'], 'DBSSR', 'shared'):
            if snapshot['Status'] == 'creating':
                snapshot['action'] = 'skip'
                continue
            snapshot['action'] = 'share'
            if debugger: logger.info('Entrou C')
            continue
        
        database = databases[snapshot['id']]
        if database['old'] == 'available':
            snapshot['action'] = 'delete_database'
            continue

        if database['status'] == 'available':
            if database['identifier'].endswith('-dbssr'):
                snapshot['action'] = 'restore'
                if debugger: logger.info('Entrou D')
                continue

            if snapshot['type'] == 'cluster' and database['mode'] != 'serverless' and 'class' not in database:
                snapshot['action'] = 'restore_cluster_instance'
                if debugger: logger.info('Entrou E')
                continue

            if database['create_time'] and datetime.strptime(database['create_time'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=None) > datetime.utcnow().replace(tzinfo=None) - timedelta(hours=BACKUP_INTERVAL):
                if database['snapshots'] == 2:
                    snapshot['action'] = 'skip'
                    if debugger: logger.info('Entrou F')
                    continue
                    
                snapshot['action'] = 'delete_snapshot'
                if debugger: logger.info('Entrou G')
                continue

            snapshot['action'] = 'rename'
            if debugger: logger.info('Entrou H')
            continue

        if database['status'] in ['renaming', 'creating']:
            snapshot['action'] = 'skip'
            if debugger: logger.info('Entrou J')
            continue

        snapshot['action'] = 'tbd'
        if debugger: logger.info('Entrou K')

    return snapshots

def process_snapshots(snapshots, databases, client):
    for snapshot in snapshots.values():
        if snapshot['action'] == 'tbd':
            logger.error("############## Bug Spotted! Snapshot without action: %s", yaml.dump(snapshot))
            continue

        if snapshot['action'] == 'skip':
            logger.info('Skipping snapshot %s', snapshot['name'])
            continue

        if snapshot['action'] == 'copy':
            logger.info("Copying snapshot %s", snapshot['name'])
            target_snapshot=snapshot['name'].split(':').pop() + '-target'
            if snapshot['type'] == 'cluster':
                client.copy_db_cluster_snapshot(SourceDBClusterSnapshotIdentifier=snapshot['name'], TargetDBClusterSnapshotIdentifier=target_snapshot, KmsKeyId=TARGET_KMS_KEY, Tags=TAGS_CREATED_BY)
            else:
                client.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshot['name'], TargetDBSnapshotIdentifier=target_snapshot, KmsKeyId=TARGET_KMS_KEY, Tags=TAGS_CREATED_BY)
            continue
        
        database = databases[snapshot['id']]
        if snapshot['action'] == 'rename':
            logger.info("Renaming current database %s", database['identifier'])
            new_database_identifier = database['identifier'] + '-dbssr'
            if snapshot['type'] == 'cluster':
                client.modify_db_cluster(DBClusterIdentifier=database['identifier'], NewDBClusterIdentifier=new_database_identifier, ApplyImmediately=True)
            else:
                client.modify_db_instance(DBInstanceIdentifier=database['identifier'], NewDBInstanceIdentifier=new_database_identifier, ApplyImmediately=True)
            continue

        if snapshot['action'] == 'share':
            logger.info("Sharing snapshot %s", snapshot['name'])
            if snapshot['type'] == 'cluster':
                client.modify_db_cluster_snapshot_attribute(DBClusterSnapshotIdentifier=snapshot['name'], AttributeName='restore', ValuesToAdd=[SOURCE_ACCOUNT])
            else:
                client.modify_db_snapshot_attribute(DBSnapshotIdentifier=snapshot['name'], AttributeName='restore', ValuesToAdd=[SOURCE_ACCOUNT])
            client.add_tags_to_resource(ResourceName=snapshot['arn'], Tags=TAGS_SHARED)
            continue

        if snapshot['action'] == 'restore_cluster_instance':
            logger.info("Provisioning cluster's instance %s", database['identifier'].replace('-cluster',''))
            instance_class = get_tag(database['tags'], 'DBSSRInstanceClass')
            tags = [
                {
                    'Key': 'DBSSR',
                    'Value': snapshot['id']
                }
            ]
            client.create_db_instance(DBInstanceIdentifier=database['identifier'].replace('-cluster',''), DBClusterIdentifier=database['identifier'], DBInstanceClass=instance_class, Engine=snapshot['Engine'], Tags=tags)
            continue

        if snapshot['action'] == 'restore':
            logger.info("Restoring snapshot %s as %s", snapshot['name'], database['identifier'])
            tags = [
                {
                    'Key': 'DBSSR',
                    'Value': snapshot['id']
                },
                {
                    'Key': 'DBSSRCreateTime',
                    'Value': datetime.utcnow().replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
                }
            ]
            if snapshot['type'] == 'cluster':
                if database['mode'] != 'serverless':
                    tags.append({'Key':'DBSSRInstanceClass','Value':database['class']})
                client.restore_db_cluster_from_snapshot(SnapshotIdentifier=snapshot['arn'], DBClusterIdentifier=database['identifier'].replace('-dbssr',''), Engine=database['engine'], Tags=tags, DBSubnetGroupName=database['subnet_group'], VpcSecurityGroupIds=database['vpc_security_groups'])
            else:
                client.restore_db_instance_from_db_snapshot(DBSnapshotIdentifier=snapshot['arn'], DBInstanceIdentifier=database['identifier'].replace('-dbssr',''), Engine=database['engine'], Tags=tags, DBInstanceClass=database['class'], DBSubnetGroupName=database['subnet_group'], VpcSecurityGroupIds=database['vpc_security_groups'])
            continue

        if snapshot['action'] == 'delete_database':
            database_name = database['identifier'] + '-dbssr'
            logger.info("Deleting old database %s", database_name)
            try:
                if snapshot['type'] == 'cluster':
                    if database['mode'] != 'serverless':
                        logger.info("deleting instance %s from cluster %s", database_name.replace('-cluster','').replace('-dbssr',''), database_name)
                        client.delete_db_instance(DBInstanceIdentifier=database_name.replace('-cluster','').replace('-dbssr',''), SkipFinalSnapshot=True)
                        time.sleep(5)
                    client.delete_db_cluster(DBClusterIdentifier=database_name, SkipFinalSnapshot=True)
                else:
                    logger.info("deleting standalone instance %s", database_name)
                    client.delete_db_instance(DBInstanceIdentifier=database_name, SkipFinalSnapshot=True)
            except Exception as e:
               logger.info("Instance probably already deleted: %s", e) 
            continue

        if snapshot['action'] == 'delete_snapshot':
            logger.info("Deleting snapshot %s", snapshot['name'])
            if find_tag(snapshot['TagList'], 'CreatedBy', 'DBSSR'):
                if snapshot['type'] == 'cluster':
                    client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snapshot['name'])
                else:
                    client.delete_db_snapshot(DBSnapshotIdentifier=snapshot['name'])
            else:
                logger.info("Did not delete snakpthot %s as it wasn't created by DBSSR!", snapshot['name'])
            continue

def filter_databases(pattern, response):
    results = {}
    databases = 'DBInstances'
    identifier = 'DBInstanceIdentifier'
    status = 'DBInstanceStatus'
    database_type = 'instance'
    arn = 'DBInstanceArn'
    create_time = 'InstanceCreateTime'
    if 'DBClusters' in response:
        databases = 'DBClusters'
        identifier = 'DBClusterIdentifier'
        status = 'Status'
        database_type = 'cluster'
        arn = 'DBClusterArn'
        create_time = 'ClusterCreateTime'
    
    for database in response[databases]:
        if find_tag(database['TagList'], 'DBSSR') and (pattern == 'ALL' or re.search(pattern, database[identifier])) and database['Engine'] in SUPPORTED_ENGINES:
            database_name = get_tag(database['TagList'], 'DBSSR')

            # Skip stopped databases
            if database[status] == 'stopped':
                continue

            # Skip Read Only Replicas
            if 'ReadReplicaSourceDBInstanceIdentifier' in database:
                continue

            if database_name in results:
                if create_time not in results[database_name] or database[create_time].replace(tzinfo=None) < results[database_name][create_time].replace(tzinfo=None):
                    results[database_name]['old'] = database[status]
                    continue
                database['old'] = results[database_name][status]

            # Get instance size from cluster's instance
            if identifier == 'DBInstanceIdentifier' and 'DBClusterIdentifier' in database:
                results[database_name] = { 'cluster': database['DBClusterIdentifier'], 'class': database['DBInstanceClass'] }
                continue

            vpc_security_groups = get_vpc_security_groups(database['VpcSecurityGroups'])
            database_status = database[status]
            create_time = get_tag(database['TagList'], 'DBSSRCreateTime')
            if database_type == 'cluster':
                results[database_name] = { 'snapshots': 0, 'create_time': create_time, 'old': database.get('old', 'none'), 'type': database_type, 'arn': database[arn], 'identifier': database[identifier], 'engine': database['Engine'], 'mode': database['EngineMode'], 'status': database_status, 'tags': database['TagList'], 'subnet_group': database['DBSubnetGroup'], 'vpc_security_groups': vpc_security_groups }
            else:
                results[database_name] = { 'snapshots': 0, 'create_time': create_time, 'old': database.get('old', 'none'), 'type': database_type, 'arn': database[arn], 'identifier': database[identifier], 'engine': database['Engine'], 'class': database['DBInstanceClass'], 'status': database_status, 'subnet_group': database['DBSubnetGroup']['DBSubnetGroupName'], 'vpc_security_groups': vpc_security_groups }
    
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

        # Ignore AWS Backup and automated snapshots
        if snapshot['SnapshotType'] in ['awsbackup', 'automated'] or 'awsbackup' in snapshot['name']:
            continue

        # Ignore unmatched snapshots
        if snapshot[identifier] not in databases:
            continue

        # Ignore as didn't match pattern or supported engine
        if ((pattern != 'ALL' and not re.search(pattern, snapshot[identifier])) or snapshot['Engine'] not in SUPPORTED_ENGINES):
            continue

        # Skip snapshots out of backup interval
        if backup_interval and 'SnapshotCreateTime' in snapshot and snapshot['SnapshotCreateTime'].replace(tzinfo=None) < datetime.utcnow().replace(tzinfo=None) - timedelta(hours=backup_interval):
            continue

        if snapshot[identifier] not in results:
            results[snapshot[identifier]] = snapshot
            databases[snapshot[identifier]]['snapshots'] += 1

        # Signal the existence of a shared and manual snapshot
        if snapshot['name'].replace('-target','') == results[snapshot[identifier]]['name'].split(':').pop() or snapshot['name'].split(':').pop() == results[snapshot[identifier]]['name'].replace('-target',''):
        # if not snapshot['SnapshotType'] == results[snapshot[identifier]]['SnapshotType']:
            print('Current: ', results[snapshot[identifier]]['name'], ':', results[snapshot[identifier]]['SnapshotType'], 'iterator:', snapshot['name'], ':', snapshot['SnapshotType'])
            databases[snapshot[identifier]]['snapshots'] += 1

        if 'SnapshotCreateTime' in snapshot and snapshot['SnapshotCreateTime'].replace(tzinfo=None) >= results[snapshot[identifier]]['SnapshotCreateTime'].replace(tzinfo=None):
            results[snapshot[identifier]] = snapshot
            continue

        results[snapshot[identifier]] = snapshot

    return results
