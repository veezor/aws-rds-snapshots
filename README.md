# AWS RDS Snapshots

## Database snapshot sharing and restoring between AWS accounts

This tool aims to run periodically both at a source and target AWS accounts and provide shared snapshots from the source
account and subsequent refreshment on the latter. You can tag source and target databases to signal both snapshot creation
on the source account and restoration on the target account.

## Supported engines

All flavors of MySQL and PostgreSQL compatible engines are supported, including Aurora and native ones. Both clusters, instances and serverless types work.

 - **Types**: Instances, Clusters
 - **Modes**: Serverless, Provisioned
 - **Engines**: Aurora, MySQL, PgSQL

### Currently tested combinations:

- Provisioned MySQL Community RDS Instance
- Provisioned PgSQL RDS Instance
- Serverless Aurora MySQL Cluster
- Serverless Aurora PgSQL Cluster
- Provisioned Aurora MySQL Cluster
- Provisioned Aurora PgSQL Cluster

## How it works

This project is composed by a couple of AWS Lambda functions that will basically perform these 9 steps:

### Source AWS account
1. Identify elegible databases based on a given pattern
2. Find automated snapshots if available in the interval defined
3. Copy existing or take a fresh snapshot from the database
4. Share snapshot with the target account\
    ...

&nbsp; &nbsp;9. Delete disposable snapshots

### Target AWS account
5. Identify new shared snapshots based on a given pattern
6. Delete its existing database counterpart
7. Restore snapshot to refresh the data
8. Signal snapshot as disposable

## Usage

These scripts are built to run as independent AWS Lambda functions on separate accounts. They will interact with each other asynchronously as they detect state changes via tags or existing resources that match patterns.

### Source account

You can define a `DATABASE_NAME_PATTERN` environment variable set to `ALL` to catch all databases on the source side, define a regex value to match, or set it to `TAG` (which is recommended to avoid uneccessary snapshots) and set a tag to the source RDS database/cluster named `DBSSRSource` with value `true`.

You should also override default environment variable values as needed as described below:

```
LOG_LEVEL=ERROR
SOURCE_AWS_REGION=us-east-1
BACKUP_INTERVAL=24
DATABASE_NAME_PATTERN=TAG
AWS_TARGET_KMS_KEY=None
AWS_TARGET_ACCOUNT=000000000000
```

### Target account

You can define `DATABASE_NAME_PATTERN` to narrow the results but you must tag the target RDS databases/clusters with `DBSSR` tag and the value set to the name of the source database.

> Please notice that for provisioned cluster databases, you must tag both the cluster and also the primary instance.

The way this script works allows you to define a different instance type and size on the target account and it will restore the source snapshot preserving the target profile regardless of the source's type and size.

You should also override default environment variable values as needed as described below:

```
LOG_LEVEL=ERROR
TARGET_AWS_REGION=us-east-1
BACKUP_INTERVAL=24
DATABASE_NAME_PATTERN=ALL
AWS_TARGET_KMS_KEY=None
AWS_SOURCE_ACCOUNT=00000000000
```

## Deploying to AWS

The deploy process uses the [Serverless Framework](https://www.serverless.com/). In order to deploy, you need to fill in the values within the `serverless.yml` file.

Important values to set are:
- `awsTargetAccount`
- `awsTargetKmsKey`

> It is important to notice that you must run this script twice and make sure each time you run you use the appropriate source and target profiles or credentials for the corresponding AWS accounts.

### Triggering functions

Functions are triggered by default using a cron expression. You can override the default value on `serverless.yml`

## Gotchas

* If there is a misbehavior in the scripts it is possible that it is related to the backup interval that at some point might have skipped
snapshots because they've got out of scope.

* No option or parameter groups will be copied.

## Encryption Keys

If your AWS source account RDS databases are encrypted, you must create a custom AWS KMS key in order to copy and share snapshots.

Also, for AWS target account RDS database encryption, you must provide an encryption key for restoration.

## Running Locally

You can run the lambdas locally by clonning this project and installing `python-lambda-local` package:

    pip install python-lambda-local


Running local commands should look like this:

### Source account

```bash
$ DATABASE_NAME_PATTERN="database1|database2|databaseN" AWS_TARGET_KMS_KEY=arn:aws:kms:us-east-1:123456789123:key/blah-blah-blah AWS_TARGET_ACCOUNT=23456789012 LOG_LEVEL=debug python-lambda-local -t 60 -l ./ -f lambda_handler copy_or_take_snapshots.py event.json
```

### Target account

```bash
$ DATABASE_NAME_PATTERN="database1|database2|databaseN" BACKUP_INTERVAL=168 AWS_TARGET_KMS_KEY=arn:aws:kms:us-east-1:23456789012:key/blah-blah-blah AWS_SOURCE_ACCOUNT=123456789123 LOG_LEVEL=debug python-lambda-local -t 60 -l ./ -f lambda_handler restore_snapshots.py event.json 
```

## Contributing

If you find a bug or want to contribute with a new feature, please feel free to open an issue and send a pull request.

## Enterprise Support

If you require assistance in installing, configuring or continuous support, please contact [Veezor](https://veezor.com).
