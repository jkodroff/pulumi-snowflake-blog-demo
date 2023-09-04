"""A Python Pulumi program"""

import pulumi
import pulumi_aws as aws
import pulumi_snowflake as snowflake

import json

bucket = aws.s3.Bucket("pulumi-snowflake-blog-demo")

ROLE_NAME = "snowflake-storage-integration"

account_id = aws.get_caller_identity().account_id

storage_integration = snowflake.StorageIntegration(
    "snowflake-storage-integration",
    enabled=True,
    storage_aws_role_arn=f"arn:aws:iam::{account_id}:role/{ROLE_NAME}",
    storage_provider="S3",
    type="EXTERNAL_STAGE",
    storage_allowed_locations=["*"]
)

snowflake_assume_role_policy = pulumi.Output.all(storage_integration.storage_aws_iam_user_arn, storage_integration.storage_aws_external_id).apply(lambda args: json.dumps({
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"AWS": args[0]},
        "Action": "sts:AssumeRole",
        "Condition": {
            "StringEquals": {"sts:ExternalId": args[1]}
        }
    }]
}))

snowflake_role = aws.iam.Role(
    "snowflake-integration-role",
    name=ROLE_NAME,
    description="Allows Snowflake to access the bucket containing files for import",
    assume_role_policy=snowflake_assume_role_policy
)

snowflake_policy = aws.iam.Policy(
    "snowflake-storage-integation-policy",
    policy=bucket.arn.apply(lambda arn: json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:DeleteObject",
                    "s3:DeleteObjectVersion"
                ],
                "Resource": f"{arn}/*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                "Resource": f"{arn}",
            }
        ]
    }))
)

aws.iam.RolePolicyAttachment(
    "snowflake-policy-attachment",
    role=ROLE_NAME,
    policy_arn=snowflake_policy.arn
)

database = snowflake.Database(
    "pulumi-snowflake-demo",
)

schema = snowflake.Schema(
    "jaffle-shop",
    name="JAFFLE_SHOP",
    database=database.name,
)

table = snowflake.Table(
    "jaffle-shop-customers",
    name="CUSTOMERS",
    database=database.name,
    schema=schema.name,
    columns=[
        # Metadata fields:
        {
            "name": "FILENAME",
            "type": "VARCHAR",
            "nullable": False
        },
        {
            "name": "LAST_MODIFIED_AT",
            "type": "TIMESTAMP_NTZ(9)",
            "nullable": False
        },
        {
            "name": "LOADED_AT",
            "type": "TIMESTAMP_NTZ(9)",
            "nullable": False
        },
        # Fields from the exported file:
        {
            "name": "ID",
            "type": "VARCHAR",
            "nullable": False
        },
        {
            "name": "NAME",
            "type": "VARCHAR",
            "nullable": False
        },
    ]
)

stage = snowflake.Stage(
    "snowpipe-stage",
    url=pulumi.Output.format("s3://{0}", bucket.bucket),
    database=database.name,
    schema=schema.name,
    storage_integration=storage_integration.name,
    comment="Loads data from an S3 bucket containing Jaffle Shop data"
)

# Notes:
# 1. The Snowflake PATTERN arguments are regex-style, not `ls` style.
# 2. The PATTERN clause is so that we do not run the COPY statement for files we don't want to import.
# 3. We intentionally skip the first column (i.e. we start with $2) because the first column of the file is the line number.
copy_statment = pulumi.Output.format("""
COPY INTO \"{0}\".\"{1}\".\"{2}\" 
FROM (SELECT metadata$filename, metadata$file_last_modified, sysdate(), $2, $3 FROM @"{0}"."{1}"."{3}")
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
PATTERN=\"jaffle-shop-customers/.*.csv\"
""", database.name, schema.name, table.name, stage.name)


pulumi.export("copy_statement", copy_statment)

pipe = snowflake.Pipe(
    "pipe",
    auto_ingest=True,
    comment="My pipe's comment",
    copy_statement=copy_statment,
    database=database.name,
    schema=schema.name
)

aws.s3.BucketNotification(
    "bucket-notification",
    bucket=bucket.bucket,
    queues=[{
        "queue_arn": pipe.notification_channel,
        "events": ["s3:ObjectCreated:*"]
    }]
)

pulumi.export("bucketName", bucket.bucket)
