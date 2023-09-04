# pulumi-snowflake-blog-demo

Temporary home for a Pulumi example w/Snowflake

To upload the sample data:

```bash
aws s3 sync ../data s3://$(pulumi stack output bucketName)
```
