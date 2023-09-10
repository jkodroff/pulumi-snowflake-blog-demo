# blog-snowflake-elt-python

This repository contains the finished code for the Medium post [Lightning-fast ELT for Python Devs with AWS, Snowpipe, and Pulumi](https://medium.com/p/4eaf056dd097) which appears in Snowflake's blog.

To deploy this code, you will need:

1. The [Pulumi command line](https://www.pulumi.com/docs/install/) and a [free Pulumi account](https://www.pulumi.com/docs/pulumi-cloud/accounts/).
1. An AWS principal (either an IAM user or an assumable role) with FullAdmin permissions. For details on how to configure your AWS credentials for use with Pulumi programs, see [AWS Classic: Installation & Configuration](https://www.pulumi.com/registry/packages/aws/installation-configuration/).
1. A Snowflake user with admin permissions. For details on how to configure your Snowflake credentials for use with Pulumi programs, see [Snowflake: Installation & Configuration](https://www.pulumi.com/registry/packages/snowflake/installation-configuration/). Note that the “Account” configuration variable is displayed as “Locator” in the Snowflake console.

To deploy the code:

```bash
cd pulumi && pulumi up
```

Note that you may see a failure when creating the `snowflake:index:Pipe` resources stating that the AWS role cannot be assumed. If you encounter this error, wait a few seconds and re-run `pulumi up`. (It appears as though the role actually takes some time after creation before it can be assumed by Snowflake.)

Once the update is complete, to upload the sample data:

```bash
cd pulumi & aws s3 sync ../data s3://$(pulumi stack output bucketName)
```
