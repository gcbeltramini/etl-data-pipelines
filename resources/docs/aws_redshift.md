# Create an AWS Redshift cluster

Reference: [Getting started with Amazon Redshift](<https://docs.aws.amazon.com/redshift/latest/gsg/getting-started.html>)

## Create an AWS account

1. <https://aws.amazon.com/console/> --> "Create a Free Account"
1. After the account is created, sign in at <https://aws.amazon.com/console/> using "Root user"

## Create an IAM user

Redshift has to act as a user who has read access to S3.

1. IAM: <https://console.aws.amazon.com/iam/>
1. Left navigation pane: [Users](https://console.aws.amazon.com/iam/home#/users)
1. [Add user](https://console.aws.amazon.com/iam/home#/users$new?step=details)
   1. `User name`: redshift
   1. `Access type`: Programmatic access
   1. `Attach existing policies directly`: check `AmazonS3ReadOnlyAccess`
   1. `Add tags`: skip
1. Download .csv

## Create a security group

Set this to connect to Redshift locally, otherwise it is accessible only from the [VPC](https://aws.amazon.com/vpc/).

1. EC2: <https://console.aws.amazon.com/ec2>
1. Since we are going to create the Redshift cluster in region `us-west-2`, make sure this is the
region in the top right corner.
1. Left navigation pane: `Network & Security` --> `Security Groups` --> `Create Security Group`
1. `Basic details`:
   - `Security group name`: "RedshiftSecurityGroup"
   - `Description`: "Authorize Redshift cluster access"
   - `VPC`: leave the default
1. `Inbound rules` --> `Add Rule`:
   - `Type`: Redshift
   - `Protocol`: TCP
   - `Port range`: 5439
   - `Source`: "Custom", 0.0.0.0/0; or "My IP", and let AWS find your IP (check [here](https://www.whatsmyip.org))
     - **Important**: Using 0.0.0.0/0 is not recommended for anything other than demonstration
     purposes because it allows access from any computer on the internet. In a real environment, you
     would create inbound rules based on your own network settings.
1. `Create security group`

## Launch a Redshift cluster

**Important**: The standard Amazon Redshift usage fees will apply for the cluster that is about to
be created until it is deleted.

1. Redshift: <https://console.aws.amazon.com/redshift/>
1. At the top right corner, choose the AWS region `us-west-2` to create the cluster.
1. On the navigation menu, choose `Clusters` --> `Launch cluster`.
1. `Cluster details`:
   - `Cluster identifier`: "redshift-cluster"
   - `Node type`: dc2.large
   - `Nodes`: 2 or 4 should be enough
   - `Database name`: "dev"
   - `Database port`: 5439
   - `Master user name`: "awsuser"
   - `Master user password`: enter a password (keep closely guarded, e.g., don't share on GitHub)
1. `Additional configurations - Network and security` (leave the default for the rest):
   - `VPC security groups`: add the security group previously created (e.g.,
   "RedshiftSecurityGroup"); leave the `default` security group.
   - `Publicly accessible`: Yes
1. `Create cluster` (it takes about 5-10 minutes)
1. In the list of clusters, click on the cluster created now and copy the endpoint (the cluster must
be created).
   - The port should be removed from the end of the endpoint.
1. Insert the endpoint and password in [Airflow when adding the connection to Redshift](airflow.md#add-connection-to-redshift).

## Create tables

1. Redshift: <https://console.aws.amazon.com/redshift/>
1. Left navigation menu: `EDITOR`
1. Run each of the SQL statements from file `create_tables.sql`

## Cleanup

Perform these steps after running the ETL.

1. Delete the cluster:
   - Redshift: <https://console.aws.amazon.com/redshift/>. Select the cluster and delete it. It is
   not necessary to create a snapshot.
1. Delete the security group:
   1. EC2: <https://console.aws.amazon.com/ec2> --> choose region `us-west-2` in the top right
   corner
   1. Left navigation pane: `Network & Security` --> `Security Groups` --> select the new security
   group (e.g., "RedshiftSecurityGroup")
   1. "Actions" --> "Delete security group"
1. Delete the IAM user:
   1. IAM: <https://console.aws.amazon.com/iam/>
   1. Left navigation pane: [Users](https://console.aws.amazon.com/iam/home#/users)
   1. Select "redshift" --> Delete user
