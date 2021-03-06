import configparser
import boto3
import json
import time

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

def check_sample_bucket(s3):
    sampleDbBucket = s3.Bucket("awssampledbuswest2")
    for bucket in sampleDbBucket.objects.filter(Prefix='ssbgz'):
        print(bucket)

def create_iam_role(iam):
    print('1.1 Creating a new IAM Role')
    dwhRole = iam.create_role(Path='/', 
                              RoleName=DWH_IAM_ROLE_NAME, 
                              Description="Allows Redshift clusters to call AWS services on your behalf.", 
                              AssumeRolePolicyDocument=json.dumps(
                                  {'Statement': [{'Action':'sts:AssumeRole', 
                                                  'Effect':'Allow', 
                                                  'Principal': {'Service':'redshift.amazonaws.com'}}], 
                                   'Version':'2012-10-17'}))
    print('1.2 Attaching Policy')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']
    print('1.3 Get the IAM role ARN')
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print(roleArn)
    return roleArn

def create_redshift_cluster(redshift, roleArn):
    response = redshift.create_cluster(        
        ClusterType=DWH_CLUSTER_TYPE, 
        NodeType=DWH_NODE_TYPE, 
        NumberOfNodes=int(DWH_NUM_NODES),
        DBName=DWH_DB, 
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        IamRoles=[roleArn]
    )

def open_tcp_port(ec2, myClusterProps):
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[1]
    print(defaultSg)
    
    defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )

def main():
    ec2 = boto3.resource('ec2', 
                     region_name="us-west-2", 
                     aws_access_key_id=KEY, 
                     aws_secret_access_key=SECRET)
    s3 = boto3.resource('s3', 
                        region_name="us-west-2",
                        aws_access_key_id=KEY, 
                        aws_secret_access_key=SECRET)
    iam = boto3.client('iam', 
                    region_name="us-west-2", 
                    aws_access_key_id=KEY, 
                    aws_secret_access_key=SECRET)
    redshift = boto3.client('redshift', 
                            region_name="us-west-2", 
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    # check_sample_bucket(s3)
    roleArn = create_iam_role(iam)
    create_redshift_cluster(redshift, roleArn)
    # wait until created
    while 1:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        if myClusterProps['ClusterStatus'] == 'available':
            break
        print("Waiting...")
        time.sleep(10)
    open_tcp_port(ec2, myClusterProps)
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)


if __name__ == '__main__':
    main()