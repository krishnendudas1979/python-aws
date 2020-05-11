import boto3
import json
import os
import botocore
import time
from boto import exception

# from boto.logs.exceptions import Kafka.Client.exceptions

# bucket_name = input("Enter s3 Bucket  name : ")
operation = input("Enter the type of operation u want to do :-")
os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAIM7FUUWPXTWFFQ5A'
os.environ['AWS_SECRET_ACCESS_KEY'] = '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'

env_var = os.environ
aws_access_key_id_env = env_var['AWS_ACCESS_KEY_ID']
aws_secret_access_key_env = env_var['AWS_SECRET_ACCESS_KEY']


class KafkaClusterMgm:
    def __init__(self):
        self.image_type = 'ami-02d0ea44ae3fe9561'
        self.instance_type = 't2.micro'
        self.region_name = 'us-west-2'
        # AvailabilityZone = 'us-west-2b'
        self.IPv4_CIDR = '172.31.0.0/16'
        self.cluster_name_arn = []
        self.kafka_client = boto3.client('kafka', aws_access_key_id=aws_access_key_id_env,
                                         aws_secret_access_key=aws_secret_access_key_env, region_name=self.region_name)
        self.ec2_resource = boto3.resource('ec2', aws_access_key_id=aws_access_key_id_env,
                                           aws_secret_access_key=aws_secret_access_key_env,
                                           region_name=self.region_name)
        self.ec2_client = boto3.client('ec2', aws_access_key_id=aws_access_key_id_env,
                                       aws_secret_access_key=aws_secret_access_key_env, region_name=self.region_name)

        self.cloudwatch_log = boto3.client('logs', aws_access_key_id=aws_access_key_id_env,
                                           aws_secret_access_key=aws_secret_access_key_env,
                                           region_name=self.region_name)

    def create_cloudwatch_log_group(self, clustername):
        try:
            log_group_name = clustername + "_kafka_aws_log_group"
            response = self.cloudwatch_log.describe_log_groups(
                logGroupNamePrefix=log_group_name,
                limit=10
            )
            if (response['ResponseMetadata']['HTTPStatusCode'] == 200) and (len(response['logGroups']) != 0):
                print("Log group {} is already exist".format(response['logGroups'][0]['logGroupName']))
                return log_group_name
            else:
                print("Log group {} doesn't exist. will create".format(log_group_name))
                response = self.cloudwatch_log.create_log_group(
                    logGroupName=log_group_name,
                    # kmsKeyId='string',
                    tags={
                        'name': 'kafka log group'
                    }
                )
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    print("Log group {} gor created successfully".format(log_group_name))
                    return log_group_name
        except botocore.exceptions.ClientError as cle:
            print("Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))
            return None

    def describe_all_instance(self):
        try:
            response = self.kafka_client.list_clusters(
                MaxResults=10
            )
            ec2_state = None
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(response)
                for itr in response['ClusterInfoList']:
                    self.cluster_name_arn.append(
                        {'ClusterName': itr['ClusterName'],
                         'ClusterArn': itr['ClusterArn']}
                    )
        except botocore.exceptions.ClientError as cle:
            print("Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))

    def describe_particular_instance(self, cluster_arn_id):
        try:
            response = self.kafka_client.describe_cluster(
                ClusterArn=cluster_arn_id
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return response['ClusterInfo']['State']
        except botocore.exceptions.ClientError as cle:
            print("Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))
            print("Cluster got delete already")
            return "Terminated"

    def __get_vpc_details(self, IPv4_CIDR):
        try:
            response = self.ec2_client.describe_vpcs(
                Filters=[{
                    'Name': 'cidr',
                    'Values': [IPv4_CIDR]}],
                DryRun=False,
                MaxResults=12
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                # print("response",json.dumps(response, indent=1))
                return response['Vpcs'][0]['VpcId']
        except botocore.exceptions.ClientError as cle:
            print("Generic error happened ", json.dumps(cle.response, indent=1))
            raise botocore.exception

    def __get_subnet_details(self, vpc_details):
        try:
            subnet_list = []
            response = self.ec2_client.describe_subnets(
                Filters=[{
                    'Name': 'vpc-id',
                    'Values': [vpc_details]}],
                DryRun=False,
                MaxResults=12
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                # print("response", json.dumps(response, indent=1))
                for itr in response['Subnets']:
                    subnet_list.append({'subnetid': itr['SubnetId'],
                                        'azoneid': itr['AvailabilityZone']})

                return subnet_list
        except botocore.exceptions.ClientError as cle:
            print("Generic error happened ", json.dumps(cle.response, indent=1))
            subnet_list = None
            return subnet_list

    def __get_security_groups_details(self, security_group):
        try:
            security_group_list = []
            response = self.ec2_client.describe_security_groups(
                GroupNames=[security_group],
                DryRun=False
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                # print("response", json.dumps(response, indent=1))
                return response['SecurityGroups'][0]['GroupId']
        except botocore.exceptions.ClientError as cle:
            print("Generic error happened ", json.dumps(cle.response, indent=1))
            return None

    def __create_security_groups_details(self, security_group_id, vpc_id):
        try:
            response = self.ec2_client.create_security_group(
                Description='security group for ec2 instance',
                GroupName=security_group_id,
                VpcId=vpc_id,
                DryRun=False
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print("response", response)
                return response['GroupId']
        except botocore.exceptions.ClientError as cle:
            print("Generic error happened ", json.dumps(cle.response, indent=1))
            return None

    def __update_security_group(self, security_group_name, security_group_id):
        try:
            security_group = self.ec2_resource.SecurityGroup(security_group_id)
            response = security_group.authorize_ingress(
                GroupName=security_group_name,
                IpPermissions=[
                    {
                        'FromPort': 22,
                        'IpProtocol': 'tcp',
                        'IpRanges': [
                            {
                                'CidrIp': '0.0.0.0/0',
                                'Description': 'all allowed'
                            }
                        ],
                        'ToPort': 22
                    },
                ]
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print("response", response)
                # return response['GroupId']
        except botocore.exceptions.ClientError as cle:
            print("Generic error happened ", json.dumps(cle.response, indent=1))
            # return None

    def create_kafka_cluster(self, cluster_name, log_group_name):
        try:
            security_group_id = None
            # get the VPC details
            vpc_details = self.__get_vpc_details(self.IPv4_CIDR)
            # get the list of subnet available under that VPC
            subnet_list = self.__get_subnet_details(vpc_details)

            # Check and create a security group (if not exist)
            security_group_name = "kafka-security-group" + self.region_name
            security_group_id = self.__get_security_groups_details(security_group_name)
            print(security_group_id)
            if security_group_id is None:
                # Create a new security group
                security_group_id = self.__create_security_groups_details(security_group_name, vpc_details)
                # Update the incoming and outgoing rule of new security group
                if security_group_id is not None:
                    self.__update_security_group(security_group_name, security_group_id)
            else:
                print(
                    "The security group {} is already exists. No need to create a new one".format(security_group_name))
            print("subnet_list  {0}".format(subnet_list))
            subnet_list_local = []
            for i in range(len(subnet_list)):
                subnet_list_local.append(subnet_list[i]['subnetid'])
            print(subnet_list_local)

            response = self.kafka_client.create_cluster(
                ClusterName=cluster_name,
                KafkaVersion='2.2.1',
                NumberOfBrokerNodes=2,
                BrokerNodeGroupInfo={
                    'BrokerAZDistribution': 'DEFAULT',
                    'ClientSubnets': [subnet_list_local[2], subnet_list_local[3]],
                    'InstanceType': 'kafka.t3.small',
                    'SecurityGroups': [security_group_id],
                    'StorageInfo': {
                        'EbsStorageInfo': {
                            'VolumeSize': 3
                        }
                    }
                },
                EncryptionInfo={
                    'EncryptionAtRest': {
                        'DataVolumeKMSKeyId':
                            'arn:aws:kms:us-west-2:534462043435:key/6e5ea570-48bc-44dc-9d16-d62d937eaa83'
                    },
                    'EncryptionInTransit': {
                        'ClientBroker': 'TLS',
                        'InCluster': True
                    }
                },
                EnhancedMonitoring='PER_TOPIC_PER_BROKER',
                LoggingInfo={
                    'BrokerLogs': {
                        'CloudWatchLogs': {
                            'Enabled': True,
                            'LogGroup': log_group_name
                        }
                    }
                },
                Tags={
                    'name': 'aws-kafka'
                }
            )
            print(response)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                while True:
                    status_response = self.describe_particular_instance(response['ClusterArn'])
                    if status_response == 'ACTIVE':
                        print('The instance with instance id  {} is state {}'.format(response['ClusterArn'],
                                                                                     status_response))
                        self.cluster_name_arn.append({cluster_name, response['ClusterArn']})

                        break
                    print("waiting for 10 seconds")
                    time.sleep(10)
        except botocore.exceptions.ClientError as cle:
            print("Generic error happened ", json.dumps(cle.response, indent=1))
            print("Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))
        except botocore.exceptions.ParamValidationError as parval:
            print("parameter validation error happened ", parval)

    def delete_cluster(self, cluster_name):
        try:
            # {'ClusterName': itr['ClusterName'],
            #  'ClusterArn': itr['ClusterArn']}
            #
            response = ""
            for itr in self.cluster_name_arn:
                print(itr['ClusterName'])
                if itr['ClusterName'] == cluster_name:
                    response = self.kafka_client.delete_cluster(
                        ClusterArn=itr['ClusterArn'])
                    ec2_state = None
                    print(response)
                    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                        while True:
                            response = self.describe_particular_instance(response['ClusterArn'])
                            if response == 'DELETING':
                                print('The instance with instance id {} is state {}'.format(itr['ClusterArn'], response))
                                break
                            print("waiting for 5 seconds")
                            time.sleep(5)
                        break
        except botocore.exceptions.ClientError as cle:
            print("Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))
            # print("Generic error happened ", json.dumps(cle.response, indent=1))


def main():
    kafkaCluster = KafkaClusterMgm()
    if operation == 'list':
        kafkaCluster.describe_all_instance()
    elif operation == "new":
        kafkaCluster.describe_all_instance()
        cluster_name = input("Please provide the kafka cluster name you want to create")
        print(cluster_name.find('_'))
        if cluster_name.find('_') < 0:
            log_group_name = kafkaCluster.create_cloudwatch_log_group(cluster_name)
            if log_group_name is not None:
                kafkaCluster.create_kafka_cluster(cluster_name, log_group_name)
                print(True)
        else:
            print("cluster name {} with {} is not accepted".format(cluster_name, "_"))
    elif operation == "drop":
        kafkaCluster.describe_all_instance()
        node_name = input("Please provide the cluster name to drop : -")
        kafkaCluster.delete_cluster(node_name)


if __name__ == "__main__":
    main()
