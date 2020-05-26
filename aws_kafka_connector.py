from base64 import b64encode

import boto3
import json
import os
import botocore
import time
from boto import exception
import paramiko
from datetime import datetime
import time

from aws_base import aws_base

os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAJSKKKXBPD6OEF6PA'
os.environ['AWS_SECRET_ACCESS_KEY'] = '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'

env_var = os.environ
aws_access_key_id_env = env_var['AWS_ACCESS_KEY_ID']
aws_secret_access_key_env = env_var['AWS_SECRET_ACCESS_KEY']


class KafkaClusterMgm(aws_base):
    def __init__(self):
        super().__init__()
        self.cluster_name_arn = []
        self.kafka_client = boto3.client('kafka', aws_access_key_id=aws_access_key_id_env,
                                         aws_secret_access_key=aws_secret_access_key_env, region_name=self.region_name)
        self.broker_list = []
        self.zookeeper_list = []

        self.key = None
        self.client = None

    def connect_to_ec2_node(self, instance_ip, cmd=None):
        print("{} Inside connect_to_ec2_node wit ip {} and cmd {}".format(datetime.now(), instance_ip, cmd))
        # key = paramiko.RSAKey.from_private_key_file("D:/pycherm_project/sample_file/MSKKeyPairNew.pem")
        # client = paramiko.SSHClient()
        # client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect/ssh to an instance
        try:
            if self.client is None:
                self.key = paramiko.RSAKey.from_private_key_file("D:/pycherm_project/sample_file/MSKKeyPairNew.pem")
                self.client = paramiko.SSHClient()
                self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                self.client.connect(hostname='54.184.128.78', username="ec2-user", pkey=self.key)

            # Here 'ubuntu' is user name and 'instance_ip' is public IP of EC2
            print("{} Inside connect_to_ec2_node..client created".format(datetime.now()))
            # self.client.connect(hostname=instance_ip, username="ec2-user", pkey=key)
            if cmd is None:
                cmd = 'pwd'

            # Execute a command(cmd) after connecting/ssh to an instance
            print("{} Inside connect_to_ec2_node..Going to execute command".format(datetime.now()))
            stdin, stdout, stderr = self.client.exec_command(cmd)
            print(stderr.read())
            # stdin, stdout, stderr = self.client.exec_command("cat /home/ec2-user/test_txt")
            # stdin,
            # stdin.flush()
            # print("{} Inside connect_to_ec2_node..Starting cntl c data".format(datetime.now()))
            print("{} Inside connect_to_ec2_node..Starting to  the data".format(datetime.now()))
            # print(stdout.read())
            data = stdout.read().splitlines()
            print(data)
            # print("{} Inside connect_to_ec2_node..waiting for the data".format(datetime.now()))
            # #
            # for line in data:
            #     x = line.decode()
            #     # print(line.decode())
            #     print(x)
            # close the client connection once the job is done

            # client.close()
            print("{} Inside connect_to_ec2_node..Closing the client".format(datetime.now()))
        except Exception as e:
            print(e)

    def consumer_client(self, topic_name):
        kafka_cmd = "bash /home/ec2-user/kafka_2.12-2.2.1/bin/kafka-console-consumer.sh"
        broker_id = " --bootstrap-server  b-3.kd-new-kafka.rccmy6.c3.kafka.us-west-2.amazonaws.com:9094"
        consumer_config = " --consumer.config /home/ec2-user/kafka_2.12-2.2.1/bin/client.properties"
        topic_name = " --topic " + topic_name
        # offset_point = " --from-beginning"
        # offset_point = " --latest"
        skip_message_on_error = " -skip-message-on-error"
        timeout_ms = " --timeout-ms 5000"
        number_of_message = " --max-messages 10"
        producer_command = kafka_cmd + broker_id + consumer_config + topic_name
        producer_command = producer_command + skip_message_on_error + timeout_ms + number_of_message
        # producer_command = "bash /home/ec2-user/kafka_2.12-2.2.1/bin/test_script.sh"
        # producer_command = "cat /home/ec2-user/kafka_2.12-2.2.1/bin/test_txt"
        print(producer_command)

        for i in range(0, 100):
            self.connect_to_ec2_node('54.184.128.78', producer_command)
            time.sleep(2)

    def producer_client(self, topic_name):
        message = "echo \"{num.replica.fetchers=2}\" |"
        kafka_cmd = "bash /home/ec2-user/kafka_2.12-2.2.1/bin/kafka-console-producer.sh"
        broker_id = " --broker-list b-3.kd-new-kafka.rccmy6.c3.kafka.us-west-2.amazonaws.com:9094,"
        consumer_config = " --producer.config /home/ec2-user/kafka_2.12-2.2.1/bin/client.properties"
        topic_name = " --topic " + topic_name
        consumer_command = message + kafka_cmd + broker_id + consumer_config + topic_name
        print(consumer_command)
        for i in range(0, 100):
            self.connect_to_ec2_node('54.184.128.78', consumer_command)
        self.client.close()

    def create_topic_client(self, topic_name):
        kafka_cmd = "bash /home/ec2-user/kafka_2.12-2.2.1/bin/kafka-topics.sh"
        create_cmd = " --create"
        zookeeper_id = " --zookeeper z-2.kd-new-kafka.rccmy6.c3.kafka.us-west-2.amazonaws.com:2181"
        replicate_partition = " --replication-factor 1 --partitions 3"
        topic_name = " --topic " + topic_name
        topic_create_command = kafka_cmd + create_cmd + zookeeper_id + replicate_partition + topic_name
        self.connect_to_ec2_node('54.184.128.78', topic_create_command)
        self.client.close()

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
                # print("response", json.dumps(response['ClusterInfoList'], indent=1))
                for itr in response['ClusterInfoList']:
                    self.cluster_name_arn.append(
                        {'ClusterName': itr['ClusterName'],
                         'ClusterArn': itr['ClusterArn']}
                    )
                # self.zookeeper_list = response['ClusterInfoList'][0]['ZookeeperConnectString']
                print("Broker list", self.broker_list)
                print("Zookeeper list", self.zookeeper_list)

        except botocore.exceptions.ClientError as cle:
            print("Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))

    def describe_configuration(self):
        try:
            response = self.kafka_client.list_configurations()
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(response['Configurations'][0]['Arn'])
                return response['Configurations'][0]['Arn']
        except botocore.exceptions.ClientError as cle:
            print(cle)
            print("Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))
            return None

    def describe_particular_instance(self, cluster_arn_id):
        try:
            response = self.kafka_client.describe_cluster(
                ClusterArn=cluster_arn_id
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return response
                # response['ClusterInfo']['State']
        except botocore.exceptions.ClientError as cle:
            print("Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))
            print("Cluster got delete already")
            return None

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

    def create_kafka_cluster_config(self, cluster_name):
        cluster_config_name = cluster_name + "_kafka_config"
        try:
            config_file = open('D:/pycherm_project/sample_file/New_folder/config-file.txt', 'r')
            server_properties = config_file.read()
            abc = b64encode(bytes(server_properties, encoding='utf-8'))
            print(abc)
            print(type(abc))
            # s = bytes(server_properties, encoding='utf-8')
            # print(type(s))
            # s2 = b64encode(bytes("auto.create.topics.enable=false", encoding='utf-8'))
            # print(type(s2))
            # print(s)
            a = 'krish'
            # print(type(abc))

            response = self.kafka_client.create_configuration(
                Name=cluster_config_name,
                Description='The configuration to use on all own clusters.',
                KafkaVersions=['2.2.1'],
                ServerProperties=abc
            )
            print(response)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print("Kafka cluster configuration {} got created successfully".format(cluster_config_name))

        except botocore.exceptions.ClientError as cle:
            print(cle)
            print("Error in {} is {} due to {}".format(__name__, cle.response['Error']['Code'],
                                                       cle.response['Error']['Message']))
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

            kafka_config_cluster_arn = self.describe_configuration()
            response = self.kafka_client.create_cluster(
                ClusterName=cluster_name,
                KafkaVersion='2.2.1',
                NumberOfBrokerNodes=3,
                BrokerNodeGroupInfo={
                    'BrokerAZDistribution': 'DEFAULT',
                    'ClientSubnets': [subnet_list_local[0], subnet_list_local[2], subnet_list_local[3]],
                    # 'ClientSubnets': [subnet_list_local[2], subnet_list_local[3]],
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
                },
                ConfigurationInfo={
                    'Arn': kafka_config_cluster_arn,
                    'Revision': 1
                }
            )
            print(response)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                while True:
                    status_response = self.describe_particular_instance(response['ClusterArn'])
                    # status_response = self.describe_particular_instance(abc)
                    # print(status_response)
                    if status_response['ClusterInfo']['State'] == 'ACTIVE':
                        print('The instance with instance id  {} is state {}'.format(response['ClusterArn'],
                                                                                     'ACTIVE'))
                        self.cluster_name_arn.append(
                            {'ClusterName': cluster_name,
                             'ClusterArn': response['ClusterArn']})

                        # populate the zookeeper_list list
                        print("status_response = ", status_response)
                        self.zookeeper_list = status_response['ClusterInfo']['ZookeeperConnectString'].split(',')
                        # populate the broker list
                        broker_list_response = self.kafka_client.get_bootstrap_brokers(
                            ClusterArn=response['ClusterArn'])
                        if broker_list_response is not None:
                            self.broker_list = broker_list_response['BootstrapBrokerStringTls'].split(',')
                        break

                    print("waiting for 40 seconds")
                    time.sleep(40)
        except botocore.exceptions.ClientError as cle:
            print("Generic error happened ", json.dumps(cle.response, indent=1))
            print("Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))
        except botocore.exceptions.ParamValidationError as parval:
            print("parameter validation error happened ", parval)

    def delete_cluster(self, cluster_name, delete_log_group='N'):
        try:
            response = ""
            count = 0
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
                                print(
                                    'The instance with instance id {} is state {}'.format(itr['ClusterArn'], response))
                                if delete_log_group == 'Y':
                                    log_group_name = cluster_name + "_kafka_aws_log_group"
                                    print("delete the log group {}".format(log_group_name))
                                    response = self.cloudwatch_log.delete_log_group(logGroupName=log_group_name)
                                    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                                        print("Log group {} gor deleted successfully".format(log_group_name))
                                    break
                            else:
                                count = count + 1
                            print("waiting for 5 seconds")
                            time.sleep(5)
                        break
            del self.cluster_name_arn[count]
        except botocore.exceptions.ClientError as cle:
            print("Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))
            # print("Generic error happened ", json.dumps(cle.response, indent=1))


def main():
    kafkaCluster = KafkaClusterMgm()

    while True:
        operation = input("Enter the type of operation u want to do :-")
        if operation not in ('list', 'exit', 'read'):
            kafkaCluster.describe_all_instance()

        if operation == 'list':
            kafkaCluster.describe_all_instance()
        elif operation == "new":
            cluster_name = input("Please provide the kafka cluster name you want to create")
            print(cluster_name.find('_'))
            if cluster_name.find('_') < 0:
                now = datetime.now()
                print("Operation start time {}".format(now.strftime('%Y-%m-%d %H:%M:%S %f')))
                log_group_name = kafkaCluster.create_cloudwatch_log_group(cluster_name)
                if log_group_name is not None:
                    # Create cluster configuration
                    # kafkaCluster.create_kafka_cluster_config(cluster_name)
                    kafkaCluster.create_kafka_cluster(cluster_name, log_group_name)
                    print(True)

                    now = datetime.now()
                    print("Operation end time {}".format(now.strftime('%Y-%m-%d %H:%M:%S %f')))
            else:
                print("cluster name {} with {} is not accepted".format(cluster_name, "_"))
        elif operation == "listc":
            kafkaCluster.describe_configuration()
        elif operation == "drop":
            node_name = input("Please provide the cluster name to drop : -")
            delete_log_group = input("Log need to be deleted Y/N: -")
            kafkaCluster.delete_cluster(node_name, delete_log_group)
        elif operation == "join":
            node_ip = input("Please provide the IP name to login : -")
            kafkaCluster.connect_to_ec2_node(node_ip)
        elif operation == "exit":
            break
        elif operation == "read":
            topic_name = input("Please provide the topic name to read :-")
            kafkaCluster.consumer_client(topic_name)
        elif operation == "write":
            topic_name = input("Please provide the topic name to write :-")
            kafkaCluster.producer_client(topic_name)
        elif operation == "topic":
            topic_name = input("Please provide the topic name to create :-")
            kafkaCluster.create_topic_client(topic_name)


if __name__ == "__main__":
    main()

"""
Readme
aws kafka describe-cluster --region us-west-2 --cluster-arn "arn:aws:kafka:us-west-2:534462043435:cluster/kd-kafka-cluster/93d1720b-5d37-4a33-a62f-7fb826d7ae9a-3"

bin/kafka-topics.sh --create --zookeeper z-3.kd-k-cluster.hlxot0.c3.kafka.us-west-2.amazonaws.com:2181 --replication-factor 1 --partitions 3 --topic AWSKafkaTopic
bin/kafka-topics.sh --create --zookeeper z-2.kd-new-kafka.rccmy6.c3.kafka.us-west-2.amazonaws.com:2181 --replication-factor 1 --partitions 3 --topicAWSKafkaTopic

aws kafka get-bootstrap-brokers --region us-west-2 --cluster-arn "arn:aws:kafka:us-west-2:534462043435:cluster/kd-new-kafka/35670bcd-d7a0-473f-8bbd-6c442aa951cd-3"


bash kafka-console-producer.sh --broker-list b-1.kd-new-cluster.4jg3y2.c3.kafka.us-west-2.amazonaws.com:9094 --producer.config client.properties --topic AWSKafkaTopic

bash kafka-console-consumer.sh --bootstrap-server b-3.kd-new-kafka.rccmy6.c3.kafka.us-west-2.amazonaws.com:9094 --consumer.config client.properties --topic AWSKafkaTopic --from-beginning
"""
