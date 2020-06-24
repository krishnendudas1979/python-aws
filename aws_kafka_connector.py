from base64 import b64encode

import boto3
import json
import os
import botocore
import paramiko
from datetime import datetime
import time
import logging
from aws_base import aws_base
from multiprocessing import Process
import signal

class KafkaClusterMgm(aws_base):
    def __init__(self):
        super().__init__()
        self.cluster_name_arn = []
        self.kafka_client = boto3.client('kafka', aws_access_key_id=self.aws_access_key_id_env,
                                         aws_secret_access_key=self.aws_secret_access_key_env,
                                         region_name=self.region_name)
        self.broker_list = []
        self.zookeeper_list = []
        self.cluster_name = None
        self.partition_count = 3
        # self.kafka_consumer_cmd = "bash /home/ec2-user/kafka_2.12-2.2.1/bin/kafka-console-consumer.sh"
        # self.kafka_producer_cmd = "bash /home/ec2-user/kafka_2.12-2.2.1/bin/kafka-console-producer.sh"
        # self.kafka_cmd = "bash /home/ec2-user/kafka_2.12-2.2.1/bin/kafka-topics.sh"
        self.consumer_offset = 0
        self.key = None
        self.client = None
        # self.ec2_Instance_Mgm = Ec2InstanceMgm()
        logging.basicConfig(format='%(asctime)s-%(process)d-%(levelname)s-%(message)s')
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self.clientConMgm = clientConnectMgm(self.logger)

    def __check_and_get_cluster_details(self, cluster_name, message):
        if self.cluster_name != cluster_name:
            cluster_details_res = self.__check_cluster_details_exits(cluster_name)
            if cluster_details_res is not None:
                self.cluster_name = cluster_name
                self.cluster_name_arn = cluster_details_res['ClusterArn']
                self.broker_list = cluster_details_res['BrokerList']
                self.zookeeper_list = cluster_details_res['ZookeeperList']
                self.partition_count = 3
            else:
                self.logger.error(f"Don't have cluster inform. {message}")
                return False
        return True

    def create_cloudwatch_log_group(self, clustername):
        try:
            log_group_name = clustername + "_kafka_aws_log_group"
            response = self.cloudwatch_log.describe_log_groups(
                logGroupNamePrefix=log_group_name,
                limit=10
            )
            if (response['ResponseMetadata']['HTTPStatusCode'] == 200) and (len(response['logGroups']) != 0):
                self.logger.info(f"Log group {response['logGroups'][0]['logGroupName']} is already exist")
                return log_group_name
            else:
                self.logger.info(f"Log group {log_group_name} doesn't exist. will create")
                response = self.cloudwatch_log.create_log_group(
                    logGroupName=log_group_name,
                    # kmsKeyId='string',
                    tags={
                        'name': 'kafka log group'
                    }
                )
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    self.logger.info(f"Log group {log_group_name} gor created successfully")
                    return log_group_name
        except botocore.exceptions.ClientError as cle:
            self.logger.error(
                "Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))
            return None

    def describe_broker_list(self, cluster_arn_id):
        try:
            broker_list_response = self.kafka_client.get_bootstrap_brokers(ClusterArn=cluster_arn_id)
            if broker_list_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return broker_list_response['BootstrapBrokerStringTls'].split(',')
        except botocore.exceptions.ClientError as cle:
            logging.error("Error is {} due to {}".format(cle.response['Error']['Code'],
                                                         cle.response['Error']['Message']))
            return None

    def describe_all_instance(self):
        try:
            response = self.kafka_client.list_clusters(
                MaxResults=10
            )
            ec2_state = None
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                # print("response", json.dumps(response['ClusterInfoList'], indent=1))
                for itr in response['ClusterInfoList']:
                    self.cluster_name_arn.append(
                        {'ClusterName': itr['ClusterName'],
                         'ClusterArn': itr['ClusterArn'],
                         'BrokerList': self.describe_broker_list(itr['ClusterArn']),
                         'ZookeeperList': itr['ZookeeperConnectString'].split(',')}
                    )
                if len(response['ClusterInfoList']) != 0:
                    self.zookeeper_list = response['ClusterInfoList'][0]['ZookeeperConnectString']
                    self.logger.info(f'Broker list {self.broker_list}')
                    self.logger.info(f'Zookeeper list {self.zookeeper_list} \n')

        except botocore.exceptions.ClientError as cle:
            self.logger.error(
                "Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))

    def describe_configuration(self):
        try:
            response = self.kafka_client.list_configurations()
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.logger.info(response['Configurations'][0]['Arn'])
                return response['Configurations'][0]['Arn']
        except botocore.exceptions.ClientError as cle:
            self.logger.error(cle)
            self.logger.error("Error is {} due to {}".format(cle.response['Error']['Code'],
                                                             cle.response['Error']['Message']))
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

    def __check_cluster_details_exits(self, cluster_name):
        cluster_details = None
        if not self.cluster_name_arn:
            self.describe_all_instance()
        for itr in self.cluster_name_arn:
            # self.logger.info(itr)
            if itr['ClusterName'] == cluster_name:
                cluster_details = itr
        return cluster_details

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
            self.logger.error("Generic error happened ", json.dumps(cle.response, indent=1))
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
            self.logger.error("Generic error happened ", json.dumps(cle.response, indent=1))
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
        cluster_config_name = cluster_name + "-kafka-config"
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

            print(cluster_config_name)
            response = self.kafka_client.create_configuration(
                Name=cluster_config_name,
                Description='The configuration to use on all own clusters.',
                KafkaVersions=['2.2.1'],
                ServerProperties=server_properties
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
            self.logger.info(security_group_id)
            if security_group_id is None:
                # Create a new security group
                security_group_id = self.__create_security_groups_details(security_group_name, vpc_details)
                # Update the incoming and outgoing rule of new security group
                if security_group_id is not None:
                    self.__update_security_group(security_group_name, security_group_id)
            else:
                self.logger.info(
                    f"The security group {security_group_name} is already exists. No need to create a new one")
            # print("subnet_list  {0}".format(subnet_list))
            subnet_list_local = []
            for i in range(len(subnet_list)):
                subnet_list_local.append(subnet_list[i]['subnetid'])
            self.logger.info(subnet_list_local)

            kafka_config_cluster_arn = self.describe_configuration()
            response = self.kafka_client.create_cluster(
                ClusterName=cluster_name,
                KafkaVersion='2.2.1',
                NumberOfBrokerNodes=3,
                BrokerNodeGroupInfo={
                    'BrokerAZDistribution': 'DEFAULT',
                    'ClientSubnets': [subnet_list_local[0], subnet_list_local[2], subnet_list_local[3]],
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
                    if status_response['ClusterInfo']['State'] == 'ACTIVE':
                        self.logger.info(f"The instance with instance id  {response['ClusterArn']} is state ACTIVE")
                        broker_list = []
                        broker_list_response = self.kafka_client.get_bootstrap_brokers(
                            ClusterArn=response['ClusterArn'])
                        if broker_list_response is not None:
                            broker_list = broker_list_response['BootstrapBrokerStringTls'].split(',')
                        self.cluster_name_arn.append(
                            {'ClusterName': cluster_name,
                             'ClusterArn': response['ClusterArn'],
                             'BrokerList': broker_list,
                             'ZookeeperList': status_response['ClusterInfo']['ZookeeperConnectString'].split(',')
                             })
                        break
                    self.logger.info("waiting for 40 seconds")
                    time.sleep(40)
        except botocore.exceptions.ClientError as cle:
            print("Generic error happened ", json.dumps(cle.response, indent=1))
            self.logger.error(f"Error is {cle.response['Error']['Code']} due to {cle.response['Error']['Message']}")
        except botocore.exceptions.ParamValidationError as parval:
            self.logger.error("parameter validation error happened ", parval)

    def delete_cluster(self, cluster_name, delete_log_group='N'):
        try:
            response = ""
            count = 0
            if self.__check_and_get_cluster_details(cluster_name, "Cluster not found"):
                response = self.kafka_client.delete_cluster(
                    ClusterArn=self.cluster_name_arn)
                ec2_state = None
                print(response)
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    clusterArn = response['ClusterArn']
                    while True:
                        response = self.describe_particular_instance(clusterArn)
                        if response == 'DELETING':
                            print(
                                'The instance with instance id {} is state {}'.format(itr['ClusterArn'], response))
                            if delete_log_group == 'Y':
                                log_group_name = cluster_name + "_kafka_aws_log_group"
                                print("delete the log group {}".format(log_group_name))
                                response_log_group = self.cloudwatch_log.delete_log_group(logGroupName=log_group_name)
                                if response_log_group['ResponseMetadata']['HTTPStatusCode'] == 200:
                                    print("Log group {} gor deleted successfully".format(log_group_name))
                                break
                        else:
                            count = count + 1
                        print("waiting for 5 seconds")
                        time.sleep(5)
            self.cluster_name_arn = None
        except botocore.exceptions.ClientError as cle:
            print("Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))
            # print("Generic error happened ", json.dumps(cle.response, indent=1))

    # def connect_to_ec2_node(self, instance_ip, cmd=None):
    #     # print("{} Inside connect_to_ec2_node wit ip {} and cmd {}".format(datetime.now(), instance_ip, cmd))
    #     # Connect/ssh to an instance
    #     try:
    #         if self.client is None:
    #             # self.key = paramiko.RSAKey.from_private_key_file("D:/pycherm_project/sample_file/MSKKeyPairNew.pem")
    #             self.key = paramiko.RSAKey.from_private_key_file(self.node_private_key)
    #             self.client = paramiko.SSHClient()
    #             self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    #             self.client.connect(hostname=instance_ip, username="ec2-user", pkey=self.key)
    #
    #         # Here 'ubuntu' is user name and 'instance_ip' is public IP of EC2
    #         # print("{} Inside connect_to_ec2_node..client created".format(datetime.now()))
    #         # self.client.connect(hostname=instance_ip, username="ec2-user", pkey=key)
    #         if cmd is None:
    #             cmd = 'pwd'
    #
    #         # Execute a command(cmd) after connecting/ssh to an instance
    #         # print("{} Inside connect_to_ec2_node..Going to execute command".format(datetime.now()))
    #         stdin, stdout, stderr = self.client.exec_command(cmd)
    #         # print(stderr.read())
    #         # stdin, stdout, stderr = self.client.exec_command("cat /home/ec2-user/test_txt")
    #         # stdin,
    #         # stdin.flush()
    #         # print("{} Inside connect_to_ec2_node..Starting cntl c data".format(datetime.now()))
    #         # self.logger.info("{} Inside connect_to_ec2_node..Starting to  the data".format(datetime.now()))
    #         # print(stdout.read())
    #         data = stdout.read().splitlines()
    #         self.logger.info(data)
    #         # print("{} Inside connect_to_ec2_node..waiting for the data".format(datetime.now()))
    #         # #
    #         # for line in data:
    #         #     x = line.decode()
    #         #     # print(line.decode())
    #         #     print(x)
    #         # close the client connection once the job is done
    #
    #         # client.close()
    #         # print("{} Inside connect_to_ec2_node..Closing the client".format(datetime.now()))
    #         return len(data)
    #     except Exception as e:
    #         self.logger.error(e)

    def create_topic_client(self, cluster_name, topic_name):
        # check whether cluster_name valid of not
        if self.__check_and_get_cluster_details(cluster_name, "Not able create topic"):

            # Check the client node is up or not
            # self.ec2_Instance_Mgm.
            create_cmd = " --create"
            zookeeper_id = " --zookeeper " + self.zookeeper_list[0]
            replicate_partition = " --replication-factor 1 --partitions 3"
            topic_name = " --topic " + topic_name
            topic_create_command = self.kafka_cmd + create_cmd + zookeeper_id + replicate_partition + topic_name
            self.clientConMgm.connect_to_ec2_node('35.161.163.0', self.node_private_key, topic_create_command)
            if self.client is not None:
                self.client.close()

    def consumer_client(self, cluster_name, topic_name):
        consumer_command = None
        if self.__check_and_get_cluster_details(cluster_name, "Not able to consume messages"):
            broker_id = " --bootstrap-server  " + self.broker_list[0]
            # consumer_config = " --consumer.config /home/ec2-user/kafka_2.12-2.2.1/bin/client.properties"
            consumer_config = " --consumer.config " + self.kafka_client_path
            topic_name = " --topic " + topic_name
            # offset_point = " --from-beginning"
            # offset_point = " --offset"
            # + str(self.consumer_offset)
            skip_message_on_error = " -skip-message-on-error"
            timeout_ms = " --timeout-ms 10000"
            number_of_message = " --max-messages 10"
            partition_details = " --partition "
            consumer_command = self.kafka_consumer_cmd + broker_id + consumer_config + topic_name
            consumer_command = consumer_command + skip_message_on_error + timeout_ms + number_of_message
            consumer_command = consumer_command + partition_details
            # producer_command = "bash /home/ec2-user/kafka_2.12-2.2.1/bin/test_script.sh"
            # producer_command = "cat /home/ec2-user/kafka_2.12-2.2.1/bin/test_txt"
            self.logger.info(f"consumer_client {consumer_command}")

            return consumer_command

        # for i in range(0, 2):
        # while True:
        #     # abc = consumer_command + str(self.consumer_offset)
        #     self.connect_to_ec2_node('35.161.163.0', consumer_command)
        #     self.consumer_offset = self.consumer_offset + 10
        #     # print("Inside consumer_client {}".format(i))
        #     # time.sleep(1)
        # self.logger.info("Exiting consumer_client")

    def producer_client(self, cluster_name, topic_name):
        if self.__check_and_get_cluster_details(cluster_name, "Not able to produce messages"):
            broker_id = " --broker-list " + self.broker_list[0]
            # producer_config = " --producer.config /home/ec2-user/kafka_2.12-2.2.1/bin/client.properties"
            producer_config = " --producer.config " + self.kafka_client_path
            topic_name = " --topic " + topic_name
            for i in range(101, 200):
                message = "echo \"{num.replica.fetchers=" + str(i) + "}\" |"
                producer_command = message + self.kafka_producer_cmd + broker_id + producer_config + topic_name
                self.logger.info(f"producer_client== {producer_command}")
                self.clientConMgm.connect_to_ec2_node('35.161.163.0', self.node_private_key, producer_command)
                time.sleep(3)
            # if self.client is not None:
            #     self.client.close()
            self.logger.info("Exiting producer_client")

    def receive_signal(self, signal_number, frame):
        self.logger.info(f"Received:  {signal_number}")
        collected = gc.collect()
        self.logger.info("Garbage collector: collected %d objects." % collected)
        return


#
# class client_thread(threading.Thread):
#     def __init__(self, threadID, name, cmd):
#         threading.Thread.__init__(self)
#         self.threadID = threadID
#         self.name = name
#         self.cmd = cmd
#         self.kafkaclusterObj = KafkaClusterMgm()
#         logging.basicConfig(format='%(asctime)s-%(process)d-%(levelname)s-%(message)s')
#         self.logger = logging.getLogger()
#         self.logger.setLevel(logging.INFO)
#
#     def run(self):
#         self.logger.info(f"Running thread name {self.name} Id {self.threadID} ")
#         consumer_offset = 0
#         offset_point = " --offset "
#         self.cmd = self.cmd + offset_point
#         while True:
#             consumer_offset = self.kafkaclusterObj.connect_to_ec2_node('35.161.163.0', self.cmd + str(
#                 self.kafkaclusterObj.consumer_offset))
#             self.kafkaclusterObj.consumer_offset += consumer_offset
#             self.logger.info(
#                 f"Running thread Id {self.threadID} and consumer_offset = {self.kafkaclusterObj.consumer_offset}")
#             # self.connect_to_ec2_node('35.161.163.0', topic_create_command)
#             # print("Inside consumer_client {}".format(i))
#             # time.sleep(1)
#             # time.sleep(3)
#             # counter += 1
#             # if counter > 5:
#             #     break
#         self.logger.info("Exiting consumer_client")


class clientConnectMgm:
    def __init__(self, logger):
        self.key = None
        self.client = None
        self.logger = logger

    def connect_to_ec2_node(self, instance_ip, node_private_key, cmd=None):
        print("{} Inside connect_to_ec2_node wit ip {} and cmd {}".format(datetime.now(), instance_ip, cmd))
        # Connect/ssh to an instance
        try:
            if self.client is None:
                self.key = paramiko.RSAKey.from_private_key_file(node_private_key)
                self.client = paramiko.SSHClient()
                self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                self.client.connect(hostname=instance_ip, username="ec2-user", pkey=self.key)

            # Here 'ubuntu' is user name and 'instance_ip' is public IP of EC2
            # print("{} Inside connect_to_ec2_node..client created".format(datetime.now()))
            # self.client.connect(hostname=instance_ip, username="ec2-user", pkey=key)
            if cmd is None:
                cmd = 'pwd'

            # Execute a command(cmd) after connecting/ssh to an instance
            # print("{} Inside connect_to_ec2_node..Going to execute command".format(datetime.now()))
            stdin, stdout, stderr = self.client.exec_command(cmd)
            # print(stderr.read())
            # stdin, stdout, stderr = self.client.exec_command("cat /home/ec2-user/test_txt")
            # stdin,
            # stdin.flush()
            # print("{} Inside connect_to_ec2_node..Starting cntl c data".format(datetime.now()))
            # self.logger.info("{} Inside connect_to_ec2_node..Starting to  the data".format(datetime.now()))
            # print(stdout.read())
            data = stdout.read().splitlines()
            self.logger.info(data)
            print(data)
            # print("{} Inside connect_to_ec2_node..waiting for the data".format(datetime.now()))
            # #
            # for line in data:
            #     x = line.decode()
            #     # print(line.decode())
            #     print(x)
            # close the client connection once the job is done

            # client.close()
            # print("{} Inside connect_to_ec2_node..Closing the client".format(datetime.now()))
            return len(data)
        except Exception as e:
            self.logger.error(e)


class client_worker(Process):
    def __init__(self, name, cmd, node_private_key, parent_logger):
        super(client_worker, self).__init__()
        self.name = name
        self.cmd = cmd
        self.consumer_offset = 0
        self.node_private_key = node_private_key
        # logging.basicConfig(format='%(asctime)s-%(process)d-%(levelname)s-%(message)s')
        self.logger = parent_logger
        # self.logger.setLevel(logging.INFO)
        self.clientConMgm = clientConnectMgm(self.logger)
        self.logger.info(f"client_worker {self.name}")

    def run(self):
        self.logger.info(f"Running process name {self.name} with process id  {os.getpid()}")
        new_offset = 1
        offset_point = " --offset "
        self.cmd = self.cmd + offset_point
        try:
            while True:
                new_offset = self.clientConMgm.connect_to_ec2_node('35.161.163.0', self.node_private_key,
                                                                        self.cmd + str(self.consumer_offset))
                # new_offset = self.clientConMgm.connect_to_ec2_node('35.161.163.0', self.node_private_key)
                print("client_worker {} and  offset {} with process id  {}".format(self.name, new_offset,os.getpid()))
                self.consumer_offset += new_offset
                self.logger.info(
                    f"Running process name {self.name} and consumer_offset = {self.consumer_offset}")
        except botocore.exceptions.ClientError as cle:
            self.logger.error(
                "Error is {} due to {}".format(cle.response['Error']['Code'], cle.response['Error']['Message']))
            return None


class Worker(Process):
    def __init__(self):
        super(Worker, self).__init__()
        print('In %s' % self.name)

    def run(self):
        while True:
            print('In KD--- %s', self.name)
            print('Waiting...', os.getpid())
            time.sleep(3)
        return


def main():
    kafkaCluster = KafkaClusterMgm()
    signal.signal(signal.SIGINT, kafkaCluster.receive_signal)

    while True:
        operation = input("Enter the type of operation u want to do new:-")
        if operation == 'list':
            kafkaCluster.describe_all_instance()
        elif operation == "new":
            cluster_name = input("Please provide the kafka cluster name you want to create")
            print(cluster_name.find('_'))
            if cluster_name.find('_') < 0:
                now = datetime.now()
                kafkaCluster.logger.info(f"Operation start time {now.strftime('%Y-%m-%d %H:%M:%S %f')}")
                log_group_name = kafkaCluster.create_cloudwatch_log_group(cluster_name)
                if log_group_name is not None:
                    # Create cluster configuration
                    # kafkaCluster.create_kafka_cluster_config(cluster_name)
                    kafkaCluster.create_kafka_cluster(cluster_name, log_group_name)
                    now = datetime.now()
                    kafkaCluster.logger.info(f"Operation end time {now.strftime('%Y-%m-%d %H:%M:%S %f')}")
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
            kafkaCluster.clientConMgm.connect_to_ec2_node(node_ip, kafkaCluster.node_private_key)
        elif operation == "exit":
            break
        elif operation == "read":
            # jobs = []
            # for i in range(5):
            #     p = Worker()
            #     jobs.append(p)
            #
            # for i in range(5):
            #     jobs[i].start()
            #     # p.start()
            # for j in jobs:
            #     j.join()

            cluster_name, topic_name = input(
                "Please provide the cluster and topic name to read separated by space:-").split()
            consumer_command = kafkaCluster.consumer_client(cluster_name, topic_name)
            # consumer_command = 'test'
            if consumer_command is not None:
                jobs = []
                for itr in range(0, kafkaCluster.partition_count):
                    jobs.append(client_worker("Process-" + str(itr), consumer_command + str(itr),
                                              kafkaCluster.node_private_key, kafkaCluster.logger))
                    # jobs.append(Worker())
                    # p = Worker()
                    # jobs.append(p)
                    # p.start()
                #
                #     jobs.append(Worker())
                for itr in range(0, kafkaCluster.partition_count):
                    jobs[itr].start()
                for itr in range(0, kafkaCluster.partition_count):
                    jobs[itr].join()

        # for itr in range(0, kafkaCluster.partition_count):
        #     threads.append(client_thread(itr, "Thread-" + str(itr), consumer_command + str(itr)))
        #
        # for itr in range(0, kafkaCluster.partition_count):
        #     threads[itr].start()
        #
        # for itr in range(0, kafkaCluster.partition_count):
        #     threads[itr].join()
        elif operation == "write":
            cluster_name, topic_name = input(
                "Please provide the cluster and topic name to write separated by space:-").split()
            kafkaCluster.producer_client(cluster_name, topic_name)
        elif operation == "topic":
            cluster_name, topic_name = input(
                "Please provide the cluster and topic name to create separated by space:-").split()
            kafkaCluster.create_topic_client(cluster_name, topic_name)
        elif operation == 'ctest':
            instance_ip = input("Please provide the public ip of the ec2 node:-")
            kafkaCluster.connect_to_ec2_node(instance_ip)


if __name__ == "__main__":
    main()
    # jobs = []
    # for i in range(5):
    #     p = Worker()
    #     jobs.append(p)
    #
    # for i in range(5):
    #     jobs[i].start()
    #     # p.start()
    # for j in jobs:
    #     j.join()

"""
Readme
aws kafka describe-cluster --region us-west-2 --cluster-arn "arn:aws:kafka:us-west-2:534462043435:cluster/kd-kafka-cluster/93d1720b-5d37-4a33-a62f-7fb826d7ae9a-3"

bin/kafka-topics.sh --create --zookeeper z-3.kd-k-cluster.hlxot0.c3.kafka.us-west-2.amazonaws.com:2181 --replication-factor 1 --partitions 3 --topic AWSKafkaTopic
bin/kafka-topics.sh --create --zookeeper z-2.kd-new-kafka.rccmy6.c3.kafka.us-west-2.amazonaws.com:2181 --replication-factor 1 --partitions 3 --topicAWSKafkaTopic

aws kafka get-bootstrap-brokers --region us-west-2 --cluster-arn "arn:aws:kafka:us-west-2:534462043435:cluster/kd-new-kafka-cs/e06dcf4e-0ffb-464f-97cf-067bed87d93c-3"

bash kafka-topics.sh --zookeeper z-1.kafka-new-kd-clus.iebp33.c3.kafka.us-west-2.amazonaws.com:2181 --topic AWSKafkaTopic --describe
Topic:AWSKafkaTopicPartitionCount:3ReplicationFactor:1Configs:
Topic: AWSKafkaTopicPartition: 0Leader: 3Replicas: 3Isr: 3
Topic: AWSKafkaTopicPartition: 1Leader: 1Replicas: 1Isr: 1
Topic: AWSKafkaTopicPartition: 2Leader: 2Replicas: 2Isr: 2
[ec2-user@ip-172-31-17-187 bin]$ 



bash kafka-console-producer.sh --broker-list b-1.kd-new-cluster.4jg3y2.c3.kafka.us-west-2.amazonaws.com:9094 --producer.config client.properties --topic AWSKafkaTopic

bash kafka-console-consumer.sh --bootstrap-server b-3.kd-new-kafka.rccmy6.c3.kafka.us-west-2.amazonaws.com:9094 --consumer.config client.properties --topic AWSKafkaTopic --from-beginning
kafka-console-consumer.sh --bootstrap-server  z-3.kd-new-kafka2.o4bf1u.c3.kafka.us-west-2.amazonaws.com:2181 --consumer.config /home/ec2-user/kafka_2.12-2.2.1/bin/client.properties --topic AWSKafkaTopic -skip-message-on-error --timeout-ms 5000 --max-messages 10
cp /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.252.b09-2.amzn2.0.1.x86_64/jre/lib/security/cacerts /tmp/kafka.client.truststore.jks
"""
