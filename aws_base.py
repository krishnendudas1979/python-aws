import os
from configparser import ConfigParser

import boto3


class aws_base:
    def __init__(self):
        configure = ConfigParser()
        configure.read('D:/pycherm_project/sample_file/config.ini')
        self.image_type = configure.get('configuration', 'image_type')
        self.instance_type = configure.get('configuration', 'instance_type')
        self.region_name = configure.get('configuration', 'region_name')
        self.IPv4_CIDR = configure.get('configuration', 'IPv4_CIDR')
        self.aws_access_key_id_env = configure.get('configuration', 'AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key_env = configure.get('configuration', 'AWS_SECRET_ACCESS_KEY')
        self.kafka_consumer_cmd = configure.get('client-node', 'kafka_consumer_cmd')
        self.kafka_producer_cmd = configure.get('client-node', 'kafka_producer_cmd')
        self.kafka_cmd = configure.get('client-node', 'kafka_cmd')
        self.  = configure.get('client-node', 'node_private_key')
        self.kafka_client_path = configure.get('client-node', 'kafka_client_path')

        self.ec2_client = boto3.client('ec2', aws_access_key_id=self.aws_access_key_id_env,
                                       aws_secret_access_key=self.aws_secret_access_key_env,
                                       region_name=self.region_name)
        self.ec2_resource = boto3.resource('ec2', aws_access_key_id=self.aws_access_key_id_env,
                                           aws_secret_access_key=self.aws_secret_access_key_env,
                                           region_name=self.region_name)
        self.cloudwatch_log = boto3.client('logs', aws_access_key_id=self.aws_access_key_id_env,
                                           aws_secret_access_key=self.aws_secret_access_key_env,
                                           region_name=self.region_name)



