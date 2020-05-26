import os

import boto3

os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAJSKKKXBPD6OEF6PA'
os.environ['AWS_SECRET_ACCESS_KEY'] = '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'
env_var = os.environ
aws_access_key_id_env = env_var['AWS_ACCESS_KEY_ID']
aws_secret_access_key_env = env_var['AWS_SECRET_ACCESS_KEY']


class aws_base:
    def __init__(self):
        self.image_type = 'ami-02d0ea44ae3fe9561'
        self.instance_type = 't2.micro'
        self.region_name = 'us-west-2'
        self.IPv4_CIDR = '172.31.0.0/16'
        self.ec2_client = boto3.client('ec2', aws_access_key_id=aws_access_key_id_env,
                                       aws_secret_access_key=aws_secret_access_key_env, region_name=self.region_name)
        self.ec2_resource = boto3.resource('ec2', aws_access_key_id=aws_access_key_id_env,
                                           aws_secret_access_key=aws_secret_access_key_env,
                                           region_name=self.region_name)
        self.cloudwatch_log = boto3.client('logs', aws_access_key_id=aws_access_key_id_env,
                                           aws_secret_access_key=aws_secret_access_key_env,
                                           region_name=self.region_name)



