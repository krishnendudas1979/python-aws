import boto3
import json
import os
import botocore
import random
import calendar
from datetime import datetime
import time
from boto import exception
from boto.logs.exceptions import ServiceUnavailableException, ResourceNotFoundException
from os import path
import datetime

# bucket_name = input("Enter s3 Bucket  name : ")
from aws_base import aws_base

operation = input("Enter the type of operation u want to do for ec2:-")

class Ec2InstanceMgm(aws_base):
    def __init__(self):
        super().__init__()

    def describe_all_instance(self, ec2_instance_name=None):
        response = self.ec2_client.describe_instances()
        ec2_state = None
        # print(response)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            for itr in response['Reservations']:
                for sub_itr in itr['Instances']:
                    if 'State' in sub_itr:
                        ec2_state = sub_itr['State']['Name']
                    if 'Tags' in sub_itr:
                        if sub_itr['Tags'][0]['Key'].lower() == 'name':
                            print(
                                'Instance name {0} with instance id as {1} is in state {2}. The image type is {3} '
                                'and instance type {4}'.format(sub_itr['Tags'][0]['Value'],
                                                               sub_itr['InstanceId'],
                                                               ec2_state,
                                                               sub_itr['ImageId'],
                                                               sub_itr['InstanceType']))
                                                               # sub_itr['PublicIpAddress']))

                            if sub_itr['Tags'][0]['Value'] == ec2_instance_name and ec2_state == 'stopped':
                                return sub_itr['InstanceId']
        return None

    # def describe_particular_instance(self, ec2_instance_name):
    #     response = self.ec2_client.describe_instances(InstanceIds=[ec2_instance_ids])
    #     ec2_state = None
    #     print(response)
    #     if response['ResponseMetadata']['HTTPStatusCode'] == 200:
    #         for itr in response['Reservations']:
    #             for sub_itr in itr['Instances']:
    #                 if 'State' in sub_itr:
    #                     ec2_state = sub_itr['State']['Name']
    #     return ec2_state

    def describe_particular_instance(self, ec2_instance_ids):
        response = self.ec2_client.describe_instances(InstanceIds=[ec2_instance_ids])
        ec2_state = None
        print(response)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            for itr in response['Reservations']:
                for sub_itr in itr['Instances']:
                    if 'State' in sub_itr:
                        ec2_state = sub_itr['State']['Name']
        return ec2_state

    def start_a_node(self, node_name):
        try:
            response = self.ec2_client.start_instances(
                InstanceIds=[node_name],
                DryRun=False
            )
            ec2_state = None
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                for itr in response['StartingInstances']:
                    print(itr)
                while True:
                    response = self.describe_particular_instance(itr['InstanceId'])
                    print(response)
                    if response == 'running':
                        print('The instance with instance id is state {}'.format(itr['InstanceId'], response))
                        break
                    print("waiting for 5 seconds")
                    time.sleep(5)
        except botocore.exceptions.ClientError as cle:
            print("Generic error happened ", json.dumps(cle.response, indent=1))

    def stop_a_node(self, node_name):
        try:
            response = self.ec2_client.stop_instances(
                InstanceIds=[node_name],
                DryRun=False
            )
            ec2_state = None
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                for itr in response['StoppingInstances']:
                    print(itr)
                while True:
                    response = self.describe_particular_instance(itr['InstanceId'])
                    if response == 'stopped':
                        print('The instance with instance id is state {}'.format(itr['InstanceId'], response))
                        break
                    print("waiting for 5 seconds")
                    time.sleep(5)
        except botocore.exceptions.ClientError as cle:
            print("Generic error happened ", json.dumps(cle.response, indent=1))

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

    def create_node(self, number_of_instance):
        try:
            security_group_id = None
            # get the VPC details
            vpc_details = self.__get_vpc_details(self.IPv4_CIDR)
            # get the list of subnet available under that VPC
            subnet_list = self.__get_subnet_details(vpc_details)

            # Check and create a security group (if not exist)
            security_group_name = "ec2-security-group" + self.region_name
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

            # client = boto3.client('pricing')
            i = 0
            if subnet_list is not None:
                for i in range(int(number_of_instance)):
                    # print(subnet_list[i]['subnetid'])
                    # print(subnet_list[i]['azoneid'])
                    try:
                        response = self.ec2_resource.create_instances(
                            BlockDeviceMappings=[
                                {
                                    'DeviceName': '/dev/xvdh',
                                    'VirtualName': 'ephemeral0',
                                    'Ebs': {
                                        'DeleteOnTermination': True,
                                        # 'Iops': 16000,
                                        'SnapshotId': 'snap-0bd651adad3ef7140',
                                        'VolumeSize': 8,
                                        'VolumeType': 'gp2',
                                        # 'KmsKeyId': 'string',
                                        'Encrypted': False
                                    }
                                }
                            ],
                            ImageId=self.image_type,
                            InstanceType=self.instance_type,
                            # KeyName='ec2-keypair',
                            MinCount=1,
                            MaxCount=1,
                            Monitoring={
                                'Enabled': True
                            },
                            Placement={
                                'AvailabilityZone': subnet_list[i]['azoneid'],
                                'Tenancy': 'default'
                            },
                            SecurityGroupIds=[security_group_id],
                            SubnetId=subnet_list[i]['subnetid'],
                            # VpcId=vpc_details,
                            TagSpecifications=[
                                {
                                    'ResourceType': 'instance',
                                    'Tags': [
                                        {
                                            'Key': 'Name',
                                            'Value': 'ec2_instance_python3.7'
                                        }
                                    ]

                                }
                            ]
                        )
                        print(response)
                        for itr in response:
                            while True:
                                if self.describe_particular_instance(itr.id) == 'running':
                                    print('The instance with instance id is state {}'.format(itr.id, response))
                                    break
                            print("waiting for 15 seconds")
                            time.sleep(15)
                    except botocore.exceptions.ClientError as cle:
                        print("Generic error happened ", json.dumps(cle.response, indent=1))
        except botocore.exceptions.ClientError as cle:
            print("Generic error happened ", json.dumps(cle.response, indent=1))
        except botocore.exceptions.ParamValidationError as parval:
            print("parameter validation error happened ", parval)

    def drop_node(self, node_name):
        try:
            response = self.ec2_client.terminate_instances(
                InstanceIds=[node_name],
                DryRun=False
            )
            ec2_state = None
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                for itr in response['TerminatingInstances']:
                    print(itr)
                while True:
                    response = describe_particular_instance(itr['InstanceId'])
                    if response == 'terminated':
                        print('The instance with instance id is state {}'.format(itr['InstanceId'], response))
                        break
                    print("waiting for 5 seconds")
                    time.sleep(5)
        except botocore.exceptions.ClientError as cle:
            print("Generic error happened ", json.dumps(cle.response, indent=1))


def main():
    ec2inst = Ec2InstanceMgm()

    if operation == 'd':
        ec2inst.describe_all_instance()
    elif operation == 'start':
        node_name = input("Please provide the instance id  : -")
        ec2inst.start_a_node(node_name)
    elif operation == 'stop':
        node_name = input("Please provide the instance id  : -")
        ec2inst.stop_a_node(node_name)
    elif operation == "create":
        number_of_instance = input("Please provide the number of instance you want to create")
        ec2inst.create_node(number_of_instance)
    elif operation == "drop":
        node_name = input("Please provide the instance id to drop : -")
        ec2inst.drop_node(node_name)


if __name__ == "__main__":
    main()
