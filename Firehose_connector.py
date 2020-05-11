import boto3
import json
import os
import botocore
import random
import calendar
from datetime import datetime
import time
from boto.logs.exceptions import ServiceUnavailableException, ResourceNotFoundException
from os import path
import datetime

firehose_name = 'test_fire_hose'
#input("Enter firehose name : ")
file_name = '0.96MB.txt'
#input("Enter file name  :")
batch_or_single_mode = input("Enter batch mode or single b/s  :")
file_name_with_path = 'd:\\pycherm_project\\Sample_file\\' + file_name
os.environ['AWS_ACCESS_KEY_ID']='AKIAIM7FUUWPXTWFFQ5A'
os.environ['AWS_SECRET_ACCESS_KEY']='hzpVbn4x1u99uyQOvJ7NTyg998h85l2/vfYoEWUw'

env_var = os.environ
aws_access_key_id_env = env_var['AWS_ACCESS_KEY_ID']
aws_secret_access_key_env = env_var['AWS_SECRET_ACCESS_KEY']

fire_hose_client = boto3.client('firehose', aws_access_key_id=aws_access_key_id_env,
                                aws_secret_access_key=aws_secret_access_key_env, region_name='eu-central-1')
try:
    fire_hose_list = fire_hose_client.list_delivery_streams(Limit=10, ExclusiveStartDeliveryStreamName='string')
    json_res = json.dumps(fire_hose_list, indent=1)
    # print(json_res)

except botocore.exceptions.ClientError as e:
    error_code = e.response['Error']['Code']
    print("error", e)
    if error_code == '404':
        print("Bucket" + bucket_name + "doesn't exists")


def put_data_into_firehose():
    try:
        print(file_name_with_path)
        # now = datetime.datetime.now()
        # print(now.strftime('%Y-%m-%d %H:%M:%S %f'))
        if path.exists(file_name_with_path):
            with open(file_name_with_path, 'r') as reader:
                payload = reader.read()
            new_payload = json.loads(payload)
            print('payload size is in bytes {} or {}', len(payload.encode('utf-8')),
                  os.path.getsize(file_name_with_path) / 1024)
            now = datetime.datetime.now()
            new_payload['op_new_ts'] = now.strftime('%Y-%m-%d %H:%M:%S %f')
            print(new_payload['op_new_ts'])
            response = fire_hose_client.put_record(
                DeliveryStreamName=firehose_name,
                Record={
                    'Data': json.dumps(new_payload).encode('utf-8')
                }
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(json.dumps(response, indent=1))
                now = datetime.datetime.now()
                print(now.strftime('%Y-%m-%d %H:%M:%S %f'))
            else:
                print("Error happened")
        else:
            print("file {} does not exist", "1.000058MB")

    except ServiceUnavailableException as sue:
        print("ServiceUnavailableException error happened", e.response)
    except botocore.exceptions.ClientError as cle:
        print("Generic error happened ", json.dumps(cle.response, indent=1))


def put_data_batch_into_firehose(putrecordbatchsize, putrecordbatchsizeinbytes):
    try:
        payload_batch = []
        if path.exists(file_name_with_path):
            with open(file_name_with_path, 'r') as reader:
                payload = reader.read()
            new_payload = json.loads(payload)
            print('payload size is in bytes {} or {}', len(payload.encode('utf-8')),
                  os.path.getsize(file_name_with_path) / 1024)
            now = datetime.datetime.now()
            new_payload['op_new_ts'] = now.strftime('%Y-%m-%d %H:%M:%S %f')
            print(new_payload['op_new_ts'])
            count = 0
            batchsizeinbytes = 0

            while (count < putrecordbatchsize) and (batchsizeinbytes < putrecordbatchsizeinbytes):
                sink_record = {'Data': json.dumps(new_payload).encode('utf-8')}
                # sink_record['Data'] = json.dumps(new_payload).encode('utf-8')
                # payload_batch.append({'Data': json.dumps(new_payload).encode('utf-8')})
                payload_batch.append(sink_record)
                count = count+1
                batchsizeinbytes = batchsizeinbytes + len(json.dumps(new_payload).encode('utf-8'))

            print(count, batchsizeinbytes)

            response = fire_hose_client.put_record_batch(
                DeliveryStreamName=firehose_name,
                Records=payload_batch
            )
            now = datetime.datetime.now()
            print(now.strftime('%Y-%m-%d %H:%M:%S %f'))
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(json.dumps(response, indent=1))
                if response['FailedPutCount'] > 0:
                    for itr in response['RequestResponses']:
                        if 'RecordId' not in itr:
                            print(" Error code {}", itr['ErrorCode'])
                            print(" ErrorMessage {}", itr['ErrorMessage'])
        else:
            print("file {} does not exist", "1.000058MB")


    except ServiceUnavailableException as sue:
        print("ServiceUnavailableException error happened", sue.response)
    except botocore.exceptions.ClientError as cle:
        print("Generic error happened ", json.dumps(cle.response, indent=1))


def main():
    while True:
        # property_value = random.randint(40, 120)
        # property_timestamp = calendar.timegm(datetime.utcnow().timetuple())
        # thing_id = 'aa-bb'

        try:
            response = fire_hose_client.describe_delivery_stream(
                DeliveryStreamName=firehose_name)
            if (response['DeliveryStreamDescription']['DeliveryStreamStatus'] != 'ACTIVE'):
                print('Firehose {0} is in state {1}'.format(firehose_name, response['DeliveryStreamDescription']['DeliveryStreamStatus']))
            else:
                print("Firehose {0} is in state {1}".format(firehose_name, response['DeliveryStreamDescription']['DeliveryStreamStatus']))

            if batch_or_single_mode == 's':
                put_data_into_firehose()
            else:
                put_data_batch_into_firehose(500, 4000000)
            # wait for 10 second
            time.sleep(10)
        except ResourceNotFoundException as rnfe:
            print(rnfe.response)


if __name__ == "__main__":
    main()
