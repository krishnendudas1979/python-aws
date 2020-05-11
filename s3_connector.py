import boto3
import json
import os
import botocore

bucket_name = input("Enter s3 Bucket  name : ")
#myawsbucket1979
#portal-bucket-source
env_var = os.environ
os.environ['AWS_ACCESS_KEY_ID']='AKIAIM7FUUWPXTWFFQ5A'
os.environ['AWS_SECRET_ACCESS_KEY']='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'

aws_access_key_id_env= env_var['AWS_ACCESS_KEY_ID']
aws_secret_access_key_env=env_var['AWS_SECRET_ACCESS_KEY']

client = boto3.client('s3', aws_access_key_id=aws_access_key_id_env, aws_secret_access_key=aws_secret_access_key_env)
try:
    response = client.get_bucket_encryption(Bucket=bucket_name)
    json_res = json.dumps(response)
    # print(json_res)

    # s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id_env, aws_secret_access_key=aws_secret_access_key_env)
    # bucket = s3.Bucket(bucket_name)
    # for obj in bucket.objects.all():
    #     print(obj.key)
    keys = ["Versions", "DeleteMarkers"]
    object_response = client.list_object_versions(Bucket=bucket_name)
    print(type(object_response))
    for k in keys:
        if k in object_response:
            for itr in object_response[k]:
                print(type(itr))
                version = itr['VersionId']
                file = itr['Key']
                print('File name {0} is in version {1}'.format(file, version))
                client.delete_object(Bucket=bucket_name, Key=file, VersionId=version)




except botocore.exceptions.ClientError as e:
    error_code = e.response['Error']['Code']
    print("error", e)
    if error_code == '404':
        print("Bucket" + bucket_name + "doesn't exists")
# print(client.get_bucket_policy(Bucket=bucket_name))
