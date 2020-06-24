import json
import boto3

def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    response = s3_client.list_objects(Bucket="dlc-test-tickets", MaxKeys=1,
                           Prefix=f"resource_pool/")
    print(response)

    return