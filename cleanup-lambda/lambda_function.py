from datetime import datetime
from dateutil.tz import tzlocal

import boto3

CLEANUP_THRESHOLD_IN_SECONDS = 86400  # 24 hours
BUCKET_NAME = "dlc-test-tickets"
FOLDER_NAME = "resource_pool/"


def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    list_objects_response = s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=FOLDER_NAME)

    # Scan for outdated resource pool entries
    deletion_list = []
    for file in list_objects_response["Contents"]:
        if file["Key"].endswith(".json"):
            last_modified_time = file["LastModified"]
            total_seconds_passed = (datetime.now(tzlocal()) - last_modified_time).total_seconds()
            if total_seconds_passed >= CLEANUP_THRESHOLD_IN_SECONDS:
                    deletion_list.append({"Key": file["Key"]})

    # Delete outdated resource pool entries
    if len(deletion_list) != 0:
        deletion_response = s3_client.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": deletion_list, "Quiet":False})
        print(deletion_response)

    print(f"Deleted {str(len(deletion_list))} resource pool tickets.")