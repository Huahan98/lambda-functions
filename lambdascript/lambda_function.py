import json
import boto3
from datetime import datetime
import constants
import sys
from functools import cmp_to_key

def update_resource_pool(ticket_name, instance_type, num_of_instances=1):
    """
    Update the S3 resource pool for usage of SageMaker resources; status = preparing.
    Naming convention of resource usage json: ticket_name-preparing.

    :param instance_type: ml.p3.8xlarge/ml.c4.4xlarge/ml.p2.8xlarge/ml.c4.8xlarge
    :param num_of_instances: number of instances required
    """
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource('s3')

    # create the in-progress pool ticket content
    pool_ticket_content = {}
    pool_ticket_content["TICKET_NAME"] = ticket_name
    pool_ticket_content["INSTANCE_TYPE"] = instance_type
    pool_ticket_content["INSTANCES_NUM"] = num_of_instances
    pool_ticket_content["STATUS"] = "preparing"

    # create json file content and upload to S3
    filename = f"{ticket_name.split('/')[1].split('.')[0]}-preparing.json"
    s3_client.put_object(Bucket="dlc-test-tickets", Key=f"resource_pool/{instance_type}/{filename}")
    S3_ticket_object = s3_resource.Object("dlc-test-tickets", f"resource_pool/{instance_type}/{filename}")
    S3_ticket_object.put(Body=bytes(json.dumps(pool_ticket_content).encode('UTF-8')))


def assign_sagemaker_instance_type(image):
    """
    :param image: <string> ECR URI
    :return: <string> type of instance used by the image
    """
    if "tensorflow" in image:
        return "ml.p3.8xlarge" if "gpu" in image else "ml.c4.4xlarge"
    else:
        return "ml.p2.8xlarge" if "gpu" in image else "ml.c4.8xlarge"


def check_sagemaker_instance_limit(image):
    """
    Check the sagemaker instance limit that the input image uses
    :param image: <string >ECR URI
    :return: <int>
    """
    instance_type = assign_sagemaker_instance_type(image)
    if "training" in image:
        return constants.TRAINING_LIMIT[instance_type]
    elif "inference" in image:
        return constants.INFERENCE_LIMIT[instance_type]
    else:
        raise ValueError("Image not valid. Image URI must contain 'training' or 'inference'.")


def query_resources(instance_type):
    """
    Query the number of a specific instance type currently running by SageMaker

    :param instance_type: <string> type of instance required
    :param instance_num: <int> number of instances required
    :return: number of relevant instances currently in use
    """
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects(Bucket="dlc-test-tickets", Prefix=f"resource_pool/{instance_type}/")

    entries_list = []
    for entry in objects["Contents"]:
        entry_key = entry["Key"]
        if entry_key.endswith("preparing.json") or entry_key.endswith("running.json"):
            entries_list.append(entry_key)

    return len(entries_list)


def trigger_build(image_uri, context, return_sqs_url, ticket_key, instance_type, num_of_instances):
    cb_client = boto3.client("codebuild")
    build = {'projectName': "DLCTestJobExecutor", 'environmentVariablesOverride': [
        {
            "name": "PYTHONBUFFERED",
            "value": "1",
            "type": "PLAINTEXT"
        },
        {
            "name": "REGION",
            "value": "us-west-2",
            "type": "PLAINTEXT"
        },
        {
            "name": "DLC_IMAGE",
            "value": image_uri,
            "type": "PLAINTEXT"
        },
        {
            "name": "BUILD_CONTEXT",
            "value": context,
            "type": "PLAINTEXT"
        },
        {
            "name": "TEST_TYPE",
            "value": "sagemaker",
            "type": "PLAINTEXT"
        },
        {
            "name": "RETURN_SQS_URL",
            "value": return_sqs_url,
            "type": "PLAINTEXT"
        },
        {
            "name": "TICKET_NAME",
            "value": ticket_key,
            "type": "PLAINTEXT"
        },
    ]}

    update_resource_pool(ticket_key, instance_type, num_of_instances)
    build_response = cb_client.start_build(**build)
    return build_response


def delete_ticket():
    pass


def update_ticket():
    pass


def check_timeout(start_time):
    if (datetime.now()-start_time).total_seconds() >= constants.TIMEOUT_LIMIT_SECONDS:
        sys.exit()


def ticket_timestamp_cmp_function(ticket1, ticket2):
    ticket1_key, ticket2_key = ticket1["Key"], ticket2["Key"]
    ticket1_timestamp, ticket2_timestamp = datetime.strptime(ticket1_key.split('/')[1].split('.')[0].split("_")[1],
                                                             "%Y-%m-%d-%H-%M-%S") , \
                                           datetime.strptime(ticket2_key.split('/')[1].split('.')[0].split("_")[1],
                                                             "%Y-%m-%d-%H-%M-%S")
    return ticket1_timestamp > ticket2_timestamp


def lambda_handler(event, context):
    start_time = datetime.now()
    bucket_name = "dlc-test-tickets"

    s3_client = boto3.client("s3")
    response = s3_client.list_objects(Bucket=bucket_name, Prefix="request_tickets/")
    tickets_list = [ticket for ticket in response["Contents"] if ticket["Key"].endswith(".json")]
    tickets_list.sort(key=cmp_to_key(ticket_timestamp_cmp_function))


    for ticket in tickets_list:
        check_timeout(start_time)
        ticket_key = ticket["Key"]
        ticket_object = s3_client.get_object(Bucket=bucket_name, Key=ticket_key)
        ticket_body = json.loads(ticket_object["Body"].read().decode("utf-8"))

        image_uri = ticket_body["ECR-URI"]
        build_context = ticket_body["CONTEXT"]
        return_sqs_url = ticket_body["RETURN-SQS-URL"]
        instances_required = ticket_body["INSTANCES_NUM"]
        instance_type = assign_sagemaker_instance_type(image_uri)

        instances_in_use = query_resources(instance_type)
        instances_limit = check_sagemaker_instance_limit(image_uri)
        assert(instances_limit >= instances_in_use)

        # if scheduling success: remove ticket upon successful trigger of CB Executor.
        # if Scheduling failed: add one to the number of tries ["SCHEDULING_TRIES"]

        # enough SageMaker resources, trigger CB Executor and remove ticket
        if (instances_in_use + instances_required) < instances_limit:
            trigger_build(image_uri, build_context, return_sqs_url, ticket_key, instance_type, instances_required)
            delete_ticket()

        # insufficient SageMaker resources, update ticket
        else:
            update_ticket()

if __name__ == "__main__":
    lambda_handler("", "")
