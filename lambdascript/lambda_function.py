import json
import logging
import os
import re
import sys

from datetime import datetime

import boto3
import boto3.exceptions
from functools import cmp_to_key

import constants


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
LOGGER.addHandler(logging.StreamHandler(sys.stdout))


def update_resource_pool(ticket_key, instance_type, num_of_instances, job_type):
    """
    Update the S3 resource pool for usage of SageMaker resources; status = preparing.
    Naming convention of resource pool json: (request ticket name)#(num of instances)-preparing.json

    :param ticket_name: <string> name of the request ticket
    :param job_type: <string> (training/inference)
    :param instance_type: ml.p3.8xlarge/ml.c4.4xlarge/ml.p2.8xlarge/ml.c4.8xlarge
    :param num_of_instances: number of instances required
    """
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")

    # create the in-progress pool ticket content
    pool_ticket_content = {
        "REQUEST_TICKET_KEY": ticket_key,
        "INSTANCE_TYPE": instance_type,
        "INSTANCES_NUM": num_of_instances,
        "STATUS": "preparing",
    }

    # create json file content and upload to S3
    # naming convention of resource-pool tickets: (request ticket name)#(num of instances)-(status).json
    filename = f"{ticket_key.split('/')[-1].split('.')[0]}#{num_of_instances}-preparing.json"
    s3_client.put_object(Bucket=constants.BUCKET_NAME, Key=f"resource_pool/{instance_type}-{job_type}/{filename}")
    S3_ticket_object = s3_resource.Object(constants.BUCKET_NAME, f"resource_pool/{instance_type}-{job_type}/{filename}")
    S3_ticket_object.put(Body=bytes(json.dumps(pool_ticket_content).encode("UTF-8")))


def assign_sagemaker_instance_type(image):
    """
    Assign the instance type that the input image needs for testing

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
    :return: <int> limit on the sagemaker instance type required by testing of the input image
    """
    assert (
        "training" in image or "inference" in image
    ), "Image not valid. Image URI must contain 'training' or 'inference'."

    instance_type = assign_sagemaker_instance_type(image)
    if "training" in image:
        return constants.TRAINING_LIMIT[instance_type]
    else:
        return constants.INFERENCE_LIMIT[instance_type]


def get_num_instances_running(instance_type, job_type):
    """
    Query the number of a specific instance type currently running by SageMaker

    :param instance_type: <string> type of instance required
    :param job_type: <string> type of the job (training/inference)
    :return: number of relevant instances currently in use
    """
    s3_client = boto3.client("s3")
    objects = s3_client.list_objects(Bucket=constants.BUCKET_NAME, Prefix=f"resource_pool/{instance_type}-{job_type}/")
    num_instances_running = 0
    re_pattern = re.compile(".*\#(\d)-.*")

    for entry in objects["Contents"]:
        if entry["Key"].endswith("preparing.json") or entry["Key"].endswith("running.json"):
            instances_required_by_entry = int(re_pattern.match(entry["Key"]).group(1))
            num_instances_running += instances_required_by_entry

    return num_instances_running


def trigger_build(image_uri, context, return_sqs_url, ticket_key, num_of_instances):
    """
    Trigger the Job Executor CodeBuild project with appropriate environment variables

    :param image_uri: ECR URI
    :param context: Build Context
    :param return_sqs_url: SQS return queue url
    :param ticket_key: Key of the request ticket
    :param instance_type: Instance type requested by the test job
    :param num_of_instances: Number of instances required by the test job
    :return: <Bool> True or False to indicate if triggering Job Executor Succeeded
    """
    cb_client = boto3.client("codebuild")
    build = {
        "projectName": "DLCTestJobExecutor",
        "environmentVariablesOverride": [
            {"name": "PYTHONBUFFERED", "value": "1", "type": "PLAINTEXT"},
            {"name": "REGION", "value": "us-west-2", "type": "PLAINTEXT"},
            {"name": "DLC_IMAGE", "value": image_uri, "type": "PLAINTEXT"},
            {"name": "BUILD_CONTEXT", "value": context, "type": "PLAINTEXT"},
            {"name": "TEST_TYPE", "value": "sagemaker", "type": "PLAINTEXT"},
            {"name": "RETURN_SQS_URL", "value": return_sqs_url, "type": "PLAINTEXT"},
            {"name": "TICKET_KEY", "value": ticket_key, "type": "PLAINTEXT"},
            {"name": "NUM_INSTANCES", "value": str(num_of_instances), "type": "PLAINTEXT"},
        ],
    }

    try:
        cb_client.start_build(**build)
        return True

    except cb_client.exceptions.InvalidInputException as e:
        LOGGER.warning(f"Invalid inputs when starting Job Executor: {e}")
    except cb_client.exceptions.ResourceNotFoundException as e:
        LOGGER.warning(f"Job Executor CodeBuild project not found: {e}")
    except cb_client.exceptions.AccountLimitExceededException as e:
        LOGGER.warning(f"Job Executor CodeBuild project could not run, account limit exceeded: {e}")
    return False


def delete_ticket(bucket, key):
    """
    Delete ticket of the given bucket and key.

    :param bucket: <string> bucket name
    :param key: <string> key to the target file
    """
    s3_client = boto3.client("s3")

    s3_client.delete_object(Bucket=bucket, Key=key)


def update_ticket(ticket_key, ticket_body):
    """
    Update the request ticket: if constants.MAX_SCHEDULING_RETRIES or timeout limit has been reached,
    move to dead letter queue.
    Otherwise add one to SCHEDULING_TRIES in the ticket.

    :param ticket_key: <string> key of the ticket
    :param ticket_body: <dict> body of the ticket
    """
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")

    num_of_tries = ticket_body["SCHEDULING_TRIES"]
    request_time = ticket_body["TIMESTAMP"]
    timeout_limit = ticket_body["TIMEOUT_LIMIT"]

    # move to the dead letter queue, max retries reached
    if num_of_tries >= constants.MAX_SCHEDULING_RETRIES:
        s3_client.delete_object(Bucket=constants.BUCKET_NAME, Key=ticket_key)
        dead_letter_filename = f"{ticket_key.split('/')[-1].split('.')[0]}-maxRetries.json"
        s3_client.put_object(Bucket=constants.BUCKET_NAME, Key=f"dead_letter_queue/{dead_letter_filename}")
        S3_ticket_object = s3_resource.Object(constants.BUCKET_NAME, f"dead_letter_queue/{dead_letter_filename}")
        S3_ticket_object.put(Body=bytes(json.dumps(ticket_body).encode("UTF-8")))

        LOGGER.warning(f"Ticket {dead_letter_filename} is moved to the dead letter queue.")

    # move to dead letter queue, timeout limit reached
    elif (datetime.now() - datetime.strptime(request_time, "%Y-%m-%d-%H-%M-%S")).total_seconds() > timeout_limit:
        s3_client.delete_object(Bucket=constants.BUCKET_NAME, Key=ticket_key)
        dead_letter_filename = f"{ticket_key.split('/')[-1].split('.')[0]}-timeout.json"
        s3_client.put_object(Bucket=constants.BUCKET_NAME, Key=f"dead_letter_queue/{dead_letter_filename}")
        S3_ticket_object = s3_resource.Object(constants.BUCKET_NAME, f"dead_letter_queue/{dead_letter_filename}")
        S3_ticket_object.put(Body=bytes(json.dumps(ticket_body).encode("UTF-8")))

        LOGGER.warning(f"Ticket {dead_letter_filename} is moved to the dead letter queue.")

    # update the number of retries
    else:
        ticket_body["SCHEDULING_TRIES"] = num_of_tries + 1
        filename = os.path.basename(ticket_key)
        s3_client.put_object(Bucket=constants.BUCKET_NAME, Key=f"request_tickets/{filename}")
        S3_ticket_object = s3_resource.Object(constants.BUCKET_NAME, f"request_tickets/{filename}")
        S3_ticket_object.put(Body=bytes(json.dumps(ticket_body).encode("UTF-8")))


def check_timeout(start_time):
    """
    Check if time since the start_time has exceeded TIMEOUT_LIMIT_SECONDS, if so exits the program
    This is to ensure this program executes and exits properly with Lambda's 15 mins timeout limit

    :param start_time: <datetime>
    """
    if (datetime.now() - start_time).total_seconds() >= constants.TIMEOUT_LIMIT_SECONDS:
        sys.exit()


def ticket_timestamp_cmp_function(ticket1, ticket2):
    """
    Compares the timestamp of the two request tickets

    :param ticket1, ticket2: <dict> S3 object descriptors from s3_client.list_objects
    :return: <bool>
    """
    ticket1_key, ticket2_key = ticket1["Key"], ticket2["Key"]
    ticket1_timestamp, ticket2_timestamp = (
        ticket1_key.split("/")[-1].split(".")[0].split("_")[-1],
        ticket2_key.split("/")[-1].split(".")[0].split("_")[-1],
    )
    return ticket1_timestamp > ticket2_timestamp


def lambda_handler(event, context):
    start_time = datetime.now()
    bucket_name = constants.BUCKET_NAME

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
        job_type = "training" if "training" in image_uri else "inference"

        instances_in_use = get_num_instances_running(instance_type, job_type)
        instances_limit = check_sagemaker_instance_limit(image_uri)
        assert (
            instances_limit >= instances_in_use
        ), f"Invalid State: Number of instances in use {instances_in_use} is larger than limit {instances_limit}"

        # enough SageMaker resources for requested job
        if (instances_in_use + instances_required) < instances_limit:
            # started Job Executor without errors
            if trigger_build(image_uri, build_context, return_sqs_url, ticket_key, instances_required):
                update_resource_pool(ticket_key, instance_type, instances_required, job_type)
                delete_ticket(bucket_name, ticket_key)

            # Errors occurred with start_build API call
            else:
                update_ticket(ticket_key, ticket_body)

        # insufficient SageMaker resources
        else:
            update_ticket(ticket_key, ticket_body)
