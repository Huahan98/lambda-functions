import json
import logging
import re
import sys
import time

from datetime import datetime

import boto3

from lambda_function import lambda_handler

"""
How tests are executed:
- place request tickets on the S3 queue requiring the same instance type, which exceeds the limit of that instance
- run the lambda handler, the desired behavior:
    1. have some later request tickets remain on queue (insufficient SageMaker resources), with number of tries add one.
    2. the in-progress pool is properly updated (showing "preparing" tickets), and jobs on the in-progress pool do not 
    exceed the total limit for the instance type. Scheduled request tickets correctly removed from the queue.
- clean up the artifacts on S3. 

Note: This unit test targets specifically the instance type ml.p3.8xlarge-training, which currently has a quota of 4 
instances, on the account dl-containers-dev@amazon.com, account number 754106851545. To test this instance type, a 
tensorflow gpu image is used.
"""

INSTANCE_TYPE = "ml.p3.8xlarge-training"
INSTANCES_LIMIT = 4
IMAGE_URI = "754106851545.dkr.ecr.us-west-2.amazonaws.com/pr-tensorflow-training:2.2.0-gpu-py37-cu101-ubuntu18.04-example-pr-269-2020-06-11-22-13-27"
SQS_RETURN_QUEUE = "DUMMY_SQS_URL"
INSTANCES_NUM_PER_TICKET = 1
TIMEOUT_LIMIT = 14400

# S3 path to the request tickets
BUCKET_NAME = "dlc-test-tickets"
REQUEST_TICKETS_FOLDER = "request_tickets"
IN_PROGRESS_POOL_FOLDER = "resource_pool"

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.StreamHandler(sys.stdout))
LOGGER.setLevel(logging.INFO)


def create_ticket_content(request_time):
    """
    Create content of the ticket to be sent to S3

    :param image: <string> ECR URI
    :param context: <string> build context (PR/MAINLINE/NIGHTLY/DEV)
    :param num_of_instances: <int> number of instances required by the test job
    :param request_time: <string> datetime timestamp of when request was made
    :return: <dict> content of the request ticket
    """
    content = {
        "CONTEXT": "PR",
        "TIMESTAMP": request_time,
        "ECR-URI": IMAGE_URI,
        "RETURN-SQS-URL": SQS_RETURN_QUEUE,
        "SCHEDULING_TRIES": 0,
        "INSTANCES_NUM": INSTANCES_NUM_PER_TICKET,
        "TIMEOUT_LIMIT": TIMEOUT_LIMIT,
    }

    return content


def test():
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")

    # place more requests on the queue than the quota
    num_of_tickets = (INSTANCES_LIMIT // INSTANCES_NUM_PER_TICKET) * 2
    assert num_of_tickets * INSTANCES_NUM_PER_TICKET > INSTANCES_LIMIT, "SageMaker quota not ran out, invalid test."

    # put the request ticket on request queue
    ticket_names = []
    for i in range(num_of_tickets):
        request_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        request_content = create_ticket_content(request_time)

        # naming convention of request tickets: {7 digit name}-{ticket name counter}_(datetime string)
        ticket_name = f"testing-{str(i)}_{request_time}"
        ticket_names.append(ticket_name)

        s3_client.put_object(Bucket=BUCKET_NAME, Key=f"{REQUEST_TICKETS_FOLDER}/{ticket_name}.json")
        S3_ticket_object = s3_resource.Object(BUCKET_NAME, f"{REQUEST_TICKETS_FOLDER}/{ticket_name}.json")
        S3_ticket_object.put(Body=bytes(json.dumps(request_content).encode("UTF-8")))

        # sleep for 1 second to make sure the ticket request times are unique
        time.sleep(1)

    # check that the request tickets have been properly placed
    placed_tickets = s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=f"{REQUEST_TICKETS_FOLDER}/testing")
    assert len(placed_tickets["Contents"]) == len(ticket_names), "Request tickets not correctly placed."
    LOGGER.info(f"Request tickets placed: {ticket_names}")

    # call the lambda scheduler
    lambda_handler("dummy_event", "dummy_context")

    # check the number of tries for the tickets remain on the queue is updated to 1
    unscheduled_tickets = s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=f"{REQUEST_TICKETS_FOLDER}/testing")
    unscheduled_keys = [ticket_record["Key"] for ticket_record in unscheduled_tickets["Contents"]]
    for unscheduled_key in unscheduled_keys:
        ticket_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=unscheduled_key)
        ticket_body = json.loads(ticket_object["Body"].read().decode("utf-8"))
        assert ticket_body["SCHEDULING_TRIES"] == 1, f"Scheduling tries not updated for ticket {unscheduled_key}"

    # check the in-progress pool is updated
    scheduled_jobs = s3_client.list_objects(
        Bucket=BUCKET_NAME, Prefix=f"{IN_PROGRESS_POOL_FOLDER}/{INSTANCE_TYPE}/testing"
    )
    scheduled_keys = [ticket_record["Key"] for ticket_record in scheduled_jobs["Contents"]]
    re_pattern = re.compile(".*\#(\d)-.*")
    for scheduled_key in scheduled_keys:
        assert (
            int(re_pattern.match(scheduled_key).group(1)) == INSTANCES_NUM_PER_TICKET
        ), f"Number of instances not correct for in-progress pool ticket {scheduled_key}"

    # check that all request tickets are either scheduled or on the queue
    assert len(scheduled_keys) + len(unscheduled_keys) == len(
        placed_tickets
    ), f"Some request tickets are lost: neither scheduled or on the queue."

    # clean up the unscheduled tickets
    for unscheduled_key in unscheduled_keys:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=unscheduled_key)

    # clean up the scheduled tickets
    for scheduled_key in scheduled_keys:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=scheduled_key)

    LOGGER.info("Tests passed.")
    return


if __name__ == "__main__":
    test()
