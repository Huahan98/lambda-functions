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
- place request tickets on the S3 queue with SCHEDULING_TRIES equals to the retry limit of lambda Scheduler.
- run the lambda handler, the desired behavior:
    1. the tickets are moved to the dead letter queue, and removed from the request ticket queue. In the dead letter 
    queue, the reason for scheduling failure (suffix of ticket name) is correctly set to "maxRetries".
- clean up the artifacts on S3. 

Note: This unit test assumes the MAX_SCHEDULING_RETRIES for the lambda Scheduler is 5 (set in constants.py). This test 
also targets the the account dl-containers-dev@amazon.com, account number 754106851545, where the max number of running 
CodeBuild jobs is 2. The Lambda Scheduler will only move tickets to the dead letter queue if maxRetries have been reached 
*and* it is impossible to schedule more jobs on the Job Executor. Hence the expected number of tickets to be moved to 
the dead letter queue is (NUM_OF_TESTING_TICKETS - MAX_RUNNING_CB_JOBS), assuming the Job Executor is not running other 
jobs.
"""

# Test parameters
LAMBDA_MAX_SCHEDULING_TRIES = 5
NUM_OF_TESTING_TICKETS = 5
IMAGE_URI = "754106851545.dkr.ecr.us-west-2.amazonaws.com/pr-tensorflow-training:2.2.0-gpu-py37-cu101-ubuntu18.04-example-pr-269-2020-06-11-22-13-27"
SQS_RETURN_QUEUE = "DUMMY_SQS_URL"
INSTANCES_NUM_PER_TICKET = 1
TIMEOUT_LIMIT_IN_SECONDS = 14400
MAX_RUNNING_CB_JOBS = 2

# S3 path to tickets
BUCKET_NAME = "dlc-test-tickets"
REQUEST_TICKETS_FOLDER = "request_tickets"
IN_PROGRESS_POOL_FOLDER = "resource_pool"
DEAD_LETTER_QUEUE_FOLDER = "dead_letter_queue"

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.StreamHandler(sys.stdout))
LOGGER.setLevel(logging.INFO)


def create_ticket_content(request_time):
    """
    Create content of the ticket to be sent to S3

    :param request_time: <string> datetime timestamp of when request was made
    :return: <dict> content of the request ticket
    """
    content = {
        "CONTEXT": "PR",
        "TIMESTAMP": request_time,
        "ECR-URI": IMAGE_URI,
        "RETURN-SQS-URL": SQS_RETURN_QUEUE,
        "SCHEDULING_TRIES": LAMBDA_MAX_SCHEDULING_TRIES,
        "INSTANCES_NUM": INSTANCES_NUM_PER_TICKET,
        "TIMEOUT_LIMIT": TIMEOUT_LIMIT_IN_SECONDS,
    }

    return content


def test():
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")

    # put the request ticket on request queue
    ticket_names = []
    for i in range(NUM_OF_TESTING_TICKETS):
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

    # check that the tickets have been moved to the dead letter queue
    dead_letter_tickets = s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=f"{DEAD_LETTER_QUEUE_FOLDER}/testing")
    dead_letter_keys = [ticket_record["Key"] for ticket_record in dead_letter_tickets["Contents"]]

    # check that all tickets are moved to the dead letter queue
    assert (
        len(dead_letter_keys) == len(ticket_names) - MAX_RUNNING_CB_JOBS
    ), f"Some tickets not moved to dead letter queue."

    # check that all tickets have the reason of scheduling failure correctly set, "maxRetires"
    re_pattern = re.compile(".*-([a-zA-Z]*).json")
    for dead_letter_key in dead_letter_keys:
        assert (
            re_pattern.match(dead_letter_key).group(1) == "maxRetries"
        ), f"Scheduling failure reason not correct set for ticket {dead_letter_key}."

    # check that all tickets have been correctly removed from the queue
    tickets_remain_on_the_queue = s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=f"{REQUEST_TICKETS_FOLDER}/testing")
    # if there is no object that satisfy the list_objects, there would be no "Contents" field in the API response
    assert (
        "Contents" not in tickets_remain_on_the_queue
    ), f"Some tickets not correctly removed from the queue: {tickets_remain_on_the_queue['Contents']}"

    # clean up the dead letter tickets
    for key in dead_letter_keys:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)

    LOGGER.info("Tests passed.")
    return


if __name__ == "__main__":
    test()
