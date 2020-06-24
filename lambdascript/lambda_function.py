import json
import boto3

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = "dlc-test-tickets"

    try:
        response = s3.list_objects(Bucket=bucket_name, Prefix="request_tickets/")
        tickets_list = [ticket for ticket in response["Contents"] if ".json" in ticket["Key"]]
        print(tickets_list)
        for ticket in tickets_list:
            ticket_key = ticket["Key"]
            ticket_object = s3.get_object(Bucket=bucket_name, Key=ticket_key)
            ticket_body = json.loads(ticket_object['Body'].read().decode('utf-8'))

            image_uri = ticket_body["ECR-URI"]
            build_context = ticket_body["CONTEXT"]
            return_sqs_url = ticket_body["RETURN-SQS-URL"]


            #building the build instructions
            CB = boto3.client("codebuild")
            build = {"projectName": "DLCTestJobExecutor",
                      "environmentVariablesOverride": [
                            {
                             "name": "PYTHONBUFFERED",
                             "value":"1",
                             "type":"PLAINTEXT"
                            },
                            {
                             "name": "REGION",
                             "value":"us-west-2",
                             "type":"PLAINTEXT"
                            },
                            {
                             "name": "DLC_IMAGE",
                             "value":image_uri,
                             "type":"PLAINTEXT"
                            },
                            {
                             "name": "BUILD_CONTEXT",
                             "value": build_context,
                             "type":"PLAINTEXT"
                            },
                            {
                             "name": "TEST_TYPE",
                             "value":"sagemaker",
                             "type":"PLAINTEXT"
                            },
                            {
                             "name": "RETURN_SQS_URL",
                             "value":return_sqs_url,
                             "type":"PLAINTEXT"
                            },
                            {
                             "name": "TICKET_NAME",
                             "value":ticket_key,
                             "type":"PLAINTEXT"
                            },
                          ]
            }

            #initiate build
            print("start building SageMakerTest....")
            buildResponse = CB.start_build(**build)
            print("launched a codebuild project!")

    except Exception as message:
        print(message)
        raise message
