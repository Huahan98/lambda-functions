# Timeout limit for this lambda program
TIMEOUT_LIMIT_SECONDS = 780 # 13 mins
BUCKET_NAME = "dlc-test-tickets"
MAX_SCHEDULING_TRIES = 5

# SageMaker inference (hosting) instance limits
P3_8XLARGE_INFERENCE = 20
C4_4XLARGE_INFERENCE = 30
P2_8XLARGE_INFERENCE = 20
C4_8XLARGE_INFERENCE = 30

# SageMaker training instance limits
P3_8XLARGE_TRAINING = 40
C4_4XLARGE_TRAINING = 50
P2_8XLARGE_TRAINING = 40
C4_8XLARGE_TRAINING = 50

TRAINING_LIMIT = {"ml.p2.8xlarge":P2_8XLARGE_TRAINING, "ml.c4.4xlarge": C4_4XLARGE_TRAINING,
                 "ml.c4.8xlarge": C4_8XLARGE_TRAINING, "ml.p3.8xlarge": P3_8XLARGE_TRAINING}

INFERENCE_LIMIT = {"ml.p2.8xlarge":P2_8XLARGE_INFERENCE, "ml.c4.4xlarge": C4_4XLARGE_INFERENCE,
                 "ml.c4.8xlarge": C4_8XLARGE_INFERENCE, "ml.p3.8xlarge": P3_8XLARGE_INFERENCE}