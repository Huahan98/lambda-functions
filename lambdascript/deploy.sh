zip lambda.zip lambda_function.py constants.py
aws lambda update-function-code --function-name DLCTestScheduler --zip-file fileb://lambda.zip
