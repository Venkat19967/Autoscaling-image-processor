import boto3 
import os
import time
import subprocess


s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
sqs = boto3.client('sqs')
input_queue_url = 'https://sqs.us-east-1.amazonaws.com/961085621450/input-queue.fifo'
#input_queue_url = 'https://sqs.us-east-1.amazonaws.com/961085621450/output-result-queue.fifo'
output_queue_url = 'https://sqs.us-east-1.amazonaws.com/961085621450/output-queue.fifo'


def recv_msgs_client(queue_url):

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=300,
        WaitTimeSeconds=0
    )
    return response

## Step0: Keep listening to new items in SQS queue
print("Starting SQS Listener")
while True:
    ## Step1: Get image name from sqs queue
    response = recv_msgs_client(input_queue_url)
    if 'Messages' in response:
        message = response['Messages'][0]
        message_attr = message['MessageAttributes']
        print(message_attr)
        receipt_handle = message['ReceiptHandle']
        img_name = message_attr.get('image_name').get('StringValue')
        img_url = message_attr.get('image_url').get('StringValue')
        print("Image name: " +img_name)
    

        ## Step2: Download image from S3 input bucket
        input_bucket_name = 'ccproject-input'
        s3.Bucket(input_bucket_name).download_file(img_name, img_name)

        ## Step3: Call image classifier with the downloaded image
        print("Running image classifier on image: " + img_name)
        byte_output = subprocess.check_output(['python3', 'image_classification.py', img_name])
        output = byte_output.decode("utf-8").rstrip() 
        print("Classified image name = " + output)

        ## Step4: Get output of classifier and put in S3 bucket
        print("Uploading output to S3")
        img_name_trunc = img_name.split('.')[0]
        output_bucket_name = 'ccproject-output'
        output_result = "(" + img_name_trunc + ", " + output + ")"
        output_filename =  img_name_trunc + ".txt"
        print("S3 bucket file name: " + output_filename)
        print("S3 bucket file contents: " + output_result)
        s3.Object(output_bucket_name, output_filename).put(Body=output_result)
        s3.Bucket(output_bucket_name).download_file(output_filename, output_filename)

        ## Step5: Put output in output SQS queue
        send_response = sqs.send_message(
                      QueueUrl=output_queue_url, 
                      MessageBody=output_result, 
                      MessageDeduplicationId=img_name_trunc,
                      MessageGroupId='ccproject1'
        )
        print("Sent message: " + output_result + " with message id: " + send_response.get('MessageId'))
        ## Step6: Delete msg from input sqs queue
        sqs.delete_message(
            QueueUrl=input_queue_url,
            ReceiptHandle=receipt_handle
        )

    else:
        print("No msgs in the queue! sleeping....")
        time.sleep(5)
    

