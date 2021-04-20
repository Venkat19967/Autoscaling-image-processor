# Importing required packages
from flask import Flask
from flask import request, redirect, render_template
import boto3


app = Flask(__name__)

# This is the route for the default User interface page with all the thee buttons
@app.route('/image_classifier')
def upload_form():
    return render_template("image_upload.html")


# This is request hadler route where the image uploads are sent to backend using a post request
@app.route('/image_classifier', methods=['POST'])
def upload_image():
    #Files are all accessed using the request object
    files = request.files.getlist('photos')
    
    # Intializing S3 bucket name and client and resource objects for S3 and SQS 
    # Used to connect to the services
    bucket_name = 'ccproject-input'
    s3 = boto3.resource('s3')
    sqs = boto3.client('sqs')
    # Input sqs queue url
    queue_url = 'https://sqs.us-east-1.amazonaws.com/961085621450/input-queue.fifo'

    # These are the conditions for different buttons in the form
    # If the user clicks on upload button then it exceutes the below code
    # It checks weather the the user has selected atleast one image 
    # If user selects multiple images then it takes all the images and uploads to S3 and messages to sqs with attributes 
    if request.form['action'] == 'Upload':
       # This function is to send messages to sqs queue with the image attributes
        def sqs_send(key, url):
            
            response = sqs.send_message(
                QueueUrl=queue_url,
                
                MessageAttributes={
                    'image_name':{ 
                    'DataType': 'String',
                    'StringValue': key
                        },
                    'image_url':{ 
                    'DataType': 'String',
                    'StringValue': url
                        }
                },
                MessageBody=(
                "Image info "
                ),
                MessageDeduplicationId= key,
                MessageGroupId='ccproject'
            )
        # This is for chcking atleast one image is selected or not, if not then message is shown to the user
        if len(files)== 1 and files[0].filename == '':
            message = 'Please select a file/files before uploading'
            return render_template('image_upload.html', message= message)
        else:
            for file in files:
                filename = file.filename
                # Upload images to S3 bucket
                s3_bucket = s3.Bucket(bucket_name).put_object(Key = filename,Body =file)
                image_url = "https://ccproject-input.s3.amazonaws.com/"+s3_bucket.key
                # Send message to SQS queue
                sqs_send(s3_bucket.key,image_url)
            return render_template('image_upload.html', filename=len(files))
    else:
        # This else is for result button 
        # It intially gets the approximate images in the queue which is queue length and processes all the messages
        # Shows results in the UI
        
        queue_url = 'https://sqs.us-east-1.amazonaws.com/961085621450/output-queue.fifo'
        # Get approximate number of messages
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
            ]
        )

        results = []
        # Receive message from SQS queue
        # Get the message count
        msg_count = response.get('Attributes').get('ApproximateNumberOfMessages')
        # Retrive all the messages and send them to the User interface
        for i in range(int(msg_count)):
            response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=[
                    'SentTimestamp'
                ],
                MaxNumberOfMessages=1,
                MessageAttributeNames=[
                    'All'
                ],
                    VisibilityTimeout=30,
                    WaitTimeSeconds=0  
            )
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
            )
            results.append(message['Body'])        
        
        return render_template('image_upload.html', results=results)


if __name__=="__main__":
    app.run(host='0.0.0.0',port='8080')
    # app.run(debug=True)
    
