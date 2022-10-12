# Author: Raghavendra Deshmukh
# Purpose: Learning how to code in Python and use some Libraries in General and from Google
# This program reads Stock Info from a Google Pub/Sub Topic
# It then Writes that Information to a BigQuery Database Table
# It can be any other DB Table as well

# Imports for the Python script
from google.api_core import retry
from google.cloud import pubsub_v1
from google.cloud import bigquery
import time
from json.decoder import JSONDecoder
import json
from google.auth import jwt

# TODO(developer) Setup the GCP Project ID and the Pub/Sub Subscription
project_id = "sap-adapter"
subscription_id = "rd_topic-sub"

#Setup BigQuery Connection Details
client = bigquery.Client()
table = "sap-adapter.TESTRD.Stockprices"

# Setup the Number of Messages to be read from the Pub/Sub Pull API.  
# Note that it is never guaranteed that the same number of messages will be read by Pull
NUM_MESSAGES = 2

# Setup the Service Account JSON Key for Pub/Sub
pubsub_service_account_json = "/Users/deshmukhr/Downloads/sap-adapter-64ca49198519.json"
service_account_info = json.load(open(pubsub_service_account_json))

# Setup the Credentials for the Pub/Sub API Call using the Service account Information
audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
credentials = jwt.Credentials.from_service_account_info(
    service_account_info, audience=audience
)

attempt = 0

# Run an Infinite Loop or Something that runs till a certain condition is met
while True:
#    subscriber = pubsub_v1.SubscriberClient()
    if attempt >= 2:
        break

    #Initialize the Subscriber and the Subscription Path
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Wrap the subscriber in a 'with' block to automatically call close() to
    # close the underlying gRPC channel when done.
    #with subscriber:
    # The subscriber pulls a specific number of messages. The actual
    # number of messages pulled may be smaller than max_messages.
    response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": NUM_MESSAGES},
            retry=retry.Retry(deadline=300), 
        )
    
    # Check if the Response is Empty, If yes, Wait and Continue the Loop to Pull again after a short wait
    if len(response.received_messages) == 0:
        print("No messages in Queue... Waiting...")
        attempt = attempt + 1
        time.sleep(10)
        continue

    # To keep track of the messages received and acknowledge them    
    msg_ids = []

    # Loop through the Received Messages
    for received_message in response.received_messages:
        print(f"Received: {received_message.message.data}.")

        # Get the Message, Convert it to a List Data which is needed for BigQuery Inserts
        data = received_message.message.data
        s1 = str(data, 'UTF-8')
        s1 = JSONDecoder().decode(s1)
        rowdata = list(s1)

        # Invoke the BigQuery Insert Call and pass the Record
        errors = client.insert_rows_json(table, rowdata)
        if errors == []:
            print("Record inserted successfully")
            msg_ids.append(received_message.ack_id)
        else:
            print("Encountered errors while inserting rows: {}".format(errors)) 
    
    # To Ensure that the same Record is not pulled from the Topic, Acknowledge it to the Subscription Path
    subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": msg_ids})

    time.sleep(3)

