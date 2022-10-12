# Author: Raghavendra Deshmukh
# Purpose: Learning how to code in Python and use some Libraries in General and from Google
# This program uses yfinance python package to Read the Current Market Price of some of the popular Companies
# It then publishes this information to a Google Pub/Sub Topic

# Imports for the Python script
import yfinance as yf
from datetime import datetime
import time
import json
from google.cloud import pubsub_v1

# The companies whose Stock price is needed
companies = ['GOOG', 'MSFT', 'AMZN', 'ORCL', 'AAPL', 'META', 'SAP']

# Setup the GCP Project, Pub/Sub Topic
project = "sap-adapter"
topic = "rd_topic"

# Setup the Publisher Client. Note that here we rely on a Service Account Key (JSON Format)
# This Key file is setup in the Environment Path: GOOGLE_APPLICATION_CREDENTIALS
publisher = pubsub_v1.PublisherClient()

# Setup the Topic Path to Publish the Information
topic_path = publisher.topic_path(project, topic)

# Initialize the JSON Encoder object
js = json.JSONEncoder()

count = 0
# Run a Loop.  Could be an Infinite Loop or something that runs based on a certain condition.  Here I run it for a finite number of times
while count < 5:
    for company in companies:
        # Get the ticker info from the yahoo finance object
        ticker = yf.Ticker(company).info
        # Get the Current Market Price 
        currprice = ticker['regularMarketPrice']
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")

        # Create the Stock Information record
        stock_info = [{u"Company":company, u"CurrentValue":currprice, u"Currenttime":current_time}]

        # We need to Encode it (default utf-8) to be sent to the Pub/Sub Topic
        pubsub_data = js.encode(stock_info)
        pubsub_data = pubsub_data.encode()
        print(stock_info)

        # Send it to the Pub/Sub Topic
        future = publisher.publish(topic_path, pubsub_data)
    count = count+1
    time.sleep(5)           
