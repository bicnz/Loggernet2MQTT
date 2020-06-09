#!/usr/bin/env python3

import sys
import json
import time
import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np

args=sys.argv
if len(args) <= 1:
    print("The argv should be SiteID and CSV filename")
    sys.exit()

# Configuration Parameters.
SITE_ID = args[1]
CSV_FILE_NAME = args[2]
MQTT_TOPIC = 'Loggernet/' + SITE_ID
MQTT_PORT = 1883
MQTT_BROKER = "127.0.0.1"
MQTT_USER = "username"
MQTT_PASS = "password"
PUBLISH_RETRIES = 3
DEBUG = 1

# On connect MQTT Callback.
def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True #set flag
        print("MQTT connected OK")
    else:
        print("No MQTT connection Returned code=",rc)

# on publish MQTT Callback.
def on_publish(client, userdata, mid):
    print("Message Published.")

# Read CSV file, Skip additional header rows, set header for column names, set RECORD column as index, change NAN strings to NaN floats
df = pd.read_csv(CSV_FILE_NAME, skiprows=[0,2,3], header = 0, index_col = ['RECORD'], na_values=['NAN'])
df = df.tail(1) # select last row from csv file
df = df.replace([np.inf, -np.inf], np.nan) # replace inf or -inf values with NaN floats
df = df.fillna(0.0) # replace NaN's with 0
float_col = df.select_dtypes(include=['int64']) # Select integer columns and recast to float
for col in float_col.columns.values:
        df[col] = df[col].astype('float64')
mask = df.astype(str).apply(lambda x : x.str.match(r'\d{4}-\d{2}-\d{2} \d{2}\:\d{2}\:\d{2}').all()) # regex for timestamps
df.loc[:,mask] = df.loc[:,mask].apply(pd.to_datetime) # set timestamps to datetime dtype
if 'StationID' in df.columns: # if StationID column exists, rename to SiteID
    df.rename(columns={'StationID': 'SiteID'}, inplace=True)
if df['SiteID'].dtype.kind in 'biufc': # if SiteID column contains numbers, replace with ID from argv
    df.loc[:, 'SiteID'] = SITE_ID
data_out = df.to_json(orient='records') # output dataframe to JSON

if DEBUG == 1:
    print (df.dtypes)
    print (df)
    print (data_out)

# Connecting to MQTT for publishing.
mqtt.Client.connected_flag=False
client = mqtt.Client()
client.username_pw_set(MQTT_USER,MQTT_PASS)
client.on_connect = on_connect # On Connect Callback.
client.on_publish = on_publish # On Publish Callback.
client.connect(MQTT_BROKER, 1883, 15) # Connecting to the MQTT Broker.
client.loop_start()
while not client.connected_flag: #wait in loop
    print("In wait loop")
    time.sleep(1)

while PUBLISH_RETRIES > 0:
        try:
            client.publish(MQTT_TOPIC, data_out)
            time.sleep(1)
            PUBLISH_RETRIES = 0
        except:
            print("Publish Failed.")
            time.sleep(2)
            PUBLISH_RETRIES -=1

client.loop_stop()
client.disconnect()
