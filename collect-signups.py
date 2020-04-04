#!/usr/bin/env python3

import time
import csv
from pathlib import Path
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.api_core.datetime_helpers import to_microseconds, from_microseconds



# IMPORTANT CONSTANTS
TARGET_MATCHING = '4-10-2020'



def getMetadata():
    with open('./signup-data/' + TARGET_MATCHING + '/metadata', 'r') as metadata:
        line = metadata.readline()
        timestamp = int(line.replace('\n', ''))
    return timestamp


def writeMetadata(timestamp):
    with open('./signup-data/' + TARGET_MATCHING + '/metadata', 'w') as metadata:
        metadata.write(str(timestamp) + '\n')


def writeData(signup):
    ageBucket = signup['ageBucket']
    placementBuckets = signup['placementBuckets']

    # build path
    path = './signup-data/' + TARGET_MATCHING + '/' + ageBucket
    for bucket in placementBuckets[:-1]: path += '/' + bucket
        
    # create placement path/file
    Path(path).mkdir(parents=True, exist_ok=True)
    with open(path + '/' + placementBuckets[-1] + '.csv', 'a+', newline='') as bucketFile:
        writer = csv.writer(bucketFile)
        writer.writerow([
            signup['id'], 
            signup['name'], 
            signup['email'], 
            signup['age'], 
            signup['country'], 
            signup['region']
        ])


def handleOnSnapshot(snapshot, changes, readTime):
    if len(changes) == 0: return
    else: print(str(len(changes)) + ' new signup(s) received...')

    # handle all signups
    for change in changes:
        if change.type.name == 'ADDED':

            # get document data
            signup = change.document.to_dict()
            timestamp = to_microseconds(signup['timestamp'])

            # add signup
            writeData(signup)

            # update latest timestamp
            writeMetadata(timestamp)

    print('Finished processing ' + str(len(changes)) + ' signup(s)...')


def handleSignups(db):
    timestamp = getMetadata()
    dt = from_microseconds(timestamp)

    # execute read query
    signupsRef = db.collection('matchings').document(TARGET_MATCHING).collection('signups')
    return signupsRef.where('timestamp', '>', dt).order_by('timestamp', 'ASCENDING').on_snapshot(handleOnSnapshot)



if __name__ == '__main__':
    listener = None

    try:

        # initialize app
        cred = credentials.Certificate('./private-key.json')
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        print('Connected app to firestore...\n')
                
        # start listener
        listener = handleSignups(db)
        while True:
            print('Listening for data...')
            time.sleep(180)

    except KeyboardInterrupt:
        print('Shutting down gracefully...')
        if listener: listener.unsubscribe()
        time.sleep(3)
    
    except Exception as error:
        print('ERROR: ' + str(error))
        if listener: listener.unsubscribe()
        time.sleep(3)