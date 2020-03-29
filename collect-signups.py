#!/usr/bin/env python3

import time
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.api_core.datetime_helpers import to_microseconds, from_microseconds



# IMPORTANT: TARGET_MATCHING CONSTANT
TARGET_MATCHING = '4-3-2020'



def getMetadata():
    with open('./signup-data/' + TARGET_MATCHING + '/metadata', 'r') as metadata:
        line = metadata.readline()
        timestamp = int(line.replace('\n', ''))
    return timestamp

def writeMetadata(timestamp):
    with open('./signup-data/' + TARGET_MATCHING + '/metadata', 'w+') as metadata:
        metadata.write(str(timestamp) + '\n')

def handleOnSnapshot(snapshot, changes, readTime):
    if len(changes) == 0: return
    else: print(str(len(changes)) + ' new signup(s) received...')

    for change in changes:
        if change.type.name == 'ADDED':

            # get document data
            signupId = change.document.id
            signup = change.document.to_dict()
            timestamp = to_microseconds(signup['timestamp'])

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
        print('Connected app to firestore...')
                
        # start listener
        listener = handleSignups(db)
        while True:
            print('Listening for data...')
            time.sleep(180)

    except KeyboardInterrupt:
        print('Shutting down gracefully...')
        if listener: listener.unsubscribe()
    
    except Exception as error:
        print('ERROR: ' + str(error))
        if listener: listener.unsubscribe()



