#!/usr/bin/env python3

import os
import time
import csv
import uuid
import random
from pathlib import Path
from itertools import product
from collections import deque
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore



# IMPORTANT CONSTANTS
TARGET_MATCHING = '4-3-2020'
EMAIL_HTML="""
<div>Hello there,</div>
<br><br>
<div>The algorithm has finished running and your match has been determined.</div>
<br>
<a href="https://globalmatchingproject.com">View Your Match</a>
<br><br><br>
<div>Sincerely,</div>
<br>
<div>The Global Matching Project</div>
"""

def createMatch(db, users):
    matchId = uuid.uuid1()
    batch = db.batch()

    # filter users
    filteredUsers = {}
    for user in users:
        filteredUsers[user[0]] = {
            'id': user[0],
            'name': user[1],
            'age': user[3],
            'country': user[4],
            'region': user[5]
        }

    # create match
    matchRef = db.collection('matchings').document(TARGET_MATCHING) \
        .collection('matches').document(matchId)
    batch.set(matchRef, {
        'id': matchId,
        'users': filteredUsers
    })

    # update/email users
    for user in users:
        userRef = db.collection('users').document(user[0])
        batch.update(userRef, {
            'currentMatching': TARGET_MATCHING,
            'currentMatchId': matchId
        })

        emailRef = db.collections('mail').doc()
        batch.set(emailRef, {
            'to': user[2],
            'message': {
                'subject': 'You\'re match is in!',
                'html': EMAIL_HTML
            }
        })

    # commit batch write
    batch.commit()


def queueSignups(signupQueue, path):

    # read signups
    with open(path, 'r', newline='') as bucketFile:
        reader = csv.reader(bucketFile)
        signups = list(reader)

    # shuffle and queue signups
    random.shuffle(signups)
    signupQueue.extend(signups)


def runAlgorithm(db):
    signupQueue = deque()
    matchCount = 0
    bucketFile = 0

    # loop over age buckets
    for age in range(16, 110, 2):
        ageBucket = str(age) + '-' + str(age + 1)

        # loop over placement buckets
        for placementBuckets in product(range(3), repeat=6):
            bucketFile += 1

            # construct path
            path = './signup-data/' + TARGET_MATCHING + '/' + ageBucket
            for bucket in placementBuckets[:-1]: path += '/' + str(bucket)
            path += '/' + str(placementBuckets[-1]) + '.csv'

            # queue new signups
            if Path(path).is_file(): 
                queueSignups(signupQueue, path)
            
            # build matches from deque
            bucketMatchCount = 0
            while len(signupQueue) >= 10:
                matchCount += 1
                bucketMatchCount += 1

                createMatch(db, [
                    signupQueue.popleft(),
                    signupQueue.popleft(),
                    signupQueue.popleft(),
                    signupQueue.popleft()
                ])
            
            print('File ' + str(bucketFile) + '/34263: Created ' \
                + str(bucketMatchCount) + ' matches.')

    while len(signupQueue) > 0:
        if len(signupQueue) % 3 == 0:
            createMatch(db, [
                signupQueue.popleft(),
                signupQueue.popleft(),
                signupQueue.popleft()
            ])
        else:
            createMatch(db, [
                signupQueue.popleft(),
                signupQueue.popleft(),
                signupQueue.popleft(),
                signupQueue.popleft()
            ])

    print('Algorithm finished! Created ' + str(matchCount) + ' matches.')



if __name__ == '__main__':
    try:

        # initialize app
        cred = credentials.Certificate('./private-key.json')
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        print('Connected app to firestore...')

        # initiate algorithm
        runAlgorithm(db)

    except Exception as error:
        print('ERROR: ' + str(error))