#!/usr/bin/python3

# fake transaction for thumb image
# inputs are esn and timestamp as a string.  It should be to a real thumbnail
# we template into a workflow document, then send to host

import requests
import json
import sys
import random
import uuid
import time
from datetime import datetime as dt, timedelta
import boto3
import urllib

aws_iam_user_key = 'AKIAQ6B426B4GNW3337I'
aws_iam_user_secret = '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'
esns = ["10081a66", "100d93f9", "1002263e", "100d4c42", "100eebbd"]
eventTypes = ["motion", "unknown", "roi_motion", "linecross", "loitering", "intrusion"]

aws_bucket_name = 'een-search-data'
s3_base_path = 'https://s3.ap-south-1.amazonaws.com/' + aws_bucket_name + '/'
s3_key_prefix = "archiver_simulator/1004a8b6"
dedupe_endpoint = "http://localhost:9090/api/v2/search/dedup/test"
#dedupe_endpoint = "http://3.109.225.94:30002/api/v2/search/dedup/test"

# Sanity Image https://s3.ap-south-1.amazonaws.com/een-search-data/sanity/search_1p.jpg
# Marcel https://s3.ap-south-1.amazonaws.com/een-search-data/vehicle/images_marcel_unique/02011G2_0_crop.jpg

doc = """{ 
        "unique": "thumbnail_$$ESN$$_$$TIMESTAMP$$_$$EVENTID$$",
        "info": {
            "image": { 
                "low_resolution": { "mimetype": "image/jpeg", "resolution": { "x": 640.0, "y": 800 }, "framing": { "axis": { "pan": 0, "tilt": 0}, "fov": { "x": 40, "y": 22.5}}, "url": "$$IMAGE_URL" },
                "high_resolution": { "mimetype": "image/jpeg", "resolution": { "x": 1920, "y": 1080 }, "framing": { "axis": { "pan": 0, "tilt": 0}, "fov": { "x": 40, "y": 22.5}}, "url": "http://$$ESN$$.a.plumv.com:28080/image.jpg?id=$$ESN$$&$$TIMESTAMP$$=20210326121212.000" }
            },
            "event": {
                "event": "$$EVENT$$",
                "eventType": "$$EVENTTYPE$$",
                "eventInfo": {"$$EVENTTYPE$$":{"event":"in", "objectid":"7dd31de4-7ea2-47e2-8dd5-ff9c54bbe4eb", "triggerid":"20220119173051.513"}, "ns":101},
                "accountid" : "",
                "cameraid": "$$ESN$$",
                "timestamp": "$$TIMESTAMP$$",
                "eventid": "$$EVENTID$$"
            }
        },
        "workflow": {
            "workFlowId": "$$ID$$",
            "currentWorker": "dedup",
            "currentPath": [ ],
            "workers": {
                "archive": {
                    "topic": "archive$$TOPIC-SUFFIX$$",
                    "emits": []
                },
                "dedup": {
                    "topic": "",
                    "emits": [
                        { "filter": { "op": "always"}, "worker": "inference" },
                        { "filter": { "op": "always", "include": { "workSequence": 0}}, "worker": "archive" }
                    ]
                },
                "inference": {
                    "topic": "inference$$TOPIC-SUFFIX$$",
                    "emits": [ 
                        { "filter": { "op": "always", "include": { "workSequence": 1}}, "worker": "archive" },
                        { "filter": { "op": "always" }, "worker": "store" },
                        { "filter": { "op": "always" }, "worker": "alert" }
                    ]
                },
                "store": {
                    "topic": "store$$TOPIC-SUFFIX$$",
                    "emits": []
                } ,
                "alert": {
                    "topic": "alert$$TOPIC-SUFFIX$$",
                    "emits": []
                } 
            }
        }
    }"""

# if len(sys.argv) < 2:
#     print("usage %s <num events>")
#     sys.exit(-1)

random.seed()
eventId = "%08x"%(random.randrange(0x7fffffff))

events = 50

# client = boto3.client(
#     's3',
#     aws_access_key_id=aws_iam_user_key,
#     aws_secret_access_key=aws_iam_user_secret)python3 

continuationToken = None
with open("url.json", 'r') as json_file:
    images = json.load(json_file)["data"]
  
image_idx = 0
sent = 0
# startTime = time.time_ns()
startTime = int(time.time()*10**9)

while True:

   
    image   = s3_base_path + urllib.parse.quote(images[image_idx])
    curDoc  = doc
    #dt      = dt - timedelta(hours=1)
    delay     = random.randint(0, 20)
    now       = dt.utcnow() - timedelta(hours=0, seconds=delay)
    esn       = random.choice(esns)
    eventType = random.choice(eventTypes)
    timestamp = now.strftime("%Y%m%d%H%M%S.%f")[:-3]
    event     = "image"
    
    #event = "thumbnail"
    #timestamp = "20221123070000.002"
    #esn = "100d93f9"
    #eventType = "roi_motion"
    #sent = 0
    #eventId = "eventid2"
    
    image = images[sent]
    curDoc = curDoc.replace("$$ESN$$",esn)
    curDoc = curDoc.replace("$$EVENT$$",event)
    curDoc = curDoc.replace("$$EVENTTYPE$$",eventType)
    # Sending time in EEN format (YYYYMMDDhhmmss.xxx ) https://apidocs.eagleeyenetworks.com/apidocs/#images-and-video
    curDoc = curDoc.replace("$$TIMESTAMP$$", timestamp)
    curDoc = curDoc.replace("$$EVENTID$$",eventId+str(sent))
    curDoc = curDoc.replace("$$IMAGE_URL",image)
    
    #if (True or (sent % 2 == 0)):
    #if (False and (sent % 2 == 0)):
    if ((sent % 2 == 0)):
        curDoc = curDoc.replace("$$TOPIC-SUFFIX$$","")
        #curDoc = curDoc
    else:
        curDoc = curDoc.replace("$$TOPIC-SUFFIX$$","-BULK")
    
    uu = uuid.uuid1()
    curDoc = curDoc.replace("$$ID$$",str(uu))

    #print(curDoc)
    print(f"sending event: {sent} url: {image}")
    resp = requests.post(dedupe_endpoint, data=curDoc,headers={"Content-Type": "application/json", "Authorization": "DEDUPE-API-KEY"})
    print(f"response code: {resp.status_code}")
    print(f"response time: {resp.elapsed.total_seconds()}")

    if (resp.status_code == 200):
        image_idx += 1
        sent += 1

        if image_idx >= len(images):
            image_idx = 0
            sent = 0
    else: 
        print(f"response msg: {resp.text}")

    print(f"total sent: {sent} rate: { sent * 1000000000 / (int(time.time()*10**9) - startTime) }")

    time.sleep(1)    #  8+ fps
    # time.sleep(0.1)    #  8+ fps
    #time.sleep(0.07)  #  10+ fps
    # time.sleep(0.065) #  11+ fps 
    #time.sleep(0.01) 
