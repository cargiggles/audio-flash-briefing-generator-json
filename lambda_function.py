import boto3
import os
import io
import json
import uuid

# 2018-09-04 - This works but it's basically resulting in a two item feed! Don't know why yet Might be the [0] thing I'm doing when reading the file *from* s3

FLASH_BRIEFING_FEED_BUCKET = os.environ['FLASH_BRIEFING_FEED_BUCKET'] # flash-briefing-feeds-acargill
FLASH_BRIEFING_FEED_FILE_NAME = os.environ['FLASH_BRIEFING_FEED_FILE_NAME'] # for example, dag_news.json

s3 = boto3.resource("s3")
bucket = s3.Bucket(FLASH_BRIEFING_FEED_BUCKET)

def feed_already_exists(bucket, file_name):
    keys =[]
    for key in bucket.objects.all():
        keys.append(key.key)

    # print keys
    
    if file_name in keys:
        return True 
    else:
        return False

def lambda_handler(event, context):
    if feed_already_exists(bucket, FLASH_BRIEFING_FEED_FILE_NAME):
        print "file already exists\n"

        # 2a. If Yes - download the existing file (function) input new item to front of dictionary
        s3.Bucket(FLASH_BRIEFING_FEED_BUCKET).download_file(FLASH_BRIEFING_FEED_FILE_NAME, "/tmp/existing_feed.json")
        existing_feed_pointer = "/tmp/existing_feed.json"

        with io.open(existing_feed_pointer, "r", encoding = "utf-8") as rf:
            existing_feed = json.loads(rf.read())[0] # taking the json file, converting it to a dictionary (encapsulated in a list) and taking the 0 index - which is the real dictionary

            print "This is the existing feed from S3\n"
            print existing_feed
            print "\n\n"


        new_item = {
            "uid": str(uuid.uuid4()),
            "updateDate": event['Records'][0]['eventTime'],
            "titleText": event['Records'][0]['s3']['object']['key'], # Parse this out and camel case it delete everything to the left of '/' replace anything that's not a character with a space and make caps
            "mainText": "",
            "streamUrl": "https://s3.amazonaws.com/" + event['Records'][0]['s3']['bucket']['name'] + "/" + event['Records'][0]['s3']['object']['key']
        }

        print "This is the new item from trigger\n"
        print new_item
        print "\n\n"
        
        new_feed = []
        new_feed.append(new_item)
        new_feed.append(existing_feed)

        print "This is the new combined feed\n"
        print new_feed

        with io.open(existing_feed_pointer, "w", encoding = "utf-8") as wf:
            wf.write(json.dumps(new_feed).decode("utf-8"))

        s3.meta.client.upload_file(existing_feed_pointer, FLASH_BRIEFING_FEED_BUCKET, FLASH_BRIEFING_FEED_FILE_NAME, ExtraArgs={'ACL': 'public-read','ContentType': "application/json"})

        #key = bucket.get_key(FLASH_BRIEFING_FEED_FILE_NAME)
        #key.set_acl("public_read")


        #print json.dumps(feed) #JSON-ifying Python dictionary





    else:
        print "file does not exist already"
    
    
    # Question: Can I used the multi-item format for a feed with one item?
    # Answer: Yes
    
    # Feed if it exists, would already be a list

    
    #s3.Bucket("flash-briefing-feeds-acargill").download_file("programmatic_flash_briefing_feed.json", "/tmp/programmatic_flash_briefing_feed.json")


# To Do:

# Tell Boto To Make File Public
# Use Boto To Set Content Header

# 1. Check if feed file already exists in destination bucket
# 2a. If Yes - download the existing file (function) input new item to front of dictionary
# 2b. If No - don't bother downloading existing file and just make a new dictionary with the one item
# 3. JSONify 
# 4. Save new file on S3, make public and set content header