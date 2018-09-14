# Audio Flash Briefing Generator - JSON v1.0
# Andrew Cargill
# Sept. 13, 2018

import boto3
import os
import io
import json
import uuid

# User Settings
single_item_feed = False
frugality = False

# Lambda Environment Variables
fb_feed_destination_bucket = os.environ['fb_feed_destination_bucket'] # Ex: flash-briefing-feeds-bucket
fb_feed_file_name = os.environ['fb_feed_file_name'] # Ex: breaking_news.json

# Boto3 Objects
s3 = boto3.resource("s3")
bucket = s3.Bucket(fb_feed_destination_bucket)

def feed_already_exists(bucket, file_name):
    keys =[]
    for key in bucket.objects.all():
        keys.append(key.key)

    if file_name in keys:
        return True
    else:
        return False

def make_title_text(object_key):
    title_text = object_key[object_key.rfind("/")+1:] # Parse key to save only the file name at end
    title_text = title_text.replace(".mp3", "") # Removes ".mp3" file extension
    title_text = title_text.replace("+", " ") # Replace "+" with space. Spaces are converted to "+" in S3 trigger
    return title_text

def reduced_redundancy(bucket, file_name):
    copy_source = {
        "Bucket": bucket,
        "Key": file_name
    }
    s3.meta.client.copy(copy_source, bucket, file_name, ExtraArgs = {'StorageClass': 'REDUCED_REDUNDANCY', 'MetadataDirective': 'COPY'})

def lambda_handler(event, context):
    print event
    # Should store valuable events here in more friendly variable names for re-use later

    if feed_already_exists(bucket, fb_feed_file_name) and not single_item_feed:
        s3.Bucket(fb_feed_destination_bucket).download_file(fb_feed_file_name, "/tmp/existing_feed.json")
        feed_pointer = "/tmp/existing_feed.json"

        with io.open(feed_pointer, "r", encoding = "utf-8") as rf:
            feed = json.loads(rf.read())

    else:
        feed_pointer = "/tmp/new_feed.json"
        feed = []

    new_item = {
        "uid": str(uuid.uuid4()),
        "updateDate": event['Records'][0]['eventTime'],
        "titleText": make_title_text(event['Records'][0]['s3']['object']['key']),
        "mainText": "",
        "streamUrl": "https://s3.amazonaws.com/" + event['Records'][0]['s3']['bucket']['name'] + "/" + event['Records'][0]['s3']['object']['key']
    }

    if frugality:
        reduced_redundancy(event['Records'][0]['s3']['bucket']['name'], event['Records'][0]['s3']['object']['key'].replace("+", " "))

    # Making .mp3 file public
    # Replacing "+" with " " to avoid key error for files with spaces
    mp3_object = s3.Object(event['Records'][0]['s3']['bucket']['name'], event['Records'][0]['s3']['object']['key'].replace("+", " "))
    mp3_object.Acl().put(ACL="public-read")

    feed.insert(0, new_item)

    with io.open(feed_pointer, "w", encoding = "utf-8") as wf:
        wf.write(json.dumps(feed, indent = 4, sort_keys = True, ensure_ascii = False).decode("utf-8"))

    s3.meta.client.upload_file(feed_pointer, fb_feed_destination_bucket, fb_feed_file_name, ExtraArgs={'ACL': 'public-read','ContentType': "application/json"})
