#!/usr/bin/env python3

import re
import logging
import datetime
import time
import boto3
import os

# Set variables
date_min = '2019-03-11'
date_max = '2019-03-12'

BUCKET_NAME = os.environ['BUCKET_NAME']
BUCKET_KEY_PREFIX = os.environ['BUCKET_KEY_PREFIX']
S3_ACCESS_KEY_ID = os.environ['S3_ACCESS_KEY_ID']
S3_SECRET_ACCESS_KEY = os.environ['S3_SECRET_ACCESS_KEY']
S3_REGION_NAME = 'eu-west-2'
GA_BUCKET_NAME = os.environ['GA_BUCKET_NAME']
#GA_BUCKET_KEY_PREFIX = 'nats'
GA_BUCKET_KEY_PREFIX = os.environ['GA_BUCKET_KEY_PREFIX']
GA_S3_ACCESS_KEY_ID = os.environ['GA_S3_ACCESS_KEY_ID']
GA_S3_SECRET_ACCESS_KEY = os.environ['GA_S3_SECRET_ACCESS_KEY']
LOG_FILE = '~/log/NATS_backload.log'

def main():
    """
    Main function
    """
    logformat = '%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s'
    form = logging.Formatter(logformat)
    logging.basicConfig(
        format=logformat,
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    logger = logging.getLogger()
    logger.info("Starting")

    boto_s3_session = boto3.Session(
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name=S3_REGION_NAME
    )

    boto_ga_s3_session = boto3.Session(
        aws_access_key_id=GA_S3_ACCESS_KEY_ID,
        aws_secret_access_key=GA_S3_SECRET_ACCESS_KEY,
        region_name=S3_REGION_NAME
    )

    s3_conn = boto_s3_session.resource('s3')
    ga_s3_conn = boto_ga_s3_session.resource('s3')
    source_bucket = s3_conn.Bucket(BUCKET_NAME)
    target_bucket = ga_s3_conn.Bucket(GA_BUCKET_NAME)
    files = []
    timestamps = []

    logger.info("Running loops...")
    for file_list in source_bucket.objects.filter(Prefix=BUCKET_KEY_PREFIX):
        regex = re.compile('\[-PRMD=EG-ADMD=ICAO-C=XX-;MTA-EGGG-1-MTCU_[A-Z0-9]{16}.*\].json')
        search = regex.findall(file_list.key)
        if len(search) != 0:
            files.append(search[0])
        else:
            continue

    for i in files:
        modified_time = (source_bucket.Object(BUCKET_KEY_PREFIX + '/' + i).last_modified)
        regex = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}')
        date = regex.findall(str(modified_time))
        timestamps.append(date[0])

    # create dictionary to compare files and timestamps
    zipObj = zip(files, timestamps)
    dictionary = dict(zipObj)

    logger.info("Copying files...")
    for x in dictionary.items():
        if (x[1] >= date_min) and (x[1] <= date_max):
            filename = x[0]
            obj = target_bucket.Object(GA_BUCKET_KEY_PREFIX + '/' + filename)
            copy_source = {'Bucket': BUCKET_NAME, 'Key': BUCKET_KEY_PREFIX + '/' + filename}
            obj.copy(copy_source)
            logger.info("Copied: " + filename)
        else:
            filename = x[0]
            logger.error("Copy failed for: " + filename)


if __name__ == '__main__':
    main()
