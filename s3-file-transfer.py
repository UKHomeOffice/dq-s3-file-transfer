#!/usr/bin/env python3

import re
import logging
import datetime
import time
import boto3
import os

# Set variables
BUCKET_NAME = os.environ['BUCKET_NAME']
BUCKET_KEY_PREFIX = os.environ['BUCKET_KEY_PREFIX']
S3_ACCESS_KEY_ID = os.environ['S3_ACCESS_KEY_ID']
S3_SECRET_ACCESS_KEY = os.environ['S3_SECRET_ACCESS_KEY']
S3_REGION_NAME = 'eu-west-2'
GA_BUCKET_NAME = os.environ['GA_BUCKET_NAME']
GA_BUCKET_KEY_PREFIX = 'nats'
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
    date_min = '2019-03-11'
    date_max = '2019-03-12'
    logger.info("Running loop")

    for file_list in source_bucket.objects.filter(Prefix=BUCKET_KEY_PREFIX):
        # print(file_list.key)
        regex = re.compile('\[-PRMD=EG-ADMD=ICAO-C=XX-;MTA-EGGG-1-MTCU_[A-Z0-9]{16}.*\].json')
        search = regex.findall(file_list.key)
        # print(search)
        if len(search) != 0:
            files.append(search)
        else:
            continue
    for item in files:
        print(item[0])
        modified_time = (source_bucket.Object(BUCKET_KEY_PREFIX + '/' + item[0]).last_modified)
        print(modified_time)


        regex = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}')
        strict_date = regex.findall(str(modified_time))
        timestamps.append(strict_date)
        print(strict_date)
        if strict_date == date_min:
            print('egg')
        else:
            print('not_egg')












        # if match.search(file_list.key):
        #     logger.info('Found {0}'.format(file_list.key))
        #     filename = file_list.key.split('/')[-1]
        #     print(filename)
        #     obj = target_bucket.Object(GA_BUCKET_KEY_PREFIX + '/' + filename)
        #     copy_source = {'Bucket': file_list.BUCKET_NAME, 'Key': file_list.key}
        #     obj.copy(copy_source)
        # else:
        #     logger.error('Copy failed')

if __name__ == '__main__':
    main()
