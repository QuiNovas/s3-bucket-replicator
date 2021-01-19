import boto3
import logging
import os
import urllib.parse

from botocore.client import Config
from multiprocessing.pool import ThreadPool


S3 = boto3.resource('s3', config=Config(signature_version='s3v4'))
DESTINATION_BUCKETS = [S3.Bucket(x) for x in os.environ['DESTINATION_BUCKETS'].split(',')]

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
  bucket_records = []
  for record in event['Records']:
    for bucket in DESTINATION_BUCKETS:
      bucket_records.append((bucket, record))
  with ThreadPool(len(DESTINATION_BUCKETS)) as pool:
    pool.map(process_record, bucket_records)
  return event


def process_record(bucket_record):
  bucket = bucket_record[0]
  record = bucket_record[1]
  key = urllib.parse.unquote(record['s3']['object']['key'])

  if record['eventName'].startswith('ObjectCreated:'):
    logger.info('Copying {} from {} to {}'.format(
      key, 
      record['s3']['bucket']['name'], 
      bucket.name
    ))
    bucket.copy(
      CopySource={
        'Bucket': record['s3']['bucket']['name'],
        'Key': key
      },
      Key=key
    )
  elif record['eventName'].startswith('ObjectRemoved:'):
    logger.info('Deleting {} from {}'.format(
      key, 
      bucket.name
    ))
    bucket.delete_objects(
      Delete={
        'Objects': [
          {
            'Key': key
          }
        ],
        'Quiet': True
      }
    )
  else:
    logger.warning('event {} not supported'.format(record['eventName']))
