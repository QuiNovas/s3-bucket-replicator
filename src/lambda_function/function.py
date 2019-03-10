import boto3
import logging
import os

from botocore.client import Config


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    return event
