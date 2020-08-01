# this script handle the sns topic from config log and send it to elasticsearch. 

import boto3
import logging
import datetime
import gzip
import os
import sys
import json
import argparse
from io import BytesIO
from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from urllib.parse import unquote

##### define standard configurations ####

# Setup the main app logger
app_log = logging.getLogger("app")
app_log.setLevel(level=logging.INFO)

# Setup the verbose logger
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
tracer = logging.getLogger('elasticsearch')
tracer.setLevel(logging.WARN)

#Setup AWS ElasticAuth.
awsauth = AWS4Auth(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], os.environ['AWS_REGION'], 'es',
                   session_token=os.environ['AWS_SESSION_TOKEN'])


#Setup timestamp
iso_now_time = datetime.datetime.now().isoformat()


#Setup for ELK general connection
elk_node = os.environ['elk_node']
es = Elasticsearch(
hosts=[{'host': elk_node, 'port': 443}],
http_auth=awsauth,
use_ssl=True,
verify_certs=True,
connection_class=RequestsHttpConnection,
timeout=60,
max_retries=10,
retry_on_timeout=True
) 

def create_index(index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "members": {
                "dynamic": "strict",
                "properties": {
                    "title": {
                        "type": "text"
                    },
                    "submitter": {
                        "type": "text"
                    },
                    "description": {
                        "type": "text"
                    },
                    "calories": {
                        "type": "integer"
                    },
                }
            }
        }
    }
    try:
        if not es.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created

def get_files_bucket(bucket, key, start_from=None, chunk_size=1000, es=None, context=None):
    s3 = boto3.client('s3')
    s3_bucket = bucket
    s3_key = key
    logger.info("Getting bucket from folder")
    s3_object = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    content = gzip.GzipFile(fileobj=BytesIO(s3_object['Body'].read())).read()
    logger.info("parsing data")
    index_name = 'config-' + datetime.datetime.now().strftime("%Y-%m-%d")
    logger.info("checking index")
    create_index(index_name)
    data = json.loads(content)
    item_count = 0
    couldnotadd_count = 0
    if data is not None:
        configuration_items = data.get("configurationItems", [])

        if configuration_items is not None:
            for item in configuration_items:
                try:
                    typename = item.get("awsRegion").lower()

                    logger.info(
                        "storing in ES: " + str(item.get("resourceType")))
                    item['snapshotTimeIso'] = iso_now_time
                    body = json.dumps(item)
                    logger.info("sending to ELK")
                    es.index(index=index_name, body=body)
                    logger.info("done!")
                except Exception:
                    return

def handler(event, context):
    es = Elasticsearch(
        hosts=[{'host': os.environ['elk_node'], 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=60,
        max_retries=10,
        retry_on_timeout=True
    )
    logger.info('Event: ' + json.dumps(event, indent=2))
    start_from = event.get('start_from', 0)
    s3_bucket = json.loads(event['Records'][0]['Sns']['Message'])['Records'][0]['s3']['bucket']['name']
    s3_object_key_unformated = json.loads(event['Records'][0]['Sns']['Message'])['Records'][0]['s3']['object']['key']
    s3_object_key = unquote(unquote(s3_object_key_unformated))
    logger.info('Notification received for S3 Bucket: {0}'.format(s3_bucket))
    logger.info('S3 Object Key: {0}'.format(s3_object_key))
    try:
        if "Config" in s3_object_key and "-Digest" not in s3_object_key:
            index_of_remaining = get_files_bucket(context=context, bucket=s3_bucket, key=s3_object_key, start_from=start_from, es=es)
            if index_of_remaining is not None:
                logger.info("Invoking lambda to continue processing from index: {0}".format(index_of_remaining))
                event['start_from'] = index_of_remaining
                lam.invoke(
                    FunctionName=context.function_name,
                    InvocationType='Event',
                    Payload=json.dumps(event)
                )
        else:
            logger.info("Lambda function triggered for s3 key not associated with Config logs: {}".format(s3_object_key))
            return

    except Exception as e:
        logger.error('Something went wrong: ' + str(e))
        return
    logger.info("Done!")
    return

def main(argv):
    os.environ['elk_node'] = argv.elk
    
    event = dict();
    event['s3_bucket'] = argv.s3_bucket
    event['s3_key'] = argv.s3_key

    get_files_bucket(event)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-b', '--s3-bucket',
        required=True
    )
    parser.add_argument(
        '-k', '--s3-key',
        required=True
    )
    parser.add_argument(
        '-e', '--elk',
        required=True
    )
    args = parser.parse_args()
    main(args)