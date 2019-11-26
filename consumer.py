from __future__ import print_function

import boto3
import datetime
import json
import time
import sys
import argparse
from pprint import pprint
from urllib import parse


import thriftpy2
from thriftpy2.protocol import TCyBinaryProtocolFactory
from thriftpy2.utils import deserialize

import snowplow_analytics_sdk.event_transformer
import snowplow_analytics_sdk.snowplow_event_transformation_exception


client = boto3.client('kinesis')
# accessKeyId = your_key_here_if_not_in_env_vars
# secretAccessKey = your_secret_here_if_not_in_env_vars

collector = thriftpy2.load("collector-payload.thrift")
collector_payload = collector.CollectorPayload()


def serializer(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def print_banner(stream_name, region, interval):
    message = f"listening on the '{stream_name}' stream in the {region} region at {interval} second intervals"
    print("-" * len(message))
    print(message)
    print("-" * len(message))


def consume_stream(stream_name, region, enriched, limit, interval, max_records, pretty):
    if limit < 2:
        limit = 2

    if interval < .5:
        interval = .5

    print_banner(stream_name, region, interval)

    response = client.describe_stream(StreamName=stream_name)

    shard_it = client.get_shard_iterator(StreamName=stream_name,
                                         ShardId=response['StreamDescription']['Shards'][0]['ShardId'],
                                         ShardIteratorType='LATEST')["ShardIterator"]

    output = client.get_records(ShardIterator=shard_it, Limit=2)

    count = 0
    while 'NextShardIterator' in output:
        output = client.get_records(ShardIterator=output['NextShardIterator'], Limit=limit)
        records = output.get('Records')
        if records:
            for record in records:
                if count > max_records:
                    sys.exit(0)
                else:
                    payload = None
                    if enriched:
                        try:
                            payload = snowplow_analytics_sdk.event_transformer.transform(record.get('Data').decode("utf-8"))
                        except():
                            print("An exception occurred when converting an enriched event.")
                    else:
                        try:
                            payload = vars(deserialize(collector_payload, record.get('Data'), TCyBinaryProtocolFactory()))
                            payload['body'] = parse.parse_qs(payload.get('body'))
                        except():
                            print("An exception occurred when converting an enriched event.")

                    record['Data'] = payload

                    result = json.dumps(record, default=serializer, ensure_ascii=False)
                    pprint(result) if pretty else print(result)

                    if max_records > 0:
                        count += 1

        time.sleep(interval)


def main(args=None):
    parser = argparse.ArgumentParser(description='AWS Kinesis stream listener for Snowplow events.')
    parser.add_argument('-s', '--stream-name', type=str, required=True, help='name of Kinesis stream')

    parser.add_argument('-r', '--region', type=str, required=True, help='AWS Region')

    parser.add_argument('-e', '--enriched', action='store_true', help='Results are enriched')

    parser.add_argument('-l', '--limit', type=int, default=25, help='Records per shard limit; default 25, minimum is 2')

    parser.add_argument('-i', '--interval', type=float, default=1.0, help='Response intervals; default 1 second')

    parser.add_argument('-m', '--max', type=int, default=0, help='Maximum records to return; default 0 (infinite)')

    parser.add_argument('-p', '--pretty', action='store_true', help='Pretty print results')
    results = parser.parse_args(args)

    consume_stream(results.name, results.region, results.enriched, results.limit, results.interval,  results.max,
                   results.pretty)


if __name__ == "__main__":
    main(sys.argv[1:])
