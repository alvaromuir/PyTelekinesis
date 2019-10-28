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


kinesis = boto3.client('kinesis')
# accessKeyId = your_key_here_if_not_in_env_vars
# secretAccessKey = your_secret_here_if_not_in_env_vars

collector = thriftpy2.load("collector-payload.thrift")
collector_payload = collector.CollectorPayload()


def serializer(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def consume_stream(stream, region, enriched, interval, limit, to_json, pretty):
    print("\n-------------------------------------------------------------------------------")
    print(f"listening on the '{stream}' stream in the {region} region at {interval} second intervals")
    print("-------------------------------------------------------------------------------\n")
    response = kinesis.describe_stream(StreamName=stream)

    shard_it = kinesis.get_shard_iterator(StreamName=stream,
                                          ShardId=response['StreamDescription']['Shards'][0]['ShardId'],
                                          ShardIteratorType='LATEST')["ShardIterator"]

    out = kinesis.get_records(ShardIterator=shard_it, Limit=2)

    count = 0
    while 'NextShardIterator' in out:
        out = kinesis.get_records(ShardIterator=out['NextShardIterator'], Limit=2)
        records = out.get('Records')
        if records:
            for record in records:
                if count > limit:
                    sys.exit(0)
                else:
                    if enriched:
                        payload = snowplow_analytics_sdk.event_transformer.transform(record.get('Data').decode("utf-8"))
                    else:
                        payload = vars(deserialize(collector_payload, record.get('Data'), TCyBinaryProtocolFactory()))
                        payload['body'] = parse.parse_qs(payload.get('body'))

                    record['Data'] = payload

                    record = json.dumps(record, default=serializer, ensure_ascii=False) if to_json else record
                    pprint(record) if pretty else print(record)

                    if limit > 0:
                        count += 1

        time.sleep(interval)


def main(args=None):
    parser = argparse.ArgumentParser(description='AWS Kinesis stream listener for Snowplow events.')
    parser.add_argument('-s', '--stream', type=str, required=True, help='Kinesis stream')

    parser.add_argument('-r', '--region', type=str, required=True, help='AWS Region')

    parser.add_argument('-e', '--enriched', action='store_true', help='Results are enriched')

    parser.add_argument('-i', '--interval', type=float, default=1.0, help='Response intervals; default 1 second')

    parser.add_argument('-l', '--limit', type=int, default=0, help='Maximum records to return; default 0 (infinite)')

    parser.add_argument('-j', '--json', action='store_true', help='JSON output')

    parser.add_argument('-p', '--pretty', action='store_true', help='Pretty print results')
    results = parser.parse_args(args)

    consume_stream(results.stream, results.region, results.enriched, results.interval, results.limit, results.json,
                   results.pretty)


if __name__ == "__main__":
    main(sys.argv[1:])