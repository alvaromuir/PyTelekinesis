import boto3
import json
from datetime import datetime
import calendar
import random
import time

kinesis = boto3.client('kinesis')
productionStream = "PyTelekinesis"


def push(_id, _value, _timestamp):
    payload = {'id': _id, 'value': str(_value), 'timestamp': str(_timestamp)}
    print(json.dumps(payload))
    kinesis.put_record(StreamName=productionStream, Data=json.dumps(payload), PartitionKey=_id)


def main():
    while True:
        event_id = 'abc123'
        event_value = random.randint(40, 120)
        event_timestamp = calendar.timegm(datetime.utcnow().timetuple())

        push(event_id, event_value, event_timestamp)
        time.sleep(1)


if __name__ == '__main__':
    main()
