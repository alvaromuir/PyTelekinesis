
import json
import time

from random import random

import boto3

awsRegion = "us-east-2"
# accessKeyId = ""  # Your AWS Access Key ID
# secretAccessKey = ""  # Your AWS Secret Access Key
inputStream = "valid"

# Variables that control properties of the generated data.
xRange = [0, 10]  # The range of values taken by the x-coordinate
yRange = [0, 10]  # The range of values taken by the y-coordinate
hotspotSideLength = 1  # The side length of the hotspot
hotspotWeight = 0.2  # The fraction of points that are draw from the hotspots


def generate_point_in_rectangle(x_min, width, y_min, height):
    """Generate points uniformly in the given rectangle."""
    return {
        'x': x_min + random() * width,
        'y': y_min + random() * height
    }


class RecordGenerator(object):
    """A class used to generate points used as input to the hotspot detection algorithm. With probability hotspotWeight,
    a point is drawn from a hotspot, otherwise it is drawn from the base distribution. The location of the hotspot
    changes after every 1000 points generated."""

    def __init__(self):
        self.x_min = xRange[0]
        self.width = xRange[1] - xRange[0]
        self.y_min = yRange[0]
        self.height = yRange[1] - yRange[0]
        self.points_generated = 0
        self.hotspot_x_min = None
        self.hotspot_y_min = None

    def get_record(self):
        if self.points_generated % 1000 == 0:
            self.update_hotspot()

        if random() < hotspotWeight:
            record = generate_point_in_rectangle(self.hotspot_x_min, hotspotSideLength, self.hotspot_y_min,
                                                 hotspotSideLength)
            record['is_hot'] = 'Y'
        else:
            record = generate_point_in_rectangle(self.x_min, self.width, self.y_min, self.height)
            record['is_hot'] = 'N'

        self.points_generated += 1
        data = json.dumps(record)
        return {'Data': bytes(data, 'utf-8'), 'PartitionKey': 'partition_key'}

    def get_records(self, n):
        return [self.get_record() for _ in range(n)]

    def update_hotspot(self):
        self.hotspot_x_min = self.x_min + random() * (self.width - hotspotSideLength)
        self.hotspot_y_min = self.y_min + random() * (self.height - hotspotSideLength)


def main():
    kinesis = boto3.client('kinesis')

    generator = RecordGenerator()
    batch_size = 10

    while True:
        records = generator.get_records(batch_size)
        print(records)
        kinesis.put_records(StreamName=inputStream, Records=records)

        time.sleep(0.1)


if __name__ == "__main__":
    main()

