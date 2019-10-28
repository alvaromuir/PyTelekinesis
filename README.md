# Telekinesis

Telekinesis is a simple AWS Lambda streaming app focused on [snowplow](https://github.com/snowplow/snowplow) formatted events.

Built in Python 3.7.x

run `$ python ./consumer`

```bash
usage: consumer.py [-h] -s STREAM -r REGION [-e] [-i INTERVAL] [-l LIMIT] [-j]
                   [-p]

AWS Kinesis stream listener for Snowplow events.

optional arguments:
  -h, --help            show this help message and exit
  -s STREAM, --stream STREAM
                        Kinesis stream
  -r REGION, --region REGION
                        AWS Region
  -e, --enriched        Results are enriched
  -i INTERVAL, --interval INTERVAL
                        Response intervals; default 1 second
  -l LIMIT, --limit LIMIT
                        Maximum records to return; default 0 (infinite)
  -j, --json            JSON output
  -p, --pretty          Pretty print results
```

[Alvaro Muir](mailto:alvaro@coca-cola.com)  
KO MDS Global Analytics
