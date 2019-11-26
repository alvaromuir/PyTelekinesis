# Telekinesis

Telekinesis is a simple AWS Lambda streaming app focused on [snowplow](https://github.com/snowplow/snowplow) formatted events.

Built in Python 3.7.x

run `$ python ./consumer`

```bash
$ python consumer.py -h                                                                                         master ✱
usage: consumer.py [-h] -n NAME -r REGION [-e] [-l LIMIT] [-i INTERVAL]
                   [-m MAX] [-p]

AWS Kinesis stream listener for Snowplow events.

optional arguments:
  -h, --help            show this help message and exit
  -n NAME, --name NAME  name of Kinesis stream
  -r REGION, --region REGION
                        AWS Region
  -e, --enriched        Results are enriched
  -l LIMIT, --limit LIMIT
                        Records per shard limit; default 25, minimum is 2
  -i INTERVAL, --interval INTERVAL
                        Response intervals; default 1 second
  -m MAX, --max MAX     Maximum records to return; default 0 (infinite)
  -p, --pretty          Pretty print results
```

[Alvaro Muir](mailto:alvaro@coca-cola.com)  
KO MDS Global Analytics
