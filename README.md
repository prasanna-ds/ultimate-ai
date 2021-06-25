# Ultimate-ai challenge

The objective is to process twitter's live tweet stream, process, accumulate every 20 seconds and merge them with total coronavirus cases from worldometer.info.
The processed micro batch of tweets with coronavirus case information will be used by the data scientists for predicting the number of potential customers.

## Assumptions

Sources may vary, but the sink(Mongodb) is same for storing the processed real-time events.

## Implementation

### Design

Implemented based on factory method that provides an interface `StreamProcessor` with concrete methods for a stream processor but can be altered
when adding new sources like `TwitterStreamProcessor`

Methods to implement on a new source,

`process` - pre-processing/data processing on a single event

Subclasses can alter the objects returned by the following factory methods,

`process_micro_batch`
`write_stream`
`write`

### Cache Implementation

This application holds a simple cache(key-value store)to lookup the corona case count if the "https://www.worldometers.info/coronavirus/" is not reachable
or timed-out. The initial value is set to -1 and on each successful request, the cache will be updated.

1. Whenever there is a request exception(assuming short hiccups), the corona case count will be retrieved from the cache.

2. "https://www.worldometers.info/coronavirus/" is external and we don't have control on changes. So we if we could not parse the
    html, we set the corona_case_count as -1 which indicates(to be monitored) that we have to adjust our application without stopping
   the processing of events and downtime.
   
## Running in Production

In Spark, there can be only spark session per JVM, so the application is designed to run as a different job for different sources,

```shell
spark-submit --master yarn --deploy-mode cluster  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 main.py --jobname ultimate_ai_socket_stream_processing --source socket
spark-submit --master yarn --deploy-mode cluster  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 main.py --jobname ultimate_ai_kafka_stream_processing --source kafka
```
This also helps in better failure management, operations and monitoring than the tightly coupled applications.

## Steps to run

```shell
docker-compose up --build -d
docker-compose down --remove-orphans
```
