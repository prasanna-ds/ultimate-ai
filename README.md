# Ultimate-ai challenge

The objective is to process twitter's live tweet stream, process, accumulate every 20 seconds and merge them with total coronavirus cases from worldometer.info.
The processed micro batch of tweets with coronavirus case information will be used by the data scientists for predicting the number of potential customers.

## Assumptions

Sources may vary, but the sink(Mongodb) can be same for storing the processed real-time events.

## Implementation



## Steps to run

```shell
docker-compose up --build -d
docker-compose down --remove-orphans
```
