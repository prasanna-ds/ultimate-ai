#!/bin/bash
# Setup application environments
export MONGODB_CONNECTION_STR="mongodb://mongo:27017"
export MONGODB_DATABASE="ultimate_ai"
export MONGODB_COLLECTION="tweet_with_corona_count"
nohup python3 twitter_simulator/twitter_simulator.py &
/spark/bin/spark-submit --master local --deploy-mode client  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 main.py --jobname ultimate_ai_stream_processing
