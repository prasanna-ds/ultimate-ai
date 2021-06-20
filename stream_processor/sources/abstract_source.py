from __future__ import annotations

import logging
import signal
import time

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from kafka_utilis.common.config import (
    MONGODB_COLLECTION,
    MONGODB_CONNECTION_STR,
    MONGODB_DATABASE,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write(stream: DataFrame, batch_id: int) -> None:
    logger.info("Writing a batch to Mongodb...")
    print("Writing a batch to Mongodb...")
    (
        stream.write.format("mongo")
        .mode("append")
        .option("database", MONGODB_DATABASE)
        .option("collection", MONGODB_COLLECTION)
        .save()
    )
    pass


class Source(ABC):
    def __init__(self, application_name):
        self.spark = (
            SparkSession.builder.appName(application_name)
            .config("spark.mongodb.input.uri", MONGODB_CONNECTION_STR)
            .config("spark.mongodb.output.uri", MONGODB_CONNECTION_STR)
            .getOrCreate()
        )
        self.stream: DataFrame = self.spark.createDataFrame([], StructType([]))
        self.streaming_query = None

        signal.signal(signal.SIGTERM, lambda x, y: self.shutdown())
        signal.signal(signal.SIGINT, lambda x, y: self.shutdown())

    @abstractmethod
    def process(self) -> DataFrame:
        raise NotImplementedError

    def write_stream(self) -> None:
        self.streaming_query: StreamingQuery = (
            self.stream.writeStream.trigger(processingTime="20 seconds")
            .foreachBatch(write)
            .start()
        )
        self.streaming_query.awaitTermination()

    def shutdown(self) -> None:
        logging.info("Received shutdown signal, stopping stream processing...")
        while self.streaming_query.isActive:
            message = self.streaming_query.status["message"]
            data_available = self.streaming_query.status["isDataAvailable"]
            trigger_active = self.streaming_query.status["isTriggerActive"]
            if (
                not data_available
                and not trigger_active
                and message != "Initializing sources"
            ):
                self.streaming_query.stop()
            time.sleep(10)

        self.streaming_query.awaitTermination(10)
