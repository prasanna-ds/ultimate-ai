from __future__ import annotations

import logging
import signal
import time

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType
from stream_processor.common.config import MONGODB_CONNECTION_STR


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Source(ABC):
    def __init__(self, application_name: str):
        self.spark = (
            SparkSession.builder.appName(application_name)
            .config("spark.mongodb.input.uri", MONGODB_CONNECTION_STR)
            .config("spark.mongodb.output.uri", MONGODB_CONNECTION_STR)
            .getOrCreate()
        )
        self.stream: DataFrame = self.spark.createDataFrame([], StructType([]))
        self.streaming_query: StreamingQuery = None  # type: ignore

        signal.signal(signal.SIGTERM, lambda x, y: self.shutdown())
        signal.signal(signal.SIGINT, lambda x, y: self.shutdown())

    @abstractmethod
    def process(self) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def write_stream(self) -> None:
        raise NotImplementedError

    def shutdown(self) -> None:
        logging.info("Received shutdown signal, stopping stream processing...")
        while self.streaming_query.isActive:
            message: str = self.streaming_query.status["message"]
            data_available: bool = self.streaming_query.status["isDataAvailable"]
            trigger_active: bool = self.streaming_query.status["isTriggerActive"]
            if (
                not data_available
                and not trigger_active
                and message != "Initializing sources"
            ):
                self.streaming_query.stop()
            time.sleep(10)

        self.streaming_query.awaitTermination(10)
