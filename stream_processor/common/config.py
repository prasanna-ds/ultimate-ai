import os


WORLDOMETER_URL: str = os.getenv(
    "WORLDOMETER_URL", "https://www.worldometers.info/coronavirus/"
)
MONGODB_CONNECTION_STR: str = os.getenv(
    "MONGODB_CONNECTION_STR", "mongodb://localhost:27017"
)
MONGODB_DATABASE: str = os.getenv("MONGODB_DATABASE", "ultimate_ai")
MONGODB_COLLECTION: str = os.getenv("MONGODB_COLLECTION", "tweet_with_corona_count")

BATCH_DURATION: str = "20 seconds"
