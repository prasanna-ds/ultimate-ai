from stream_processor.processor.abstract_stream_processor import StreamProcessor
from stream_processor.processor.twitter_stream_processor import TwitterStreamProcessor


def get_stream_processor(source: str, application_name: str) -> StreamProcessor:
    """Sources Factory"""
    stream_processor = {"socket": TwitterStreamProcessor}

    return stream_processor[source](application_name, source)
