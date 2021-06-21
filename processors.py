from stream_processor.sources.abstract_source import Source
from stream_processor.sources.twitter_source import TwitterSocketSource


def get_stream_processor(source: str, application_name: str) -> Source:
    """Sources Factory"""
    stream_processor = {"twitter": TwitterSocketSource}

    return stream_processor[source](application_name)
